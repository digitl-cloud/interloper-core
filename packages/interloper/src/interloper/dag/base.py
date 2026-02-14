"""Directed Acyclic Graph for asset execution."""

import inspect
from typing import TYPE_CHECKING

from interloper.assets.base import Asset, AssetDefinition
from interloper.assets.keys import AssetInstanceKey
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.serialization.base import Serializable
from interloper.serialization.dag import DAGSpec
from interloper.source.base import Source, SourceDefinition

if TYPE_CHECKING:
    from interloper.runners.state import RunState


class DAG(Serializable):
    """Represents a Directed Acyclic Graph of assets.

    Automatically infers dependencies between assets and provides
    execution capabilities.
    """

    def __init__(self, *assets_or_sources: Asset | Source | AssetDefinition | SourceDefinition):
        """Create a DAG from individual assets or sources.

        Args:
            *assets_or_sources: Assets/Sources or their Definitions to include in the DAG
        """
        self.assets: list[Asset] = []
        self.asset_map: dict[AssetInstanceKey, Asset] = {}
        self.predecessors: dict[AssetInstanceKey, list[AssetInstanceKey]] = {}
        self.successors: dict[AssetInstanceKey, list[AssetInstanceKey]] = {}
        self._dependency_params: dict[AssetInstanceKey, dict[AssetInstanceKey, str]] = {}
        self._build_graph(assets_or_sources)
        self._validate()

    def _build_graph(self, assets_or_sources: tuple[Asset | Source | AssetDefinition | SourceDefinition, ...]) -> None:
        """Build the dependency graph from assets and sources.

        Accepts runtime instances (`Asset`, `Source`) as well as definitions
        (`AssetDefinition`, `SourceDefinition`). Definitions are instantiated
        with best-effort config inference (env-based) when available.
        """
        if not assets_or_sources:
            raise ValueError("DAG must contain at least one asset or source")

        # Collect all assets
        for item in assets_or_sources:
            if isinstance(item, SourceDefinition):
                source_instance = item()
                self.assets.extend(source_instance.assets.values())
            elif isinstance(item, AssetDefinition):
                asset_instance = item()
                self.assets.append(asset_instance)
            elif isinstance(item, Source):
                self.assets.extend(item.assets.values())
            elif isinstance(item, Asset):
                self.assets.append(item)
            else:
                raise TypeError(f"Expected Asset or Source, got {type(item)}")

        # Build asset map using key
        self.asset_map = {asset.instance_key: asset for asset in self.assets}

        # Check for duplicate keys
        if len(self.asset_map) != len(self.assets):
            seen_keys = set()
            duplicates = []
            for asset in self.assets:
                if asset.instance_key in seen_keys:
                    duplicates.append(asset.instance_key)
                seen_keys.add(asset.instance_key)
            raise ValueError(f"Duplicate key found: {duplicates}")

        # Initialize successors dict with empty lists
        for asset in self.assets:
            self.successors[asset.instance_key] = []

        # Build dependency graph
        for asset in self.assets:
            # Skip non-materializable assets: their dependencies are not considered
            # This is necessary when building mini-DAGS.
            if not asset.materializable:
                continue

            self.predecessors[asset.instance_key] = []

            # Inspect function signature for dependencies
            sig = inspect.signature(asset.func)
            for param_name in sig.parameters:
                if param_name in ("context", "config", "self"):
                    continue

                upstream_key = self.resolve_dependency_key(asset, param_name)

                # This is a dependency
                if upstream_key in self.asset_map:
                    self.predecessors[asset.instance_key].append(upstream_key)
                    self.successors[upstream_key].append(asset.instance_key)
                    if asset.instance_key not in self._dependency_params:
                        self._dependency_params[asset.instance_key] = {}
                    self._dependency_params[asset.instance_key][upstream_key] = param_name
                else:
                    # Dependency not found in DAG
                    raise ValueError(
                        f"Asset '{asset.instance_key}' depends on '{upstream_key}' which is not in the DAG. "
                        f"Available assets: {list(self.asset_map.keys())}"
                    )

    def _resolve_source_alias_key(self, source_name: str, param_name: str) -> AssetInstanceKey | None:
        """Resolve a dependency by matching a renamed asset within the same source."""
        matches: list[AssetInstanceKey] = []
        for asset in self.assets:
            if not asset.source or asset.source.name != source_name:
                continue
            original_name = asset.metadata.get("source_original_name")
            if original_name == param_name:
                matches.append(asset.instance_key)

        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous dependency '{param_name}' in source '{source_name}'. "
                f"Multiple renamed assets match: {sorted(matches)}."
            )
        return matches[0]

    def resolve_dependency_key(self, asset: Asset, param_name: str) -> AssetInstanceKey:
        """Resolve a dependency key for a parameter.

        Resolution order:
        - Explicit mapping via asset.deps
        - Same-source implicit mapping (param -> source.asset), including renamed assets
        - Standalone implicit mapping (param -> param)
        """
        if param_name in asset.deps:
            return AssetInstanceKey(asset.deps[param_name])

        if asset.source:
            upstream_key = AssetInstanceKey(f"{asset.source.instance_key}:{param_name}")
            if upstream_key in self.asset_map:
                return upstream_key

            alias_key = self._resolve_source_alias_key(asset.source.name, param_name)
            if alias_key is not None:
                return alias_key

            return upstream_key

        # Standalone asset - just use param name
        return AssetInstanceKey(param_name)

    def _validate(self) -> None:
        """Validate the DAG.

        Checks:
        - No circular dependencies (must be acyclic)
        - Partition dependencies are valid
        - Requires constraints match resolved upstream definitions

        Raises errors at DAG construction time, not at runtime.
        """
        self._check_circular_dependencies()
        self._check_partition_dependencies()
        self._check_requires_constraints()

    def _check_partition_dependencies(self) -> None:
        """Check that no non-partitioned asset depends on a partitioned asset."""
        for asset_key, preds in self.predecessors.items():
            asset = self.asset_map[asset_key]
            for pred_key in preds:
                upstream_asset = self.asset_map[pred_key]
                if upstream_asset.partitioning is not None and asset.partitioning is None:
                    raise ValueError(
                        f"Invalid dependency: partitioned asset '{upstream_asset.instance_key}' "
                        f"cannot be a dependency of non-partitioned asset '{asset.instance_key}'"
                    )

    def _check_requires_constraints(self) -> None:
        """Check that resolved upstream assets match declared requires definitions."""
        for asset_key, preds in self.predecessors.items():
            asset = self.asset_map[asset_key]
            requires = asset.definition.requires
            if not requires:
                continue
            for pred_key in preds:
                upstream_asset = self.asset_map[pred_key]
                param_name = self._dependency_params[asset_key][pred_key]
                if param_name in requires:
                    expected_def_key = requires[param_name]
                    actual_def_key = upstream_asset.definition.definition_key
                    if actual_def_key != expected_def_key:
                        raise ValueError(
                            f"Asset '{asset.instance_key}' requires parameter '{param_name}' "
                            f"to come from definition '{expected_def_key}', "
                            f"but resolved to '{actual_def_key}'"
                        )

    def _check_circular_dependencies(self) -> None:
        """Check for circular dependencies using DFS."""
        visited = set()
        stack = set()

        def has_cycle(node: AssetInstanceKey) -> bool:
            visited.add(node)
            stack.add(node)

            for neighbor in self.predecessors.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in stack:
                    return True

            stack.remove(node)
            return False

        for asset_key in self.predecessors:
            if asset_key not in visited:
                if has_cycle(asset_key):
                    raise ValueError(f"Circular dependency detected involving asset '{asset_key}'")

    def topological_generations(self) -> list[list[Asset]]:
        """Return assets grouped by parallelizable generations.

        Each inner list contains assets that can be executed in parallel
        (no dependencies between them). Lists are ordered so that all
        dependencies of a level appear in previous levels.
        """
        # Kahn's algorithm adapted to produce levels
        in_degree = {key: len(preds) for key, preds in self.predecessors.items()}
        # Initial level: assets with no dependencies
        current_level = sorted([key for key, degree in in_degree.items() if degree == 0])
        levels: list[list[Asset]] = []

        processed_count = 0
        while current_level:
            # Append current level as Asset objects
            levels.append([self.asset_map[key] for key in current_level])

            # Prepare next level
            next_level: list[AssetInstanceKey] = []
            for asset_key in current_level:
                processed_count += 1
                # For each dependent, decrement in-degree and collect newly free nodes
                for dependent_key, preds in self.predecessors.items():
                    if asset_key in preds:
                        in_degree[dependent_key] -= 1
                        if in_degree[dependent_key] == 0:
                            next_level.append(dependent_key)

            # Deterministic ordering
            current_level = sorted(next_level)

        if processed_count != len(self.assets):
            raise ValueError("Circular dependency detected in DAG")

        return levels

    def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """Execute all assets in dependency order and write to IO.

        This is syntactic sugar that internally uses MultiThreadRunner.

        Args:
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            RunResult
        """
        from interloper.runners.multi_thread import MultiThreadRunner

        runner = MultiThreadRunner()
        return runner.run(dag=self, partition_or_window=partition_or_window)

    def get_predecessors(self, asset_key: AssetInstanceKey) -> list[AssetInstanceKey]:
        """Return list of upstream asset keys (dependencies) for the given asset.

        Args:
            asset_key: The key of the asset to get predecessors for

        Returns:
            List of asset keys that the given asset depends on

        Raises:
            KeyError: If the asset_key is not found in the DAG
        """
        if asset_key not in self.asset_map:
            raise KeyError(f"Asset '{asset_key}' not found in DAG")
        return self.predecessors.get(asset_key, [])

    def get_successors(self, asset_key: AssetInstanceKey) -> list[AssetInstanceKey]:
        """Return list of downstream asset keys (dependents) for the given asset.

        Args:
            asset_key: The key of the asset to get successors for

        Returns:
            List of asset keys that depend on the given asset

        Raises:
            KeyError: If the asset_key is not found in the DAG
        """
        if asset_key not in self.asset_map:
            raise KeyError(f"Asset '{asset_key}' not found in DAG")
        return self.successors.get(asset_key, [])

    def mini_dag(self, asset_key: AssetInstanceKey) -> "DAG":
        """Create a mini-DAG with the target and its immediate parents only.

        The parents are marked as non-materializable.

        Args:
            asset_key: The key of the asset to create a mini-DAG for

        Returns:
            A mini-DAG with the target and its immediate parents only
        """
        if asset_key not in self.asset_map:
            raise KeyError(f"Asset '{asset_key}' not found in DAG")

        target = self.asset_map[asset_key].copy()

        assets: list[Asset] = []
        for upstream_key in self.get_predecessors(asset_key):
            parent = self.asset_map[upstream_key].copy(materializable=False)
            assets.append(parent)

        assets.append(target)
        return DAG(*assets)

    def copy(self) -> "DAG":
        """Clone the DAG."""
        return DAG(*self.assets)

    def to_spec(self) -> DAGSpec:
        """Convert to serializable spec."""
        return DAGSpec(assets=[asset.to_spec() for asset in self.assets])

    @classmethod
    def from_failed_state(cls, state: "RunState") -> "DAG":
        """Return a DAG marking completed assets as non-materializable.

        All assets that were COMPLETED in the provided state are set to
        `materializable=False`. All others (FAILED, QUEUED, etc.) remain
        materializable for retry purposes.
        """
        dag = state.dag.copy()

        completed_keys = set(
            key for key, info in state.asset_executions.items() if info.status == ExecutionStatus.COMPLETED
        )

        for asset in dag.assets:
            asset.materializable = asset.instance_key not in completed_keys

        return dag
