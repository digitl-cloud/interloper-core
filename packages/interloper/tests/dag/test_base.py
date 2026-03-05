"""Tests for DAG.

Key semantics:
- Standalone asset (no source): key = asset.name
- Asset from a source: key = source.name + ":" + asset.name

Dependency resolution:
- With no dataset, param name is used as upstream key (matches standalone keys).
- With dataset set, inference uses source.name:param_name; standalone
  keys use the name directly, so explicit deps are required when referencing other assets.
"""

import datetime as dt

import pytest

import interloper as il
from interloper.errors import (
    AssetNotFoundError,
    CircularDependencyError,
    DAGError,
    DependencyNotFoundError,
    PartitionError,
)
from interloper.runners.results import ExecutionStatus


class TestDAGInitialization:
    """What the DAG constructor accepts."""

    def test_accepts_single_asset(self):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())
        assert len(dag.assets) == 1
        assert dag.asset_map["my_asset"].name == "my_asset"

    def test_accepts_multiple_assets(self):
        @il.asset
        def a1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def a2(context: il.ExecutionContext) -> str:
            return "b"

        dag = il.DAG(a1(), a2())
        assert len(dag.assets) == 2
        assert "a1" in dag.asset_map and "a2" in dag.asset_map

    def test_accepts_source_instance(self):
        @il.source
        class MySource:
            @il.asset
            def a1(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def a2(self, context: il.ExecutionContext) -> str:
                return "b"

        dag = il.DAG(MySource())
        assert len(dag.assets) == 2

    def test_accepts_mixed_assets_and_sources(self):
        @il.asset
        def standalone(context: il.ExecutionContext) -> str:
            return "a"

        @il.source
        class MySource:
            @il.asset
            def source_asset(self, context: il.ExecutionContext) -> str:
                return "b"

        dag = il.DAG(standalone(), MySource())
        assert len(dag.assets) == 2
        assert "standalone" in dag.asset_map
        assert "MySource:source_asset" in dag.asset_map

    def test_accepts_asset_definition_instantiates(self):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset)
        assert len(dag.assets) == 1
        assert any(a.name == "my_asset" for a in dag.assets)

    def test_accepts_source_definition_instantiates(self):
        @il.source
        class MySource:
            @il.asset
            def a1(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def a2(self, context: il.ExecutionContext) -> str:
                return "b"

        dag = il.DAG(MySource)
        assert len(dag.assets) == 2
        assert any(a.name == "a1" for a in dag.assets)
        assert any(a.name == "a2" for a in dag.assets)

    def test_accepts_mixed_definitions_and_instances(self):
        @il.asset
        def from_def(context: il.ExecutionContext) -> str:
            return "x"

        @il.asset
        def from_instance(context: il.ExecutionContext) -> str:
            return "y"

        @il.source
        class SourceDef:
            @il.asset
            def source_asset(self, context: il.ExecutionContext) -> str:
                return "z"

        dag = il.DAG(from_def, from_instance(), SourceDef)
        assert any(a.name == "from_def" for a in dag.assets)
        assert any(a.name == "from_instance" for a in dag.assets)
        assert any(a.name == "source_asset" for a in dag.assets)

    def test_asset_definition_with_config_instantiates(self):
        class TestConfig(il.Config):
            value: str = "default"

        @il.asset(config=TestConfig)
        def config_asset(context: il.ExecutionContext, config: TestConfig) -> str:
            return config.value

        dag = il.DAG(config_asset)
        assert len(dag.assets) == 1
        assert any(a.name == "config_asset" for a in dag.assets)

    def test_source_definition_with_config_instantiates(self):
        class TestConfig(il.Config):
            value: str = "default"

        @il.source(config=TestConfig)
        class ConfigSource:
            def __init__(self, config: TestConfig) -> None:
                self.cfg = config

            @il.asset
            def source_asset(self, context: il.ExecutionContext) -> str:
                return self.cfg.value

        dag = il.DAG(ConfigSource)
        assert len(dag.assets) == 1
        assert any(a.name == "source_asset" for a in dag.assets)

    def test_rejects_invalid_type(self):
        with pytest.raises(DAGError, match="Expected Asset or Source"):
            il.DAG("invalid")  # type: ignore[arg-type]
        with pytest.raises(DAGError, match="Expected Asset or Source"):
            il.DAG(123)  # type: ignore[arg-type]


class TestDAGKeys:
    """Asset key semantics: standalone vs source."""

    def test_standalone_asset_key_equals_name(self):
        """Standalone assets use their name as the DAG key."""

        @il.asset(dataset="dataset1")
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(dataset="dataset2")
        def asset2(context: il.ExecutionContext) -> str:
            return "b"

        dag = il.DAG(asset1(), asset2())
        assert list(dag.asset_map.keys()) == ["asset1", "asset2"]
        assert dag.asset_map["asset1"].name == "asset1"
        assert dag.asset_map["asset2"].name == "asset2"

    def test_source_asset_key_equals_source_name_colon_asset_name(self):
        """Assets from a source use source_name:asset_name as key."""

        @il.source
        class Source1:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "v1"

        @il.source
        class Source2:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "v2"

        dag = il.DAG(Source1(), Source2())
        assert len(dag.asset_map) == 2
        assert "Source1:my_asset" in dag.asset_map
        assert "Source2:my_asset" in dag.asset_map

    def test_source_asset_key_uses_custom_source_name_when_given(self):
        """Source instances can override name; key uses that name."""

        @il.source(name="custom_source1")
        class S1:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "v1"

        @il.source(name="custom_source2")
        class S2:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "v2"

        dag = il.DAG(S1(), S2())
        assert "custom_source1:my_asset" in dag.asset_map
        assert "custom_source2:my_asset" in dag.asset_map

    def test_duplicate_key_raises_when_two_standalone_assets_same_name(self):
        """Two standalone assets with the same name produce the same key → ValueError."""

        @il.asset(dataset="dataset1")
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(dataset="dataset1", name="asset1")
        def other_asset(context: il.ExecutionContext) -> str:
            return "b"

        with pytest.raises(DAGError, match="Duplicate key found"):
            il.DAG(asset1(), other_asset())


class TestDAGDependencyResolution:
    """How dependencies are resolved: inference vs explicit deps."""

    def test_inferred_dependency_when_no_dataset_param_name_is_key(self):
        """With no dataset, param name is used as upstream key (matches standalone key)."""

        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_explicit_deps_resolve_by_asset_key(self):
        """Explicit deps map param to asset key; required when param name != key or keys are name-only."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(dataset="dataset2")
        def downstream_asset(context: il.ExecutionContext, upstream: str) -> str:
            return f"downstream_{upstream}"

        dag = il.DAG(upstream_asset(), downstream_asset(deps={"upstream": "upstream_asset"}))
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_standalone_assets_with_dataset_infer_deps_by_param_name(self):
        """Standalone assets infer deps by param name (dataset is for storage, not keys)."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(dataset="dataset1")
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        # Standalone assets have key = asset name, deps are inferred as param name
        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_explicit_dep_key_not_in_dag_raises(self):
        """Explicit dep pointing to a key not in the DAG raises."""

        @il.asset
        def my_asset(context: il.ExecutionContext, missing: str) -> str:
            return missing

        with pytest.raises(DependencyNotFoundError, match="depends on 'nonexistent.asset' which is not in the DAG"):
            il.DAG(my_asset(deps={"missing": "nonexistent.asset"}))


class TestDAGStructure:
    """Dependency graph structure: chains, multiple deps, cycles, missing upstream."""

    def test_chain_dependencies(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b + "c"

        dag = il.DAG(asset_a(io=il.FileIO(tmp_path)), asset_b(io=il.FileIO(tmp_path)), asset_c(io=il.FileIO(tmp_path)))
        assert dag.predecessors["asset_b"] == ["asset_a"]
        assert dag.predecessors["asset_c"] == ["asset_b"]

    def test_multiple_dependencies(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_a: str, asset_b: str) -> str:
            return asset_a + asset_b

        dag = il.DAG(asset_a(io=il.FileIO(tmp_path)), asset_b(io=il.FileIO(tmp_path)), asset_c(io=il.FileIO(tmp_path)))
        assert set(dag.predecessors["asset_c"]) == {"asset_a", "asset_b"}

    def test_definition_dependencies_build_correctly(self):
        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        dag = il.DAG(upstream_asset, downstream_asset)
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_circular_dependency_raises(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a

        with pytest.raises(CircularDependencyError, match="Circular dependency"):
            il.DAG(asset_a(io=il.FileIO(tmp_path)), asset_b(io=il.FileIO(tmp_path)))

    def test_missing_upstream_param_raises(self, tmp_path):
        @il.asset
        def my_asset(context: il.ExecutionContext, missing_asset: str) -> str:
            return missing_asset

        with pytest.raises(DependencyNotFoundError):
            il.DAG(my_asset(io=il.FileIO(tmp_path)))


class TestDAGPartitioning:
    """Partition rules: non-partitioned → partitioned ok; partitioned → non-partitioned invalid."""

    def test_non_partitioned_to_partitioned_valid(self, tmp_path):
        @il.asset
        def config_asset(context: il.ExecutionContext) -> str:
            return "config"

        @il.asset(
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(context: il.ExecutionContext, config_asset: str) -> str:
            return f"{config_asset}_{context.partition_date}"

        dag = il.DAG(config_asset(io=il.FileIO(tmp_path)), daily_asset(io=il.FileIO(tmp_path)))
        assert "config_asset" in dag.predecessors["daily_asset"]

    def test_partitioned_to_non_partitioned_raises(self, tmp_path):
        @il.asset(
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(context: il.ExecutionContext) -> str:
            return f"daily_{context.partition_date}"

        @il.asset
        def summary_asset(context: il.ExecutionContext, daily_asset: str) -> str:
            return daily_asset

        with pytest.raises(DAGError):
            il.DAG(daily_asset(io=il.FileIO(tmp_path)), summary_asset(io=il.FileIO(tmp_path)))


class TestDAGMaterialize:
    """DAG.materialize() behavior."""

    def test_materialize_without_partition(self, tmp_path):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        result = dag.materialize()
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition(self, tmp_path):
        @il.asset(
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        result = dag.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition_window(self, tmp_path):
        @il.asset(
            partitioning=il.TimePartitionConfig(column="date", allow_window=True),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            start, end = context.partition_date_window
            return [{"start": start, "end": end}]

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        result = dag.materialize(
            partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        )
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition_window_not_allowed_raises(self, tmp_path):
        @il.asset(
            partitioning=il.TimePartitionConfig(column="date", allow_window=False),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        with pytest.raises(PartitionError, match="Windowed runs require all partitioned assets"):
            dag.materialize(
                partition_or_window=il.TimePartitionWindow(
                    start=dt.date(2025, 1, 1),
                    end=dt.date(2025, 1, 7),
                )
            )


class TestDAGGraphTraversal:
    """get_predecessors / get_successors and invalid key handling."""

    def test_get_predecessors_single(self, tmp_path):
        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(io=il.FileIO(tmp_path)), downstream_asset(io=il.FileIO(tmp_path)))
        preds = dag.get_predecessors("downstream_asset")
        assert preds == ["upstream_asset"]

    def test_get_predecessors_multiple(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_a: str, asset_b: str) -> str:
            return asset_a + asset_b

        dag = il.DAG(asset_a(io=il.FileIO(tmp_path)), asset_b(io=il.FileIO(tmp_path)), asset_c(io=il.FileIO(tmp_path)))
        preds = dag.get_predecessors("asset_c")
        assert set(preds) == {"asset_a", "asset_b"}

    def test_get_predecessors_empty_for_root(self, tmp_path):
        @il.asset
        def root_asset(context: il.ExecutionContext) -> str:
            return "root"

        dag = il.DAG(root_asset(io=il.FileIO(tmp_path)))
        assert dag.get_predecessors("root_asset") == []

    def test_get_successors_single(self, tmp_path):
        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(io=il.FileIO(tmp_path)), downstream_asset(io=il.FileIO(tmp_path)))
        succs = dag.get_successors("upstream_asset")
        assert succs == ["downstream_asset"]

    def test_get_successors_multiple(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_c"

        dag = il.DAG(asset_a(io=il.FileIO(tmp_path)), asset_b(io=il.FileIO(tmp_path)), asset_c(io=il.FileIO(tmp_path)))
        succs = dag.get_successors("asset_a")
        assert set(succs) == {"asset_b", "asset_c"}

    def test_get_successors_empty_for_leaf(self, tmp_path):
        @il.asset
        def leaf_asset(context: il.ExecutionContext) -> str:
            return "leaf"

        dag = il.DAG(leaf_asset(io=il.FileIO(tmp_path)))
        assert dag.get_successors("leaf_asset") == []

    def test_get_predecessors_invalid_key_raises(self, tmp_path):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        with pytest.raises(AssetNotFoundError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_predecessors("invalid_key")

    def test_get_successors_invalid_key_raises(self, tmp_path):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        with pytest.raises(AssetNotFoundError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_successors("invalid_key")

    def test_diamond_dag_traversal(self, tmp_path):
        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_c"

        @il.asset
        def asset_d(context: il.ExecutionContext, asset_b: str, asset_c: str) -> str:
            return asset_b + "_" + asset_c

        io = il.FileIO(tmp_path)
        dag = il.DAG(asset_a(io=io), asset_b(io=io), asset_c(io=io), asset_d(io=io))

        assert dag.get_predecessors("asset_a") == []
        assert set(dag.get_successors("asset_a")) == {"asset_b", "asset_c"}
        assert set(dag.get_predecessors("asset_d")) == {"asset_b", "asset_c"}
        assert dag.get_successors("asset_d") == []

    def test_traversal_with_explicit_deps(self, tmp_path):
        """Traversal works when dependency is resolved via explicit deps (param name ≠ key)."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(
            dataset="dataset2",
        )
        def downstream_asset(context: il.ExecutionContext, external: str) -> str:
            return f"downstream_{external}"

        io = il.FileIO(tmp_path)
        dag = il.DAG(
            upstream_asset(io=io),
            downstream_asset(io=io, deps={"external": "upstream_asset"}),
        )

        assert dag.get_predecessors("downstream_asset") == ["upstream_asset"]
        assert dag.get_successors("upstream_asset") == ["downstream_asset"]


class TestDAGRequiresValidation:
    """Validation of requires constraints against upstream definition keys."""

    def test_requires_matching_definition_key_passes(self):
        """Asset with requires wired to the correct upstream definition — no error."""

        @il.asset
        def upstream(context: il.ExecutionContext) -> str:
            return "data"

        @il.asset(requires={"upstream": il.AssetDefinitionKey("upstream")})
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream

        # Should not raise
        dag = il.DAG(upstream(), downstream())
        assert "upstream" in dag.predecessors["downstream"]

    def test_requires_mismatched_definition_key_raises(self):
        """Asset with requires wired to a different upstream definition — ValueError."""

        @il.asset
        def upstream(context: il.ExecutionContext) -> str:
            return "data"

        @il.asset(requires={"upstream": il.AssetDefinitionKey("other_source:upstream")})
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream

        with pytest.raises(
            DAGError,
            match=r"requires parameter 'upstream' to come from definition 'other_source:upstream'",
        ):
            il.DAG(upstream(), downstream())

    def test_requires_partial_only_declared_params_checked(self):
        """Asset with requires on one param but not another — only the declared param is checked."""

        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset(requires={"asset_a": il.AssetDefinitionKey("asset_a")})
        def downstream(context: il.ExecutionContext, asset_a: str, asset_b: str) -> str:
            return asset_a + asset_b

        # Should not raise — asset_a matches, asset_b has no requires constraint
        dag = il.DAG(asset_a(), asset_b(), downstream())
        assert set(dag.predecessors["downstream"]) == {"asset_a", "asset_b"}

    def test_no_requires_skips_validation(self):
        """Asset without requires — no validation (existing behavior)."""

        @il.asset
        def upstream(context: il.ExecutionContext) -> str:
            return "data"

        @il.asset
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream

        # Should not raise — no requires declared
        dag = il.DAG(upstream(), downstream())
        assert "upstream" in dag.predecessors["downstream"]

    def test_requires_with_source_assets(self):
        """Requires validation works with source-bound assets."""

        @il.source
        class SourceA:
            @il.asset
            def data(self, context: il.ExecutionContext) -> str:
                return "a"

        @il.source
        class SourceB:
            @il.asset
            def data(self, context: il.ExecutionContext) -> str:
                return "b"

        @il.asset(requires={"source_a_data": il.AssetDefinitionKey("SourceA:data")})
        def consumer(context: il.ExecutionContext, source_a_data: str) -> str:
            return source_a_data

        # Wire to the correct source via explicit deps
        dag = il.DAG(
            SourceA(),
            SourceB(),
            consumer(deps={"source_a_data": "SourceA:data"}),
        )
        assert "SourceA:data" in dag.predecessors["consumer"]

    def test_requires_mismatch_with_source_assets_raises(self):
        """Requires validation catches wiring to wrong source."""

        @il.source
        class SourceA:
            @il.asset
            def data(self, context: il.ExecutionContext) -> str:
                return "a"

        @il.source
        class SourceB:
            @il.asset
            def data(self, context: il.ExecutionContext) -> str:
                return "b"

        # Requires source-a's data but we wire to source-b
        @il.asset(requires={"wrong_data": il.AssetDefinitionKey("nonexistent_source:data")})
        def consumer(context: il.ExecutionContext, wrong_data: str) -> str:
            return wrong_data

        with pytest.raises(DAGError, match="requires parameter 'wrong_data' to come from definition"):
            il.DAG(
                SourceA(),
                SourceB(),
                consumer(deps={"wrong_data": "SourceB:data"}),
            )


class TestEmptyDAG:
    """DAG() with no arguments."""

    def test_empty_dag_raises(self):
        """DAG with no arguments raises DAGError."""
        with pytest.raises(DAGError, match="at least one asset or source"):
            il.DAG()


class TestTopologicalGenerations:
    """topological_generations grouping."""

    def test_chain(self):
        """Chain A->B->C yields three levels."""

        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_b: str) -> str:
            return "c"

        dag = il.DAG(asset_a(), asset_b(), asset_c())
        gens = dag.topological_generations()

        assert len(gens) == 3
        assert [a.name for a in gens[0]] == ["asset_a"]
        assert [a.name for a in gens[1]] == ["asset_b"]
        assert [a.name for a in gens[2]] == ["asset_c"]

    def test_diamond(self):
        """Diamond A->{B,C}->D yields three levels with B and C parallel."""

        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_a: str) -> str:
            return "c"

        @il.asset
        def asset_d(context: il.ExecutionContext, asset_b: str, asset_c: str) -> str:
            return "d"

        dag = il.DAG(asset_a(), asset_b(), asset_c(), asset_d())
        gens = dag.topological_generations()

        assert len(gens) == 3
        assert [a.name for a in gens[0]] == ["asset_a"]
        assert sorted(a.name for a in gens[1]) == ["asset_b", "asset_c"]
        assert [a.name for a in gens[2]] == ["asset_d"]


class TestMiniDAG:
    """mini_dag creates a sub-DAG with target and immediate parents."""

    def test_structure(self):
        """mini_dag includes target and its parents, parents non-materializable."""

        @il.asset
        def parent_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def parent_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset
        def child(context: il.ExecutionContext, parent_a: str, parent_b: str) -> str:
            return "c"

        dag = il.DAG(parent_a(), parent_b(), child())
        mini = dag.mini_dag("child")

        assert len(mini.assets) == 3
        assert "child" in mini.asset_map

        # Parents should be non-materializable
        for key in ("parent_a", "parent_b"):
            assert mini.asset_map[key].materializable is False

        # Target should remain materializable
        assert mini.asset_map["child"].materializable is True

    def test_invalid_key_raises(self):
        """mini_dag with unknown key raises AssetNotFoundError."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "v"

        dag = il.DAG(my_asset())
        with pytest.raises(AssetNotFoundError, match="not found in DAG"):
            dag.mini_dag("nonexistent")


class TestCopy:
    """DAG.copy creates a new DAG with the same assets."""

    def test_copy_preserves_assets(self):
        """Copied DAG has the same asset keys."""

        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return "b"

        dag = il.DAG(asset_a(), asset_b())
        copied = dag.copy()

        assert set(copied.asset_map.keys()) == set(dag.asset_map.keys())
        assert copied is not dag


class TestFromFailedState:
    """from_failed_state marks completed assets as non-materializable."""

    def test_completed_assets_become_non_materializable(self):
        """Assets that completed in the state are non-materializable in the new DAG."""
        from interloper.runners.state import RunState

        @il.asset
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return "b"

        @il.asset
        def asset_c(context: il.ExecutionContext, asset_b: str) -> str:
            return "c"

        dag = il.DAG(asset_a(), asset_b(), asset_c())
        state = RunState(dag)

        # Simulate: asset_a completed, asset_b failed, asset_c still queued
        state.asset_executions["asset_a"].status = ExecutionStatus.COMPLETED
        state.asset_executions["asset_b"].status = ExecutionStatus.FAILED

        retry_dag = il.DAG.from_failed_state(state)

        # asset_a completed -> non-materializable
        assert retry_dag.asset_map["asset_a"].materializable is False
        # asset_b failed -> materializable (for retry)
        assert retry_dag.asset_map["asset_b"].materializable is True
        # asset_c was queued -> materializable
        assert retry_dag.asset_map["asset_c"].materializable is True


class TestResolveSourceAliasKey:
    """_resolve_source_alias_key finds renamed assets by original name."""

    def test_renamed_asset_resolved_by_original_name(self):
        """When a source renames an asset, a sibling can depend on it by original name."""

        @il.source
        class MySource:
            @il.asset
            def original(self, context: il.ExecutionContext) -> str:
                return "value"

            @il.asset
            def consumer(self, context: il.ExecutionContext, original: str) -> str:
                return original

        # Rename 'original' to 'renamed' at instantiation time
        src = MySource(assets={"original": "renamed", "consumer": "consumer"})

        dag = il.DAG(src)

        # The renamed asset should have key MySource:renamed
        assert "MySource:renamed" in dag.asset_map
        # consumer should successfully resolve its dependency via the alias
        assert "MySource:renamed" in dag.predecessors["MySource:consumer"]
