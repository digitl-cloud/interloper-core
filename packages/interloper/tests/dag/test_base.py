"""Tests for DAG.

Key semantics:
- Standalone asset (no source): key = asset.name
- Asset from a source: key = source.name + "." + asset.name

Dependency resolution:
- With no dataset, param name is used as upstream key (matches standalone keys).
- With dataset set, inference uses dataset.param_name; standalone keys are
  name-only, so explicit deps are required when referencing other assets.
"""

import datetime as dt

import pytest

import interloper as il


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
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def a1(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def a2(context: il.ExecutionContext) -> str:
                return "b"

            return (a1, a2)

        dag = il.DAG(my_source())
        assert len(dag.assets) == 2

    def test_accepts_mixed_assets_and_sources(self):
        @il.asset
        def standalone(context: il.ExecutionContext) -> str:
            return "a"

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return "b"

            return (source_asset,)

        dag = il.DAG(standalone(), my_source())
        assert len(dag.assets) == 2
        assert "standalone" in dag.asset_map
        assert "my_source.source_asset" in dag.asset_map

    def test_accepts_asset_definition_instantiates(self):
        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset)
        assert len(dag.assets) == 1
        assert any(a.name == "my_asset" for a in dag.assets)

    def test_accepts_source_definition_instantiates(self):
        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def a1(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def a2(context: il.ExecutionContext) -> str:
                return "b"

            return (a1, a2)

        dag = il.DAG(my_source)
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
        def source_def() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return "z"

            return (source_asset,)

        dag = il.DAG(from_def, from_instance(), source_def)
        assert any(a.name == "from_def" for a in dag.assets)
        assert any(a.name == "from_instance" for a in dag.assets)
        assert any(a.name == "source_asset" for a in dag.assets)

    def test_asset_definition_with_config_instantiates(self):
        from pydantic_settings import BaseSettings

        class TestConfig(BaseSettings):
            value: str = "default"

        @il.asset(config=TestConfig)
        def config_asset(context: il.ExecutionContext, config: TestConfig) -> str:
            return config.value

        dag = il.DAG(config_asset)
        assert len(dag.assets) == 1
        assert any(a.name == "config_asset" for a in dag.assets)

    def test_source_definition_with_config_instantiates(self):
        from pydantic_settings import BaseSettings

        class TestConfig(BaseSettings):
            value: str = "default"

        @il.source(config=TestConfig)
        def config_source(config: TestConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return config.value

            return (source_asset,)

        dag = il.DAG(config_source)
        assert len(dag.assets) == 1
        assert any(a.name == "source_asset" for a in dag.assets)

    def test_rejects_invalid_type(self):
        with pytest.raises(TypeError, match="Expected Asset or Source"):
            il.DAG("invalid")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Expected Asset or Source"):
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

    def test_source_asset_key_equals_source_name_dot_asset_name(self):
        """Assets from a source use source.name.asset.name as key."""

        @il.source
        def source1() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "v1"

            return (my_asset,)

        @il.source
        def source2() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "v2"

            return (my_asset,)

        dag = il.DAG(source1(), source2())
        assert len(dag.asset_map) == 2
        assert "source1.my_asset" in dag.asset_map
        assert "source2.my_asset" in dag.asset_map

    def test_source_asset_key_uses_custom_source_name_when_given(self):
        """Source instances can override name; key uses that name."""

        @il.source(name="custom_source1")
        def s1() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "v1"

            return (my_asset,)

        @il.source(name="custom_source2")
        def s2() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "v2"

            return (my_asset,)

        dag = il.DAG(s1(), s2())
        assert "custom_source1.my_asset" in dag.asset_map
        assert "custom_source2.my_asset" in dag.asset_map

    def test_duplicate_key_raises_when_two_standalone_assets_same_name(self):
        """Two standalone assets with the same name produce the same key → ValueError."""

        @il.asset(dataset="dataset1")
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(dataset="dataset1", name="asset1")
        def other_asset(context: il.ExecutionContext) -> str:
            return "b"

        with pytest.raises(ValueError, match="Duplicate key found"):
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

        @il.asset(dataset="dataset2", deps={"upstream": "upstream_asset"})
        def downstream_asset(context: il.ExecutionContext, upstream: str) -> str:
            return f"downstream_{upstream}"

        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_inferred_dependency_with_dataset_looks_up_dataset_dot_param_not_key(self):
        """With dataset set, inference uses dataset.param_name; keys are name-only so explicit deps required."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(dataset="dataset1")
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        # No explicit deps: DAG infers "dataset1.upstream_asset" but key is "upstream_asset", so not in DAG.
        with pytest.raises(ValueError, match="depends on 'dataset1.upstream_asset' which is not in the DAG"):
            il.DAG(upstream_asset(), downstream_asset())

    def test_explicit_dep_key_not_in_dag_raises(self):
        """Explicit dep pointing to a key not in the DAG raises."""

        @il.asset(deps={"missing": "nonexistent.asset"})
        def my_asset(context: il.ExecutionContext, missing: str) -> str:
            return missing

        with pytest.raises(ValueError, match="depends on 'nonexistent.asset' which is not in the DAG"):
            il.DAG(my_asset())


class TestDAGStructure:
    """Dependency graph structure: chains, multiple deps, cycles, missing upstream."""

    def test_chain_dependencies(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b + "c"

        dag = il.DAG(asset_a(), asset_b(), asset_c())
        assert dag.predecessors["asset_b"] == ["asset_a"]
        assert dag.predecessors["asset_c"] == ["asset_b"]

    def test_multiple_dependencies(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_a: str, asset_b: str) -> str:
            return asset_a + asset_b

        dag = il.DAG(asset_a(), asset_b(), asset_c())
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
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a

        with pytest.raises(ValueError, match="Circular dependency"):
            il.DAG(asset_a(), asset_b())

    def test_missing_upstream_param_raises(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext, missing_asset: str) -> str:
            return missing_asset

        with pytest.raises(Exception):
            il.DAG(my_asset())


class TestDAGPartitioning:
    """Partition rules: non-partitioned → partitioned ok; partitioned → non-partitioned invalid."""

    def test_non_partitioned_to_partitioned_valid(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def config_asset(context: il.ExecutionContext) -> str:
            return "config"

        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(context: il.ExecutionContext, config_asset: str) -> str:
            return f"{config_asset}_{context.partition_date}"

        dag = il.DAG(config_asset(), daily_asset())
        assert "config_asset" in dag.predecessors["daily_asset"]

    def test_partitioned_to_non_partitioned_raises(self, tmp_path):
        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(context: il.ExecutionContext) -> str:
            return f"daily_{context.partition_date}"

        @il.asset(io=il.FileIO(tmp_path))
        def summary_asset(context: il.ExecutionContext, daily_asset: str) -> str:
            return daily_asset

        with pytest.raises(Exception):
            il.DAG(daily_asset(), summary_asset())


class TestDAGMaterialize:
    """DAG.materialize() behavior."""

    def test_materialize_without_partition(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())
        result = dag.materialize()
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition(self, tmp_path):
        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        dag = il.DAG(my_asset())
        result = dag.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition_window(self, tmp_path):
        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date", allow_window=True),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            start, end = context.partition_date_window
            return [{"start": start, "end": end}]

        dag = il.DAG(my_asset())
        result = dag.materialize(
            partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        )
        assert isinstance(result, il.RunResult)

    @pytest.mark.skip(reason="Default MemoryIO means upstream assets always have IO by default")
    def test_materialize_with_upstream_no_io(self, tmp_path):
        @il.asset(io=None)
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream + "b"

        dag = il.DAG(upstream(), downstream())
        result = dag.materialize()
        assert result.status == "failed"
        assert "downstream" in result.failed_assets

    @pytest.mark.skip(reason="Type hint validation not yet implemented")
    def test_type_hint_matching(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: int) -> str:
            return "b"

        with pytest.raises(Exception):
            il.DAG(upstream(), downstream())


class TestDAGGraphTraversal:
    """get_predecessors / get_successors and invalid key handling."""

    def test_get_predecessors_single(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(), downstream_asset())
        preds = dag.get_predecessors("downstream_asset")
        assert preds == ["upstream_asset"]

    def test_get_predecessors_multiple(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_a: str, asset_b: str) -> str:
            return asset_a + asset_b

        dag = il.DAG(asset_a(), asset_b(), asset_c())
        preds = dag.get_predecessors("asset_c")
        assert set(preds) == {"asset_a", "asset_b"}

    def test_get_predecessors_empty_for_root(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def root_asset(context: il.ExecutionContext) -> str:
            return "root"

        dag = il.DAG(root_asset())
        assert dag.get_predecessors("root_asset") == []

    def test_get_successors_single(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(), downstream_asset())
        succs = dag.get_successors("upstream_asset")
        assert succs == ["downstream_asset"]

    def test_get_successors_multiple(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_c"

        dag = il.DAG(asset_a(), asset_b(), asset_c())
        succs = dag.get_successors("asset_a")
        assert set(succs) == {"asset_b", "asset_c"}

    def test_get_successors_empty_for_leaf(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def leaf_asset(context: il.ExecutionContext) -> str:
            return "leaf"

        dag = il.DAG(leaf_asset())
        assert dag.get_successors("leaf_asset") == []

    def test_get_predecessors_invalid_key_raises(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())
        with pytest.raises(KeyError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_predecessors("invalid_key")

    def test_get_successors_invalid_key_raises(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())
        with pytest.raises(KeyError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_successors("invalid_key")

    def test_diamond_dag_traversal(self, tmp_path):
        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "_c"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_d(context: il.ExecutionContext, asset_b: str, asset_c: str) -> str:
            return asset_b + "_" + asset_c

        dag = il.DAG(asset_a(), asset_b(), asset_c(), asset_d())

        assert dag.get_predecessors("asset_a") == []
        assert set(dag.get_successors("asset_a")) == {"asset_b", "asset_c"}
        assert set(dag.get_predecessors("asset_d")) == {"asset_b", "asset_c"}
        assert dag.get_successors("asset_d") == []

    def test_traversal_with_explicit_deps(self, tmp_path):
        """Traversal works when dependency is resolved via explicit deps (param name ≠ key)."""

        @il.asset(dataset="dataset1", io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(
            dataset="dataset2",
            deps={"external": "upstream_asset"},
            io=il.FileIO(tmp_path),
        )
        def downstream_asset(context: il.ExecutionContext, external: str) -> str:
            return f"downstream_{external}"

        dag = il.DAG(upstream_asset(), downstream_asset())

        assert dag.get_predecessors("downstream_asset") == ["upstream_asset"]
        assert dag.get_successors("upstream_asset") == ["downstream_asset"]
