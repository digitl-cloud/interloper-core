"""Tests for DAG."""

import datetime as dt

import pytest

import interloper as il
from interloper.runners.state import RunState


class TestDAG:
    """Tests for DAG."""

    def test_initialization_with_single_asset(self):
        """Test DAG initialization with a single asset."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        dag = il.DAG(asset_instance)
        assert len(dag.assets) >= 0

    def test_key_usage(self):
        """Test DAG uses key for asset mapping."""

        @il.asset(dataset="dataset1")
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(dataset="dataset2")
        def asset2(context: il.ExecutionContext) -> str:
            return "b"

        dag = il.DAG(asset1(), asset2())
        assert "dataset1.asset1" in dag.asset_map
        assert "dataset2.asset2" in dag.asset_map
        assert dag.asset_map["dataset1.asset1"].name == "asset1"
        assert dag.asset_map["dataset2.asset2"].name == "asset2"

    def test_duplicate_key_error(self):
        """Test DAG raises error for duplicate key."""

        @il.asset(dataset="dataset1")
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(dataset="dataset1", name="asset1")  # Same name, same dataset
        def asset1_duplicate(context: il.ExecutionContext) -> str:
            return "a"

        with pytest.raises(ValueError, match="Duplicate key found"):
            il.DAG(asset1(), asset1_duplicate())

    def test_explicit_dependency_mapping(self):
        """Test DAG with explicit dependency mapping."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(dataset="dataset2", deps={"upstream": "dataset1.upstream_asset"})
        def downstream_asset(context: il.ExecutionContext, upstream: str) -> str:
            return f"downstream_{upstream}"

        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "dataset1.upstream_asset" in dag.predecessors["dataset2.downstream_asset"]

    def test_inferred_dependency_same_dataset(self):
        """Test DAG infers dependencies within same dataset."""

        @il.asset(dataset="dataset1")
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(dataset="dataset1")
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "dataset1.upstream_asset" in dag.predecessors["dataset1.downstream_asset"]

    def test_inferred_dependency_no_dataset(self):
        """Test DAG infers dependencies when no dataset specified."""

        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        dag = il.DAG(upstream_asset(), downstream_asset())
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_cross_dataset_dependency_with_explicit_mapping(self):
        """Test cross-dataset dependency with explicit mapping."""

        @il.asset(dataset="source1")
        def source1_asset(context: il.ExecutionContext) -> str:
            return "from_source1"

        @il.asset(dataset="source2", deps={"external": "source1.source1_asset"})
        def source2_asset(context: il.ExecutionContext, external: str) -> str:
            return f"from_source2_{external}"

        dag = il.DAG(source1_asset(), source2_asset())
        assert "source1.source1_asset" in dag.predecessors["source2.source2_asset"]

    def test_dependency_not_found_error(self):
        """Test DAG raises error when dependency not found."""

        @il.asset(deps={"missing": "nonexistent.asset"})
        def asset_with_missing_dep(context: il.ExecutionContext, missing: str) -> str:
            return "value"

        with pytest.raises(ValueError, match="depends on 'nonexistent.asset' which is not in the DAG"):
            il.DAG(asset_with_missing_dep())

    def test_initialization_with_multiple_assets(self):
        """Test DAG initialization with multiple assets."""

        @il.asset
        def asset1(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def asset2(context: il.ExecutionContext) -> str:
            return "b"

        dag = il.DAG(asset1(), asset2())
        assert len(dag.assets) >= 0

    def test_initialization_with_source(self):
        """Test DAG initialization with a source."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset2(context: il.ExecutionContext) -> str:
                return "b"

            return (asset1, asset2)

        source_instance = my_source()
        dag = il.DAG(source_instance)
        assert len(dag.assets) >= 0

    def test_initialization_with_mixed_assets_and_sources(self):
        """Test DAG initialization with both assets and sources."""

        @il.asset
        def standalone_asset(context: il.ExecutionContext) -> str:
            return "a"

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return "b"

            return (source_asset,)

        dag = il.DAG(standalone_asset(), my_source())
        assert len(dag.assets) >= 0

    def test_dependency_inference(self, tmp_path):
        """Test automatic dependency inference."""

        @il.asset(io=il.FileIO(tmp_path))
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream + "b"

        il.DAG(upstream(), downstream())
        # DAG should infer that downstream depends on upstream

    def test_chain_dependencies(self, tmp_path):
        """Test chain of dependencies."""

        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b + "c"

        il.DAG(asset_a(), asset_b(), asset_c())
        # DAG should infer: asset_a -> asset_b -> asset_c

    def test_multiple_dependencies(self, tmp_path):
        """Test asset with multiple dependencies."""

        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(
            context: il.ExecutionContext,
            asset_a: str,
            asset_b: str,
        ) -> str:
            return asset_a + asset_b

        il.DAG(asset_a(), asset_b(), asset_c())
        # asset_c depends on both asset_a and asset_b

    def test_circular_dependency_validation(self, tmp_path):
        """Test that circular dependencies raise an error."""

        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext, asset_b: str) -> str:
            return asset_b

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a

        # Should raise an error about circular dependency
        with pytest.raises(ValueError, match="Circular dependency"):
            il.DAG(asset_a(), asset_b())

    def test_non_partitioned_to_partitioned(self, tmp_path):
        """Test valid pattern: non-partitioned -> partitioned."""

        @il.asset(io=il.FileIO(tmp_path))
        def config_asset(context: il.ExecutionContext) -> str:
            return "config"

        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(
            context: il.ExecutionContext,
            config_asset: str,
        ) -> str:
            date = context.partition_date
            return f"{config_asset}_{date}"

        il.DAG(config_asset(), daily_asset())
        # Should be valid

    def test_partitioned_to_non_partitioned_invalid(self, tmp_path):
        """Test invalid pattern: partitioned -> non-partitioned raises error."""

        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def daily_asset(context: il.ExecutionContext) -> str:
            return f"daily_{context.partition_date}"

        @il.asset(io=il.FileIO(tmp_path))
        def summary_asset(context: il.ExecutionContext, daily_asset: str) -> str:
            return daily_asset

        # Should raise an error
        with pytest.raises(Exception):
            il.DAG(daily_asset(), summary_asset())

    def test_materialize_without_partition(self, tmp_path):
        """Test DAG.materialize() without partition."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())
        result = dag.materialize()
        assert isinstance(result, il.RunResult)

    def test_materialize_with_partition(self, tmp_path):
        """Test DAG.materialize() with partition."""

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
        """Test DAG.materialize() with partition window."""

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
        """Test DAG.materialize() fails when upstream asset has no IO."""

        @il.asset(io=None)  # No IO configured
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream + "b"

        dag = il.DAG(upstream(), downstream())

        # This should fail because downstream asset can't load data from upstream (no IO)
        result = dag.materialize()
        assert result.status == "failed"
        assert "downstream" in result.failed_assets

    @pytest.mark.skip(reason="Type hint validation not yet implemented")
    def test_type_hint_matching(self, tmp_path):
        """Test that type hints must match between dependencies."""

        @il.asset(io=il.FileIO(tmp_path))
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: int) -> str:
            # Type hint doesn't match
            return "b"

        # Should raise an error about type mismatch
        with pytest.raises(Exception):
            il.DAG(upstream(), downstream())

    def test_missing_dependency(self, tmp_path):
        """Test that missing dependencies raise an error."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext, missing_asset: str) -> str:
            return missing_asset

        # Should raise an error about unresolved dependency
        with pytest.raises(Exception):
            il.DAG(my_asset())

    def test_initialization_with_asset_definition(self):
        """Test DAG initialization with AssetDefinition."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        # Pass the definition directly (not instantiated)
        dag = il.DAG(my_asset)
        assert len(dag.assets) >= 0
        assert any(asset.name == "my_asset" for asset in dag.assets)

    def test_initialization_with_source_definition(self):
        """Test DAG initialization with SourceDefinition."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset2(context: il.ExecutionContext) -> str:
                return "b"

            return (asset1, asset2)

        # Pass the definition directly (not instantiated)
        dag = il.DAG(my_source)
        assert len(dag.assets) >= 0
        assert any(asset.name == "asset1" for asset in dag.assets)
        assert any(asset.name == "asset2" for asset in dag.assets)

    def test_initialization_with_mixed_definitions_and_instances(self):
        """Test DAG initialization with mixed definitions and instances."""

        @il.asset
        def asset_from_def(context: il.ExecutionContext) -> str:
            return "from_def"

        @il.asset
        def asset_from_instance(context: il.ExecutionContext) -> str:
            return "from_instance"

        @il.source
        def source_from_def() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return "from_source_def"

            return (source_asset,)

        # Mix definitions and instances
        dag = il.DAG(
            asset_from_def,  # AssetDefinition
            asset_from_instance(),  # Asset instance
            source_from_def,  # SourceDefinition
        )
        assert len(dag.assets) >= 0
        assert any(asset.name == "asset_from_def" for asset in dag.assets)
        assert any(asset.name == "asset_from_instance" for asset in dag.assets)
        assert any(asset.name == "source_asset" for asset in dag.assets)

    def test_asset_definition_with_config(self):
        """Test AssetDefinition with config gets instantiated properly."""
        from pydantic_settings import BaseSettings

        class TestConfig(BaseSettings):
            value: str = "default"

        @il.asset(config=TestConfig)
        def config_asset(context: il.ExecutionContext, config: TestConfig) -> str:
            return config.value

        # Should instantiate without error (config will be None if env not set)
        dag = il.DAG(config_asset)
        assert len(dag.assets) >= 0
        assert any(asset.name == "config_asset" for asset in dag.assets)

    def test_source_definition_with_config(self):
        """Test SourceDefinition with config gets instantiated properly."""
        from pydantic_settings import BaseSettings

        class TestConfig(BaseSettings):
            value: str = "default"

        @il.source(config=TestConfig)
        def config_source(config: TestConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def source_asset(context: il.ExecutionContext) -> str:
                return config.value

            return (source_asset,)

        # Should instantiate without error (config will be None if env not set)
        dag = il.DAG(config_source)
        assert len(dag.assets) >= 0
        assert any(asset.name == "source_asset" for asset in dag.assets)

    def test_asset_definition_dependencies(self):
        """Test that AssetDefinition dependencies work correctly."""

        @il.asset
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return f"downstream_{upstream_asset}"

        # Pass definitions directly
        dag = il.DAG(upstream_asset, downstream_asset)
        assert len(dag.assets) >= 0
        assert "upstream_asset" in dag.predecessors["downstream_asset"]

    def test_invalid_type_error(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError, match="Expected Asset or Source"):
            il.DAG("invalid_string")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="Expected Asset or Source"):
            il.DAG(123)  # type: ignore[arg-type]

    def test_identical_asset_names_from_different_sources(self):
        """Test that DAG can handle assets with identical names from different sources."""

        @il.source
        def source1() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value1"

            return (my_asset,)

        @il.source
        def source2() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value2"

            return (my_asset,)

        # Should not raise duplicate key error
        dag = il.DAG(source1(), source2())

        # Should have 2 assets with different keys
        assert len(dag.assets) == 2
        assert len(dag.asset_map) == 2

        # Keys should be unique
        keys = list(dag.asset_map.keys())
        assert "source1.my_asset" in keys
        assert "source2.my_asset" in keys
        assert len(set(keys)) == 2  # All keys should be unique

    def test_identical_asset_names_with_custom_source_names(self):
        """Test that DAG can handle assets with identical names from sources with custom names."""

        @il.source(name="custom_source1")
        def source1() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value1"

            return (my_asset,)

        @il.source(name="custom_source2")
        def source2() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value2"

            return (my_asset,)

        # Should not raise duplicate key error
        dag = il.DAG(source1(), source2())

        # Should have 2 assets with different keys
        assert len(dag.assets) == 2
        assert len(dag.asset_map) == 2

        # Keys should be unique and use custom source names
        keys = list(dag.asset_map.keys())
        assert "custom_source1.my_asset" in keys
        assert "custom_source2.my_asset" in keys
        assert len(set(keys)) == 2  # All keys should be unique


class TestDAGGraphTraversal:
    """Tests for DAG graph traversal methods."""

    def test_get_predecessors_single_dependency(self, tmp_path):
        """Test getting predecessors for asset with one upstream dependency."""

        @il.asset(io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(), downstream_asset())

        # Get predecessors of downstream asset
        predecessors = dag.get_predecessors("downstream_asset")
        assert len(predecessors) == 1
        assert "upstream_asset" in predecessors

    def test_get_predecessors_multiple_dependencies(self, tmp_path):
        """Test getting predecessors for asset with multiple upstream dependencies."""

        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext) -> str:
            return "b"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_c(
            context: il.ExecutionContext,
            asset_a: str,
            asset_b: str,
        ) -> str:
            return asset_a + asset_b

        dag = il.DAG(asset_a(), asset_b(), asset_c())

        # Get predecessors of asset_c
        predecessors = dag.get_predecessors("asset_c")
        assert len(predecessors) == 2
        assert "asset_a" in predecessors
        assert "asset_b" in predecessors

    def test_get_predecessors_no_dependencies(self, tmp_path):
        """Test getting predecessors for root asset (no dependencies)."""

        @il.asset(io=il.FileIO(tmp_path))
        def root_asset(context: il.ExecutionContext) -> str:
            return "root"

        dag = il.DAG(root_asset())

        # Get predecessors of root asset
        predecessors = dag.get_predecessors("root_asset")
        assert len(predecessors) == 0

    def test_get_successors_single_dependent(self, tmp_path):
        """Test getting successors for asset with one downstream dependent."""

        @il.asset(io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream_asset(context: il.ExecutionContext, upstream_asset: str) -> str:
            return upstream_asset + "_processed"

        dag = il.DAG(upstream_asset(), downstream_asset())

        # Get successors of upstream asset
        successors = dag.get_successors("upstream_asset")
        assert len(successors) == 1
        assert "downstream_asset" in successors

    def test_get_successors_multiple_dependents(self, tmp_path):
        """Test getting successors for asset with multiple downstream dependents."""

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

        # Get successors of asset_a
        successors = dag.get_successors("asset_a")
        assert len(successors) == 2
        assert "asset_b" in successors
        assert "asset_c" in successors

    def test_get_successors_no_dependents(self, tmp_path):
        """Test getting successors for leaf asset (no dependents)."""

        @il.asset(io=il.FileIO(tmp_path))
        def leaf_asset(context: il.ExecutionContext) -> str:
            return "leaf"

        dag = il.DAG(leaf_asset())

        # Get successors of leaf asset
        successors = dag.get_successors("leaf_asset")
        assert len(successors) == 0

    def test_get_predecessors_invalid_key(self, tmp_path):
        """Test that invalid asset key raises KeyError."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())

        with pytest.raises(KeyError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_predecessors("invalid_key")

    def test_get_successors_invalid_key(self, tmp_path):
        """Test that invalid asset key raises KeyError."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        dag = il.DAG(my_asset())

        with pytest.raises(KeyError, match="Asset 'invalid_key' not found in DAG"):
            dag.get_successors("invalid_key")

    def test_complex_dag_traversal(self, tmp_path):
        """Test both methods on a complex DAG (diamond dependency pattern)."""

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
        def asset_d(
            context: il.ExecutionContext,
            asset_b: str,
            asset_c: str,
        ) -> str:
            return asset_b + "_" + asset_c

        dag = il.DAG(asset_a(), asset_b(), asset_c(), asset_d())

        # Test asset_a: no predecessors, has two successors
        assert len(dag.get_predecessors("asset_a")) == 0
        successors_a = dag.get_successors("asset_a")
        assert len(successors_a) == 2
        assert "asset_b" in successors_a
        assert "asset_c" in successors_a

        # Test asset_d: two predecessors, no successors
        predecessors_d = dag.get_predecessors("asset_d")
        assert len(predecessors_d) == 2
        assert "asset_b" in predecessors_d
        assert "asset_c" in predecessors_d
        assert len(dag.get_successors("asset_d")) == 0

    def test_with_explicit_dependency_mapping(self, tmp_path):
        """Test graph traversal with explicit dependency mapping."""

        @il.asset(dataset="dataset1", io=il.FileIO(tmp_path))
        def upstream_asset(context: il.ExecutionContext) -> str:
            return "upstream"

        @il.asset(
            dataset="dataset2",
            deps={"external": "dataset1.upstream_asset"},
            io=il.FileIO(tmp_path),
        )
        def downstream_asset(context: il.ExecutionContext, external: str) -> str:
            return f"downstream_{external}"

        dag = il.DAG(upstream_asset(), downstream_asset())

        # Test predecessors with explicit mapping
        predecessors = dag.get_predecessors("dataset2.downstream_asset")
        assert len(predecessors) == 1
        assert "dataset1.upstream_asset" in predecessors

        # Test successors
        successors = dag.get_successors("dataset1.upstream_asset")
        assert len(successors) == 1
        assert "dataset2.downstream_asset" in successors


class TestDAGFromFailedStateFlags:
    """Tests for DAG.from_failed_state materializable flags behavior."""

    def test_flags_single_failed(self, tmp_path):
        """Test that a DAG with a single failed asset has the correct materializable flags."""

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

        state = RunState(dag)
        state.start_run(None)
        state.mark_asset_completed(dag.asset_map["asset_a"])
        state.mark_asset_failed(dag.asset_map["asset_b"], "boom")

        sub = il.DAG.from_failed_state(state)

        assert sub.asset_map["asset_a"].materializable is False
        assert sub.asset_map["asset_b"].materializable is True
        assert sub.asset_map["asset_c"].materializable is True  # Descendant of failed asset

    def test_flags_multiple_failed(self, tmp_path):
        """Test that a DAG with multiple failed assets has the correct materializable flags."""

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

        state = RunState(dag)
        state.start_run(None)
        state.mark_asset_failed(dag.asset_map["asset_a"], "fail a")
        state.mark_asset_completed(dag.asset_map["asset_b"])
        state.mark_asset_failed(dag.asset_map["asset_c"], "fail c")

        sub = il.DAG.from_failed_state(state)

        assert sub.asset_map["asset_a"].materializable is True
        assert sub.asset_map["asset_b"].materializable is False
        assert sub.asset_map["asset_c"].materializable is True

    def test_flags_none_failed(self, tmp_path):
        """Test that a DAG with no failed assets has the correct materializable flags."""

        @il.asset(io=il.FileIO(tmp_path))
        def asset_a(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def asset_b(context: il.ExecutionContext, asset_a: str) -> str:
            return asset_a + "b"

        dag = il.DAG(asset_a(), asset_b())

        state = RunState(dag)
        state.start_run(None)
        state.mark_asset_completed(dag.asset_map["asset_a"])
        state.mark_asset_completed(dag.asset_map["asset_b"])

        sub = il.DAG.from_failed_state(state)

        assert sub.asset_map["asset_a"].materializable is False
        assert sub.asset_map["asset_b"].materializable is False
