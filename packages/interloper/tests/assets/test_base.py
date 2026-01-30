"""Tests for Asset and AssetDefinition."""

import datetime as dt

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int


class TestAssetDefinition:
    """Tests for AssetDefinition."""

    def test_initialization(self):
        """Test AssetDefinition initialization."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        assert asset_def.func == func
        assert asset_def.name == "func"
        assert asset_def.dataset is None
        assert asset_def.schema is None
        assert asset_def.config is None
        assert asset_def.io is None
        assert asset_def.partitioning is None
        assert asset_def.default_io_key is None
        assert asset_def.deps == {}

    def test_key_without_dataset(self):
        """Test key property without dataset."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, name="my_asset")
        assert asset_def.key == "my_asset"

    def test_key_with_dataset(self):
        """Test key property with dataset."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, name="my_asset", dataset="my_dataset")
        assert asset_def.key == "my_dataset.my_asset"

    def test_key_without_dataset_or_source(self):
        """Test key property without dataset or source context."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, name="my_asset")
        assert asset_def.key == "my_asset"

    def test_deps_initialization(self):
        """Test deps field initialization."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        assert asset_def.deps == {}

    def test_deps_with_explicit_mapping(self):
        """Test deps with explicit dependency mapping."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        deps = {"param1": "dataset1.asset1", "param2": "dataset2.asset2"}
        asset_def = il.AssetDefinition(func, deps=deps)
        assert asset_def.deps == deps

    def test_call_with_deps_override(self):
        """Test __call__ with deps override."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, deps={"param1": "original.asset"})
        asset = asset_def(deps={"param1": "override.asset"})
        assert asset.deps == {"param1": "override.asset"}

    def test_call_with_deps_merge(self):
        """Test __call__ with deps merge."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, deps={"param1": "original.asset"})
        asset = asset_def(deps={"param2": "new.asset"})
        assert asset.deps == {"param1": "original.asset", "param2": "new.asset"}

    def test_custom_name(self):
        """Test AssetDefinition with custom name."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, name="custom_name")
        assert asset_def.name == "custom_name"

    def test_with_all_parameters(self, tmp_path):
        """Test AssetDefinition with all parameters."""
        io = il.FileIO(tmp_path)
        partitioning = il.TimePartitionConfig(column="date")

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(
            func,
            name="test",
            schema=SampleSchema,
            io=io,
            partitioning=partitioning,
            dataset="data",
            default_io_key="default",
        )

        assert asset_def.name == "test"
        assert asset_def.schema == SampleSchema
        assert asset_def.io == io
        assert asset_def.partitioning == partitioning
        assert asset_def.dataset == "data"
        assert asset_def.default_io_key == "default"

    def test_callable_creates_asset_instance(self):
        """Test that calling AssetDefinition creates an Asset instance."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        asset_instance = asset_def()
        assert isinstance(asset_instance, il.Asset)

    def test_callable_with_config_override(self):
        """Test calling AssetDefinition with config override."""
        from pydantic_settings import BaseSettings

        class Config(BaseSettings):
            key: str = "default"

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, config=Config)
        config = Config(key="override")
        asset_instance = asset_def(config=config)
        assert isinstance(asset_instance, il.Asset)

    def test_callable_with_io_override(self):
        """Test calling AssetDefinition with IO override."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, io=il.FileIO("default/"))
        new_io = il.FileIO("override/")
        asset_instance = asset_def(io=new_io)
        assert isinstance(asset_instance, il.Asset)


class TestAsset:
    """Tests for Asset."""

    def test_initialization(self):
        """Test Asset initialization using decorator."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.definition == my_asset
        assert asset_instance.name == "my_asset"
        assert asset_instance.schema is None
        assert asset_instance.config is None
        assert asset_instance.dataset is None
        assert asset_instance.default_io_key is None
        assert isinstance(asset_instance.io, il.MemoryIO)

    def test_key_with_source(self):
        """Test key property with source context."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value"

            return (my_asset,)

        source_instance = my_source()

        assert source_instance.my_asset.key == "my_source.my_asset"

    def test_key_with_source_name_override(self):
        """Test key property with source name override."""

        @il.source(name="new_source_name")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value"

            return (my_asset,)

        source_instance = my_source()
        assert source_instance.my_asset.key == "new_source_name.my_asset"

    def test_key_with_multiple_sources_same_name(self):
        """Test that assets from different sources with same name get different keys."""

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

        source1_instance = source1()
        source2_instance = source2()

        asset1 = source1_instance.assets["my_asset"]
        asset2 = source2_instance.assets["my_asset"]

        # Both assets have the same name but different keys
        assert asset1.name == asset2.name == "my_asset"
        assert asset1.key == "source1.my_asset"
        assert asset2.key == "source2.my_asset"
        assert asset1.key != asset2.key

    def test_key_with_custom_source_name(self):
        """Test key property with custom source name."""

        @il.source(name="custom_source_name")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value"

            return (my_asset,)

        source_instance = my_source()
        asset_instance = source_instance.assets["my_asset"]

        # Asset should have source context with custom name
        assert asset_instance.source is not None
        assert asset_instance.source.name == "custom_source_name"

        # Key should include custom source name
        assert asset_instance.key == "custom_source_name.my_asset"

    def test_with_dataset(self):
        """Test dataset property."""

        @il.asset(dataset="my_dataset")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert asset_instance.dataset == "my_dataset"

    def test_dataset_from_source_name(self):
        """Test dataset property from source."""

        @il.source()
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value"

            return (my_asset,)

        source_instance = my_source()
        assert source_instance.my_asset.dataset == "my_source"

    def test_dataset_from_source_dataset(self):
        """Test dataset property from source."""

        @il.source(dataset="my_dataset")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def my_asset(context: il.ExecutionContext) -> str:
                return "value"

            return (my_asset,)

        source_instance = my_source()
        assert source_instance.my_asset.dataset == "my_dataset"

    def test_deps_initialization(self):
        """Test deps field initialization."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert asset_instance.deps == {}

    def test_deps_with_explicit_mapping(self):
        """Test deps with explicit dependency mapping."""

        @il.asset(deps={"param1": "dataset1.asset1", "param2": "dataset2.asset2"})
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert asset_instance.deps == {"param1": "dataset1.asset1", "param2": "dataset2.asset2"}

    def test_call_with_deps_override(self):
        """Test __call__ with deps override."""

        @il.asset(deps={"param1": "original.asset"})
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(deps={"param1": "override.asset"})
        assert asset_instance.deps == {"param1": "override.asset"}

    def test_call_with_deps_merge(self):
        """Test __call__ with deps merge."""

        @il.asset(deps={"param1": "original.asset"})
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(deps={"param2": "new.asset"})
        assert asset_instance.deps == {"param1": "original.asset", "param2": "new.asset"}

    def test_with_config(self):
        """Test Asset with config using decorator."""
        from pydantic_settings import BaseSettings

        class Config(BaseSettings):
            key: str = "test"

        @il.asset(config=Config)
        def my_asset(context: il.ExecutionContext, config: Config) -> str:
            return "value"

        config = Config()
        asset_instance = my_asset(config=config)
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.config == config

    def test_with_io(self, tmp_path):
        """Test Asset with IO using decorator."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.io is not None

    def test_run_without_partition(self):
        """Test Asset.run() without partition."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        value = asset_instance.run()
        assert value == "value"

    def test_run_with_partition(self):
        """Test Asset.run() with partition."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        asset_instance = my_asset()
        value = asset_instance.run(il.TimePartition(dt.date(2025, 1, 1)))
        assert value == [{"date": dt.date(2025, 1, 1)}]

    def test_run_with_partition_window(self):
        """Test Asset.run() with partition window."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=True))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            start, end = context.partition_date_window
            return [{"start": start, "end": end}]

        asset_instance = my_asset()
        value = asset_instance.run(
            partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        )
        assert value == [{"start": dt.date(2025, 1, 1), "end": dt.date(2025, 1, 7)}]

    def test_materialize_without_partition(self, tmp_path):
        """Test Asset.materialize() without partition."""

        @il.asset(io=il.FileIO(tmp_path))
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        value = asset_instance.materialize()
        assert value == "value"

    def test_materialize_with_partition(self, tmp_path):
        """Test Asset.materialize() with partition."""

        @il.asset(
            io=il.FileIO(tmp_path),
            partitioning=il.TimePartitionConfig(column="date"),
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        asset_instance = my_asset()
        value = asset_instance.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert value == [{"date": dt.date(2025, 1, 1)}]

    def test_materialize_with_multiple_ios(self, tmp_path):
        """Test Asset.materialize() with multiple IOs."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        @il.asset(
            io={
                "local": il.FileIO(str(local_dir)),
                "cloud": il.FileIO(str(cloud_dir)),
            },
            default_io_key="local",
        )
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        value = asset_instance.materialize()
        assert value == "value"

    def test_schema_validation(self):
        """Test schema validation during execution."""

        @il.asset(schema=SampleSchema)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        value = asset_instance.run()
        assert value == "value"

    def test_dependency_resolution(self, tmp_path):
        """Test asset with dependencies requires DAG."""

        @il.asset(io=il.FileIO(tmp_path))
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(tmp_path))
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream + "b"

        # Dependencies cannot be resolved without a DAG
        # This should fail because upstream data needs to be loaded from IO
        downstream_instance = downstream()
        with pytest.raises(ValueError, match="Pass a DAG to run\\(\\) or materialize\\(\\) for dependency resolution"):
            # Will fail: cannot resolve 'upstream' parameter
            value = downstream_instance.run()
            assert value == "ab"

    def test_asset_copy_overrides_and_preserves_fields(self, tmp_path):
        """Asset.copy should override provided fields and preserve others without mutating original."""

        class Cfg(BaseSettings):
            key: str = "x"

        @il.asset(config=Cfg, deps={"a": "ds.up"}, dataset="ds")
        def my_asset(context: il.ExecutionContext, config: Cfg) -> str:
            return "value"

        original = my_asset(io=il.FileIO(tmp_path))

        new_cfg = Cfg(key="y")
        new_io = il.FileIO(tmp_path / "other")
        new_deps = {"b": "ds2.other"}

        copied = original.copy(config=new_cfg, io=new_io, deps=new_deps, dataset="new_ds", materializable=False)

        # Original unchanged
        assert isinstance(original.io, il.FileIO)
        assert original.config is not new_cfg
        assert original.deps == {"a": "ds.up"}
        assert original.dataset == "ds"
        assert original.materializable is True

        # Copied has overrides and preserved identity for func/definition
        assert copied is not original
        assert copied.func is original.func
        assert copied.definition is original.definition
        assert copied.config == new_cfg
        assert copied.io is new_io
        assert copied.deps == new_deps
        assert copied.dataset == "new_ds"
        assert copied.materializable is False

    def test_asset_copy_retains_source_reference(self, tmp_path):
        """Asset.copy should retain the .source reference when the asset is part of a Source."""
        import interloper as il

        io = il.FileIO(tmp_path)

        @il.source
        def src() -> tuple[il.AssetDefinition, ...]:
            @il.asset(io=io)
            def a(context: il.ExecutionContext) -> str:
                return "v"

            return (a,)

        source_instance = src()
        asset_from_source = source_instance.assets["a"]

        copied = asset_from_source.copy()

        # The copied asset should still reference the original source
        assert copied.source is source_instance
        assert copied.key == asset_from_source.key


class TestConfigInference:
    """Tests for config inference and validation."""

    def test_config_inferred_from_env_vars(self, monkeypatch):
        """Test that config can be inferred from environment variables."""
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class EnvConfig(BaseSettings):
            a: str
            b: str

            model_config = SettingsConfigDict(env_prefix="TEST_")

        # Set environment variables
        monkeypatch.setenv("TEST_A", "A")
        monkeypatch.setenv("TEST_B", "B")

        @il.asset(config=EnvConfig)
        def my_asset(context: il.ExecutionContext, config: EnvConfig) -> str:
            assert config.a == "A"
            assert config.b == "B"
            return "value"

        # Should load config from environment automatically
        asset_instance = my_asset()
        value = asset_instance.run()
        assert value == "value"

    def test_config_override_takes_precedence(self, monkeypatch):
        """Test that explicit config override takes precedence over env vars."""
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class EnvConfig(BaseSettings):
            a: str

            model_config = SettingsConfigDict(env_prefix="TEST_")

        # Set environment variable
        monkeypatch.setenv("TEST_A", "A")

        @il.asset(config=EnvConfig)
        def my_asset(context: il.ExecutionContext, config: EnvConfig) -> str:
            return config.a

        # Override should take precedence
        override_config = EnvConfig(a="from_override")
        asset_instance = my_asset(config=override_config)
        value = asset_instance.run()
        assert value == "from_override"

    def test_config_mandatory_when_configured(self):
        """Test that config is mandatory when configured in decorator."""
        from pydantic_settings import BaseSettings

        class RequiredConfig(BaseSettings):
            required_field: str  # No default value, must be provided

        @il.asset(config=RequiredConfig)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        # Should fail when config can't be inferred and no override provided
        with pytest.raises(Exception):
            asset_instance = my_asset()
            asset_instance.run()

    def test_config_not_mandatory_when_not_configured(self):
        """Test that config is not required when not configured."""

        @il.asset  # No config specified
        def my_asset(context: il.ExecutionContext) -> str:
            # No config parameter when not configured
            return "value"

        # Should work fine without config
        asset_instance = my_asset()
        value = asset_instance.run()
        assert value == "value"

    def test_config_with_defaults_works_without_env(self):
        """Test that config with all defaults doesn't require env vars."""
        from pydantic_settings import BaseSettings

        class DefaultConfig(BaseSettings):
            a: str = "A"

        @il.asset(config=DefaultConfig)
        def my_asset(context: il.ExecutionContext, config: DefaultConfig) -> str:
            assert config.a == "A"
            return "value"

        # Should work with defaults
        asset_instance = my_asset()
        asset_instance.run()

    def test_config_parameter_optional_in_signature(self):
        """Test that config parameter is optional in function signature even when configured."""
        from pydantic_settings import BaseSettings

        class OptionalConfig(BaseSettings):
            a: str = "A"

        @il.asset(config=OptionalConfig)
        def my_asset(context: il.ExecutionContext) -> str:
            # Config is configured but not used in function signature
            # This is okay - the function doesn't have to use the config
            return "value"

        # Should work fine
        asset_instance = my_asset()
        asset_instance.run()

    def test_partial_env_config_fails(self, monkeypatch):
        """Test that partial env config fails when required fields are missing."""
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class PartialConfig(BaseSettings):
            a: str
            b: str

            model_config = SettingsConfigDict(env_prefix="TEST_")

        # Only set one of two required fields
        monkeypatch.setenv("TEST_A", "A")
        # TEST_B not set

        @il.asset(config=PartialConfig)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        # Should fail due to missing required field
        with pytest.raises(Exception):
            asset_instance = my_asset()
            asset_instance.run()
