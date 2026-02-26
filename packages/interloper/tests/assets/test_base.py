"""Tests for Asset and AssetDefinition."""

import datetime as dt

import pytest
from pydantic import BaseModel

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
        assert asset_def.partitioning is None
        assert asset_def.requires == {}

    def test_requires_initialization(self):
        """Test requires field initialization."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        assert asset_def.requires == {}

    def test_requires_with_explicit_mapping(self):
        """Test requires with explicit definition key mapping."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        requires = {"campaign": "facebook_ads:campaign", "display": "amazon_ads:display"}
        asset_def = il.AssetDefinition(func, requires=requires)
        assert asset_def.requires == requires

    def test_call_with_deps(self):
        """Test __call__ with deps (instance-level only)."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        asset = asset_def(deps={"param1": "source.asset"})
        assert asset.deps == {"param1": "source.asset"}

    def test_call_without_deps(self):
        """Test __call__ without deps results in empty deps."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        asset = asset_def()
        assert asset.deps == {}

    def test_custom_name(self):
        """Test AssetDefinition with custom name."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func, name="custom_name")
        assert asset_def.name == "custom_name"

    def test_invalid_name_rejected(self):
        """Test that invalid names are rejected at definition time."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        with pytest.raises(ValueError, match="invalid"):
            il.AssetDefinition(func, name="my-asset")

        with pytest.raises(ValueError, match="invalid"):
            il.AssetDefinition(func, name="123")

    def test_invalid_name_rejected_at_instantiation(self):
        """Test that invalid name overrides are rejected when calling a definition."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        with pytest.raises(ValueError, match="invalid"):
            my_asset(name="bad-name")

    def test_definition_key_standalone(self):
        """Standalone asset definition_key equals the asset name."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.definition_key == "my_asset"

    def test_definition_key_source_bound(self):
        """Source-bound asset definition_key is source_name:asset_name."""

        @il.source
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        asset_def = MySource.my_asset
        assert asset_def.source_definition is MySource
        assert asset_def.definition_key == "MySource:my_asset"

    def test_source_definition_wired_by_decorator(self):
        """The @source decorator wires source_definition on collected asset defs."""

        @il.source
        class MySource:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def b(self, context: il.ExecutionContext) -> str:
                return "b"

        for asset_def in MySource.asset_defs.values():
            assert asset_def.source_definition is MySource

    def test_source_definition_none_for_standalone(self):
        """Standalone asset definitions have source_definition=None."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.source_definition is None

    def test_with_all_parameters(self):
        """Test AssetDefinition with all parameters."""
        partitioning = il.TimePartitionConfig(column="date")

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(
            func,
            name="test",
            schema=SampleSchema,
            partitioning=partitioning,
            dataset="data",
        )

        assert asset_def.name == "test"
        assert asset_def.schema == SampleSchema
        assert asset_def.partitioning == partitioning
        assert asset_def.dataset == "data"

    def test_callable_creates_asset_instance(self):
        """Test that calling AssetDefinition creates an Asset instance."""

        def func(context: il.ExecutionContext) -> str:
            return "value"

        asset_def = il.AssetDefinition(func)
        asset_instance = asset_def()
        assert isinstance(asset_instance, il.Asset)

    def test_callable_with_config_override(self):
        """Test calling AssetDefinition with config override."""

        class Config(il.Config):
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

        asset_def = il.AssetDefinition(func)
        new_io = il.FileIO("override/")
        asset_instance = asset_def(io=new_io)
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.io == new_io


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
        assert isinstance(asset_instance.io, il.MemoryIO)

    def test_key_with_source(self):
        """Test key property with source context."""

        @il.source
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()

        assert source_instance.my_asset.instance_key == "MySource:my_asset"

    def test_key_with_source_name_override(self):
        """Test key property with source name override."""

        @il.source(name="new_source_name")
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        assert source_instance.my_asset.instance_key == "new_source_name:my_asset"

    def test_key_with_multiple_sources_same_name(self):
        """Test that assets from different sources with same name get different keys."""

        @il.source
        class Source1:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value1"

        @il.source
        class Source2:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value2"

        source1_instance = Source1()
        source2_instance = Source2()

        asset1 = source1_instance.assets["my_asset"]
        asset2 = source2_instance.assets["my_asset"]

        # Both assets have the same name but different keys
        assert asset1.name == asset2.name == "my_asset"
        assert asset1.instance_key == "Source1:my_asset"
        assert asset2.instance_key == "Source2:my_asset"
        assert asset1.instance_key != asset2.instance_key

    def test_key_with_custom_source_name(self):
        """Test key property with custom source name."""

        @il.source(name="custom_source_name")
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        asset_instance = source_instance.assets["my_asset"]

        # Asset should have source context with custom name
        assert asset_instance.source is not None
        assert asset_instance.source.name == "custom_source_name"

        # Key should include custom source name
        assert asset_instance.instance_key == "custom_source_name:my_asset"

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
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        assert source_instance.my_asset.dataset == "MySource"

    def test_dataset_from_source_dataset(self):
        """Test dataset property from source."""

        @il.source(dataset="my_dataset")
        class MySource:
            @il.asset
            def my_asset(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        assert source_instance.my_asset.dataset == "my_dataset"

    def test_deps_initialization(self):
        """Test deps field initialization."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert asset_instance.deps == {}

    def test_deps_with_explicit_mapping(self):
        """Test deps with explicit dependency mapping at instantiation."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(deps={"param1": "dataset1.asset1", "param2": "dataset2.asset2"})
        assert asset_instance.deps == {"param1": "dataset1.asset1", "param2": "dataset2.asset2"}

    def test_call_with_deps_at_instantiation(self):
        """Test deps passed at instantiation time."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(deps={"param1": "override.asset"})
        assert asset_instance.deps == {"param1": "override.asset"}

    def test_call_without_deps_has_empty_deps(self):
        """Test instantiation without deps results in empty deps."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        assert asset_instance.deps == {}

    def test_with_config(self):
        """Test Asset with config using decorator."""

        class Config(il.Config):
            key: str = "test"

        @il.asset(config=Config)
        def my_asset(context: il.ExecutionContext, config: Config) -> str:
            return "value"

        config = Config()
        asset_instance = my_asset(config=config)
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.config == config

    def test_with_io(self, tmp_path):
        """Test Asset with IO passed at call time."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        io = il.FileIO(tmp_path)
        asset_instance = my_asset(io=io)
        assert isinstance(asset_instance, il.Asset)
        assert asset_instance.io is io

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

    def test_run_with_partition_window_not_allowed_raises(self):
        """Test Asset.run() rejects windows when allow_window=False."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=False))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        asset_instance = my_asset()
        with pytest.raises(ValueError, match="does not support windowed runs"):
            asset_instance.run(
                partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
            )

    def test_materialize_without_partition(self, tmp_path):
        """Test Asset.materialize() without partition."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(io=il.FileIO(tmp_path))
        value = asset_instance.materialize()
        assert value == "value"

    def test_materialize_with_partition(self, tmp_path):
        """Test Asset.materialize() with partition."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        asset_instance = my_asset(io=il.FileIO(tmp_path))
        value = asset_instance.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert value == [{"date": dt.date(2025, 1, 1)}]

    def test_materialize_with_multiple_ios(self, tmp_path):
        """Test Asset.materialize() with multiple IOs."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }
        asset_instance = my_asset(io=ios, default_io_key="local")
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
        io = il.FileIO(tmp_path)

        @il.asset
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            return upstream + "b"

        # Dependencies cannot be resolved without a DAG
        # This should fail because upstream data needs to be loaded from IO
        downstream_instance = downstream(io=io)
        with pytest.raises(ValueError, match="Pass a DAG to run\\(\\) or materialize\\(\\) for dependency resolution"):
            # Will fail: cannot resolve 'upstream' parameter
            value = downstream_instance.run()
            assert value == "ab"

    def test_asset_copy_overrides_and_preserves_fields(self, tmp_path):
        """Asset.copy should override provided fields and preserve others without mutating original."""

        class Cfg(il.Config):
            key: str = "x"

        @il.asset(config=Cfg, dataset="ds")
        def my_asset(context: il.ExecutionContext, config: Cfg) -> str:
            return "value"

        original = my_asset(io=il.FileIO(tmp_path), deps={"a": "ds.up"})

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
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "v"

        source_instance = Src(io=io)
        asset_from_source = source_instance.assets["a"]

        copied = asset_from_source.copy()

        # The copied asset should still reference the original source
        assert copied.source is source_instance
        assert copied.instance_key == asset_from_source.instance_key


class TestConfigInference:
    """Tests for config inference and validation."""

    def test_config_inferred_from_env_vars(self, monkeypatch):
        """Test that config can be inferred from environment variables."""
        from pydantic_settings import SettingsConfigDict

        class EnvConfig(il.Config):
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
        from pydantic_settings import SettingsConfigDict

        class EnvConfig(il.Config):
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

        class RequiredConfig(il.Config):
            required_field: str  # No default value, must be provided

        @il.asset(config=RequiredConfig)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        # Should fail when config can't be inferred and no override provided
        with pytest.raises(ValueError):
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

        class DefaultConfig(il.Config):
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

        class OptionalConfig(il.Config):
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
        from pydantic_settings import SettingsConfigDict

        class PartialConfig(il.Config):
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
        with pytest.raises(ValueError):
            asset_instance = my_asset()
            asset_instance.run()
