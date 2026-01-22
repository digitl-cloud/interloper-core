"""Tests for Source and SourceDefinition."""

import pytest
from pydantic_settings import BaseSettings

import interloper as il


class SampleConfig(BaseSettings):
    """Sample config."""

    api_key: str = "test"


class TestSourceDefinition:
    """Tests for SourceDefinition."""

    def test_initialization(self):
        """Test SourceDefinition initialization."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        assert isinstance(my_source, il.SourceDefinition)
        assert my_source.config is None
        assert my_source.name == "my_source"
        assert my_source.dataset == "my_source"
        assert my_source.io is None

    def test_with_config(self):
        """Test SourceDefinition with config."""

        @il.source(config=SampleConfig)
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        assert my_source.config == SampleConfig

    def test_with_dataset(self):
        """Test SourceDefinition with dataset."""

        @il.source(dataset="data")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        assert my_source.dataset == "data"

    def test_with_io(self, tmp_path):
        """Test SourceDefinition with IO."""
        io = il.FileIO(tmp_path)

        @il.source(io=io)
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        assert my_source.io == io

    def test_with_all_parameters(self, tmp_path):
        """Test SourceDefinition with all parameters."""
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        ios = {"local": il.FileIO(str(local_dir))}

        @il.source(config=SampleConfig, dataset="data", io=ios, default_io_key="local")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        assert my_source.config == SampleConfig
        assert my_source.dataset == "data"
        assert my_source.io == ios
        assert my_source.default_io_key == "local"

    def test_callable_with_config_override(self):
        """Test calling SourceDefinition with config override."""

        @il.source(config=SampleConfig)
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        config = SampleConfig(api_key="override")
        source_instance = my_source(config=config)
        assert source_instance.config == config

    def test_callable_with_asset_config_override(self):
        """Test calling SourceDefinition with config override."""

        @il.source()
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset(config=SampleConfig)
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        config = SampleConfig(api_key="override")
        source_instance = my_source(config=config)
        assert source_instance.asset1.config == config

    def test_callable_with_io_override(self):
        """Test calling SourceDefinition with config override."""

        @il.source(io=il.FileIO("data"))
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        io = il.FileIO("data2")
        source_instance = my_source(io=io)
        assert source_instance.io == io

    def test_callable_with_asset_io_override(self):
        """Test calling SourceDefinition with asset IO override."""

        @il.source()
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset(io=il.FileIO("data"))
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        io = il.FileIO("data2")
        source_instance = my_source(io=io)
        assert source_instance.assets["asset1"].io == io

    def test_callable_with_config_type_error(self):
        """Test calling SourceDefinition with config type error."""

        @il.source(config=SampleConfig)
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        class WrongConfig(BaseSettings):
            api_key: str = "wrong"

        with pytest.raises(
            TypeError, match=r"Config provided to source 'my_source' must be of type SampleConfig, got WrongConfig."
        ):
            my_source(config=WrongConfig())

    def test_callable_with_asset_config_override_type_mismatch(self):
        """Test calling SourceDefinition with asset config override type mismatch.

        The config override should be ignored and a warning should be printed.
        """

        @il.source()
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset(config=SampleConfig)
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        class WrongConfig(BaseSettings):
            api_key: str = "wrong"

        source_instance = my_source(config=WrongConfig())
        assert source_instance.assets["asset1"].config == SampleConfig()


class TestSource:
    """Tests for Source."""

    def test_initialization(self):
        """Test Source initialization using decorator."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        source_instance = my_source()
        assert isinstance(source_instance, il.Source)
        assert source_instance.definition == my_source

    def test_with_config(self):
        """Test Source with config using decorator."""

        @il.source(config=SampleConfig)
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        config = SampleConfig()
        source_instance = my_source(config=config)
        assert isinstance(source_instance, il.Source)
        assert source_instance.config == config

    def test_asset_access_by_name(self):
        """Test accessing assets by name as attributes."""

        @il.source
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset_a(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(context: il.ExecutionContext) -> str:
                return "b"

            return (asset_a, asset_b)

        source_instance = my_source()
        # Should be able to access assets by name
        assert source_instance.asset_a.name == "asset_a"
        assert source_instance.asset_b.name == "asset_b"

    def test_config_inheritance(self):
        """Test that assets inherit config from source."""

        @il.source(config=SampleConfig)
        def my_source(config: SampleConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        config = SampleConfig(api_key="inherited")
        source_instance = my_source(config=config)
        # Assets should inherit the config
        assert source_instance.assets["asset1"].config == config
        assert source_instance.config == config

    def test_io_inheritance(self, tmp_path):
        """Test that assets inherit IO from source."""
        io = il.FileIO(tmp_path)

        @il.source(io=io)
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        source_instance = my_source()
        # Assets should inherit the IO
        assert source_instance.assets["asset1"].io is io
        assert source_instance.io is io

    def test_dataset_inheritance(self):
        """Test that assets inherit dataset from source."""

        @il.source(dataset="my_dataset")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        source_instance = my_source()
        # Assets should inherit the dataset
        assert source_instance.assets["asset1"].dataset == "my_dataset"
        assert source_instance.assets["asset1"].key == "my_dataset.asset1"

    def test_dataset_defaults_to_source_name(self):
        """Test that assets default to source name as dataset when source dataset is not set."""

        @il.source  # No dataset specified
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            @il.asset
            def asset2(context: il.ExecutionContext, asset1: str) -> str:
                return asset1

            return (asset1, asset2)

        source_instance = my_source()
        # Assets should default to source name as dataset
        assert source_instance.assets["asset1"].dataset == "my_source"
        assert source_instance.assets["asset1"].key == "my_source.asset1"
        assert source_instance.assets["asset2"].dataset == "my_source"
        assert source_instance.assets["asset2"].key == "my_source.asset2"

        # DAG should be able to resolve dependencies
        dag = il.DAG(source_instance)
        assert "my_source.asset1" in dag.asset_map
        assert "my_source.asset2" in dag.asset_map
        assert "my_source.asset1" in dag.get_predecessors("my_source.asset2")

    def test_asset_level_override(self):
        """Test that asset-level parameters override source-level."""

        @il.source(io=il.FileIO("source/"), dataset="source_dataset")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def inherits(context: il.ExecutionContext) -> str:
                return "a"

            @il.asset(io=il.FileIO("asset/"), dataset="asset_dataset")
            def overrides(context: il.ExecutionContext) -> str:
                return "b"

            return (inherits, overrides)

        source_instance = my_source()
        # First asset should inherit, second should use its own
        assert source_instance.assets["inherits"].dataset == "source_dataset"
        assert source_instance.assets["inherits"].io.base_path == "source/"
        assert source_instance.assets["overrides"].dataset == "asset_dataset"
        assert source_instance.assets["overrides"].io.base_path == "asset/"

    def test_default_io_key_inheritance(self, tmp_path):
        """Test that assets inherit default_io_key from source."""
        ios = {
            "path1": il.FileIO(str(tmp_path / "path1")),
            "path2": il.FileIO(str(tmp_path / "path2")),
        }

        @il.source(io=ios, default_io_key="path1")
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        source_instance = my_source()
        # Assets should inherit the default_io_key
        assert source_instance.default_io_key == "path1"
        assert source_instance.assets["asset1"].default_io_key == "path1"

    def test_source_copy_overrides_config_and_io(self, tmp_path):
        """Source.copy should override provided config and IO without mutating original."""

        class Cfg(BaseSettings):
            api_key: str = "a"

        io1 = il.FileIO(tmp_path)
        io2 = il.FileIO(tmp_path / "other")

        @il.source(config=Cfg, io=io1)
        def src(config: Cfg) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def a(context: il.ExecutionContext) -> str:
                return "v"

            return (a,)

        original = src()

        new_cfg = Cfg(api_key="b")
        copied = original.copy(config=new_cfg, io=io2)

        # Original unchanged
        assert original.config is None or original.config != new_cfg
        assert original.io is io1

        # Copied has overrides
        assert copied is not original
        assert copied.config == new_cfg
        assert copied.io is io2

    def test_source_copy_does_not_rewire_asset_source(self, tmp_path):
        """Document current behavior: shallow Source.copy retains assets pointing to original source."""
        import interloper as il

        @il.source
        def src() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def a(context: il.ExecutionContext) -> str:
                return "v"

            return (a,)

        original = src()
        copied = original.copy()

        # Assets dict is the same objects
        assert copied.assets is original.assets
        assert copied.assets["a"] is original.assets["a"]

        # Asset.source still points to the original source
        assert original.assets["a"].source is original
        assert copied.assets["a"].source is original


class TestSourceConfigInference:
    """Tests for source config inference and validation."""

    def test_config_inferred_from_env_vars(self, monkeypatch):
        """Test that source config can be inferred from environment variables."""
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class EnvConfig(BaseSettings):
            a: str
            b: str

            model_config = SettingsConfigDict(env_prefix="SOURCE_TEST_")

        # Set environment variables
        monkeypatch.setenv("SOURCE_TEST_A", "A")
        monkeypatch.setenv("SOURCE_TEST_B", "B")

        @il.source(config=EnvConfig)
        def my_source(config: EnvConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext, config: EnvConfig) -> str:
                assert config.a == "A"
                assert config.b == "B"
                return "value"

            return (asset1,)

        # Should load config from environment automatically
        source_instance = my_source()
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_override_takes_precedence(self, monkeypatch):
        """Test that explicit config override takes precedence over env vars."""
        from pydantic_settings import BaseSettings, SettingsConfigDict

        class EnvConfig(BaseSettings):
            a: str

            model_config = SettingsConfigDict(env_prefix="SOURCE_TEST_")

        # Set environment variable
        monkeypatch.setenv("SOURCE_TEST_A", "A")

        @il.source(config=EnvConfig)
        def my_source(config: EnvConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext, config: EnvConfig) -> str:
                # Should use override, not env
                return config.a

            return (asset1,)

        # Override should take precedence
        override_config = EnvConfig(a="from_override")
        source_instance = my_source(config=override_config)
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_mandatory_when_configured(self):
        """Test that config is mandatory when configured in source decorator."""
        from pydantic_settings import BaseSettings

        class RequiredConfig(BaseSettings):
            required_field: str
            # No default value, must be provided

        @il.source(config=RequiredConfig)
        def my_source(config: RequiredConfig) -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        # Should fail when config can't be inferred and no override provided
        with pytest.raises(Exception):
            my_source()

    def test_config_not_mandatory_when_not_configured(self):
        """Test that config is not required when not configured in source."""

        @il.source  # No config specified
        def my_source() -> tuple[il.AssetDefinition, ...]:
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                # No config parameter when not configured
                return "value"

            return (asset1,)

        # Should work fine without config
        source_instance = my_source()
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_parameter_optional_in_signature(self):
        """Test that config parameter is optional in function signature even when configured."""
        from pydantic_settings import BaseSettings

        class OptionalConfig(BaseSettings):
            api_key: str = "default_key"

        @il.source(config=OptionalConfig)
        def my_source() -> tuple[il.AssetDefinition, ...]:
            # Config is configured but not in function signature
            # This is okay - the function doesn't have to use the config
            @il.asset
            def asset1(context: il.ExecutionContext) -> str:
                return "value"

            return (asset1,)

        # Should work fine
        source_instance = my_source()
        dag = il.DAG(source_instance)
        dag.materialize()
