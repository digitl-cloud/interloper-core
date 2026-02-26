"""Tests for Source and SourceDefinition."""

import pytest

import interloper as il


class SampleConfig(il.Config):
    """Sample config."""

    api_key: str = "test"


class TestSourceDefinition:
    """Tests for SourceDefinition."""

    def test_initialization(self):
        """Test SourceDefinition initialization."""

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

            @il.asset
            def asset2(self, context: il.ExecutionContext) -> str:
                return "value"

        assert isinstance(MySource, il.SourceDefinition)
        assert len(MySource.asset_defs) == 2
        assert all(isinstance(ad, il.AssetDefinition) for ad in MySource.asset_defs.values())
        assert MySource.name == "MySource"
        assert MySource.dataset is None
        assert MySource.config is None

    def test_invalid_name_rejected(self):
        """Test that invalid source names are rejected at definition time."""
        with pytest.raises(ValueError, match="invalid"):

            @il.source(name="my-source")
            class BadSource:
                @il.asset
                def asset1(self, context: il.ExecutionContext) -> str:
                    return "value"

    def test_invalid_instance_name_rejected(self):
        """Test that invalid name overrides are rejected when instantiating a source."""

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        with pytest.raises(ValueError, match="invalid"):
            MySource(name="bad-name")

    def test_with_config(self):
        """Test SourceDefinition with config."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        assert MySource.config == SampleConfig

    def test_with_dataset(self):
        """Test SourceDefinition with dataset."""

        @il.source(dataset="data")
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        assert MySource.dataset == "data"

    def test_callable_with_config_override(self):
        """Test calling SourceDefinition with config override."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        config = SampleConfig(api_key="override")
        source_instance = MySource(config=config)
        assert source_instance.config == config

    def test_callable_with_asset_config_override(self):
        """Test calling SourceDefinition with config override propagated to assets."""

        @il.source()
        class MySource:
            @il.asset(config=SampleConfig)
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        config = SampleConfig(api_key="override")
        source_instance = MySource(config=config)
        assert source_instance.asset1.config == config

    def test_callable_with_io_override(self):
        """Test calling SourceDefinition with IO override."""

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        io = il.FileIO("data2")
        source_instance = MySource(io=io)
        assert source_instance.io == io

    def test_callable_with_asset_io_override(self):
        """Test calling SourceDefinition with asset IO override."""

        @il.source()
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        io = il.FileIO("data2")
        source_instance = MySource(io=io)
        assert source_instance.assets["asset1"].io == io

    def test_callable_with_config_type_error(self):
        """Test calling SourceDefinition with config type error."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        class WrongConfig(il.Config):
            api_key: str = "wrong"

        with pytest.raises(
            TypeError, match=r"Config provided to source 'MySource' must be of type SampleConfig, got WrongConfig."
        ):
            MySource(config=WrongConfig())

    def test_callable_with_asset_config_override_type_mismatch(self):
        """Test calling SourceDefinition with asset config override type mismatch.

        The config override should be ignored and a warning should be printed.
        """

        @il.source()
        class MySource:
            @il.asset(config=SampleConfig)
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        class WrongConfig(il.Config):
            api_key: str = "wrong"

        source_instance = MySource(config=WrongConfig())
        assert source_instance.assets["asset1"].config == SampleConfig()

    def test_callable_with_assets_filter(self):
        """Test calling SourceDefinition with assets filter."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(self, context: il.ExecutionContext) -> str:
                return "b"

            @il.asset
            def asset_c(self, context: il.ExecutionContext) -> str:
                return "c"

        # Filter to only asset_a and asset_c
        source_instance = MySource(assets=["asset_a", "asset_c"])
        assert set(source_instance.assets.keys()) == {"asset_a", "asset_c"}
        assert "asset_b" not in source_instance.assets

    def test_callable_with_assets_filter_single(self):
        """Test calling SourceDefinition with single asset filter."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(self, context: il.ExecutionContext) -> str:
                return "b"

        # Filter to only asset_b
        source_instance = MySource(assets=["asset_b"])
        assert set(source_instance.assets.keys()) == {"asset_b"}

    def test_callable_with_assets_filter_invalid_name(self):
        """Test calling SourceDefinition with invalid asset name raises ValueError."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(self, context: il.ExecutionContext) -> str:
                return "b"

        with pytest.raises(
            ValueError,
            match=r"Invalid asset names: \['nonexistent'\]. Valid asset names are: \['asset_a', 'asset_b'\].",
        ):
            MySource(assets=["nonexistent"])

    def test_callable_with_assets_filter_multiple_invalid_names(self):
        """Test calling SourceDefinition with multiple invalid asset names."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

        with pytest.raises(
            ValueError,
            match=r"Invalid asset names: \['invalid1', 'invalid2'\]. Valid asset names are: \['asset_a'\].",
        ):
            MySource(assets=["invalid1", "invalid2"])

    def test_callable_with_assets_filter_empty_list(self):
        """Test calling SourceDefinition with empty assets list."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

        # Empty list should result in no assets
        source_instance = MySource(assets=[])
        assert source_instance.assets == {}

    def test_callable_with_assets_filter_none_includes_all(self):
        """Test calling SourceDefinition with assets=None includes all assets."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(self, context: il.ExecutionContext) -> str:
                return "b"

        # None (default) should include all assets
        source_instance = MySource(assets=None)
        assert set(source_instance.assets.keys()) == {"asset_a", "asset_b"}


class TestRequiresInference:
    """Tests for auto-inference of requires from function param names."""

    def test_infers_requires_from_sibling_param_name(self):
        """Param matching a sibling asset name gets auto-wired in requires."""

        @il.source
        class Src:
            @il.asset
            def raw(self, context: il.ExecutionContext) -> str:
                return "data"

            @il.asset
            def transformed(self, context: il.ExecutionContext, raw: str) -> str:
                return raw.upper()

        assert Src.asset_defs["transformed"].requires == {
            "raw": il.AssetDefinitionKey("Src:raw"),
        }
        assert Src.asset_defs["raw"].requires == {}

    def test_no_self_reference(self):
        """An asset whose param name matches its own name is not inferred."""

        @il.source
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext, a: str) -> str:
                return a

        assert Src.asset_defs["a"].requires == {}

    def test_explicit_requires_not_overwritten(self):
        """Explicit requires entries take precedence over inference."""

        @il.source
        class Src:
            @il.asset
            def raw(self, context: il.ExecutionContext) -> str:
                return "data"

            @il.asset(requires={"raw": il.AssetDefinitionKey("other_source:raw")})
            def transformed(self, context: il.ExecutionContext, raw: str) -> str:
                return raw

        assert Src.asset_defs["transformed"].requires == {
            "raw": il.AssetDefinitionKey("other_source:raw"),
        }

    def test_mixed_inferred_and_explicit(self):
        """Explicit requires on one param, inferred on another."""

        @il.source
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def b(self, context: il.ExecutionContext) -> str:
                return "b"

            @il.asset(requires={"a": il.AssetDefinitionKey("other:a")})
            def c(self, context: il.ExecutionContext, a: str, b: str) -> str:
                return a + b

        requires = Src.asset_defs["c"].requires
        assert requires["a"] == il.AssetDefinitionKey("other:a")
        assert requires["b"] == il.AssetDefinitionKey("Src:b")

    def test_non_matching_params_ignored(self):
        """Params that don't match any sibling asset are left alone."""

        @il.source
        class Src:
            @il.asset
            def raw(self, context: il.ExecutionContext) -> str:
                return "data"

            @il.asset
            def transformed(self, context: il.ExecutionContext, raw: str, external: str) -> str:
                return raw + external

        assert Src.asset_defs["transformed"].requires == {
            "raw": il.AssetDefinitionKey("Src:raw"),
        }
        assert "external" not in Src.asset_defs["transformed"].requires

    def test_chain_inference(self):
        """Multi-step chain: a -> b -> c, all inferred."""

        @il.source
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def b(self, context: il.ExecutionContext, a: str) -> str:
                return a

            @il.asset
            def c(self, context: il.ExecutionContext, b: str) -> str:
                return b

        assert Src.asset_defs["a"].requires == {}
        assert Src.asset_defs["b"].requires == {"a": il.AssetDefinitionKey("Src:a")}
        assert Src.asset_defs["c"].requires == {"b": il.AssetDefinitionKey("Src:b")}


class TestSource:
    """Tests for Source."""

    def test_initialization(self):
        """Test Source initialization using decorator."""

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        assert isinstance(source_instance, il.Source)
        assert source_instance.definition == MySource

    def test_with_config(self):
        """Test Source with config using decorator."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        config = SampleConfig()
        source_instance = MySource(config=config)
        assert isinstance(source_instance, il.Source)
        assert source_instance.config == config

    def test_asset_access_by_name(self):
        """Test accessing assets by name as attributes."""

        @il.source
        class MySource:
            @il.asset
            def asset_a(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset
            def asset_b(self, context: il.ExecutionContext) -> str:
                return "b"

        source_instance = MySource()
        # Should be able to access assets by name
        assert source_instance.asset_a.name == "asset_a"
        assert source_instance.asset_b.name == "asset_b"

    def test_config_inheritance(self):
        """Test that assets inherit config from source."""

        @il.source(config=SampleConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        config = SampleConfig(api_key="inherited")
        source_instance = MySource(config=config)
        # Assets should inherit the config
        assert source_instance.assets["asset1"].config == config
        assert source_instance.config == config

    def test_io_inheritance(self, tmp_path):
        """Test that assets inherit IO from source when passed at call time."""
        io = il.FileIO(tmp_path)

        @il.source
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource(io=io)
        # Assets should inherit the IO
        assert source_instance.assets["asset1"].io is io
        assert source_instance.io is io

    def test_dataset_inheritance(self):
        """Test that assets inherit dataset from source."""

        @il.source(dataset="my_dataset")
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        source_instance = MySource()
        # Assets should inherit the dataset
        assert source_instance.assets["asset1"].dataset == "my_dataset"

    def test_dataset_defaults_to_source_name(self):
        """Test that assets default to source name as dataset when source dataset is not set."""

        @il.source  # No dataset specified
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

            @il.asset
            def asset2(self, context: il.ExecutionContext, asset1: str) -> str:
                return asset1

        source_instance = MySource()
        # Assets should default to source name as dataset
        assert source_instance.assets["asset1"].dataset == "MySource"
        assert source_instance.assets["asset1"].instance_key == "MySource:asset1"
        assert source_instance.assets["asset2"].dataset == "MySource"
        assert source_instance.assets["asset2"].instance_key == "MySource:asset2"

        # DAG should be able to resolve dependencies
        dag = il.DAG(source_instance)
        assert "MySource:asset1" in dag.asset_map
        assert "MySource:asset2" in dag.asset_map
        assert "MySource:asset1" in dag.get_predecessors("MySource:asset2")

    def test_asset_level_dataset_override(self):
        """Test that asset-level dataset overrides source-level."""

        @il.source(dataset="source_dataset")
        class MySource:
            @il.asset
            def inherits(self, context: il.ExecutionContext) -> str:
                return "a"

            @il.asset(dataset="asset_dataset")
            def overrides(self, context: il.ExecutionContext) -> str:
                return "b"

        source_instance = MySource()
        # First asset should inherit, second should use its own
        assert source_instance.assets["inherits"].dataset == "source_dataset"
        assert source_instance.assets["overrides"].dataset == "asset_dataset"

    def test_source_copy_overrides_config_and_io(self, tmp_path):
        """Source.copy should override provided config and IO without mutating original."""

        class Cfg(il.Config):
            api_key: str = "a"

        io1 = il.FileIO(tmp_path)
        io2 = il.FileIO(tmp_path / "other")

        @il.source(config=Cfg)
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "v"

        original = Src(io=io1)

        new_cfg = Cfg(api_key="b")
        copied = original.copy(config=new_cfg, io=io2)

        # Original unchanged
        assert original.config is None or original.config != new_cfg
        assert original.io is io1

        # Copied has overrides
        assert copied is not original
        assert copied.config == new_cfg
        assert copied.io is io2

    def test_source_copy_produces_independent_copy(self):
        """Source.copy produces independent assets that point to the new source."""

        @il.source
        class Src:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "v"

        original = Src()
        copied = original.copy()

        # Assets are independent copies
        assert copied.assets is not original.assets
        assert copied.assets["a"] is not original.assets["a"]

        # Each asset points to its own source
        assert original.assets["a"].source is original
        assert copied.assets["a"].source is copied


class TestSourceConfigInference:
    """Tests for source config inference and validation."""

    def test_config_inferred_from_env_vars(self, monkeypatch):
        """Test that source config can be inferred from environment variables."""
        from pydantic_settings import SettingsConfigDict

        class EnvConfig(il.Config):
            a: str
            b: str

            model_config = SettingsConfigDict(env_prefix="SOURCE_TEST_")

        # Set environment variables
        monkeypatch.setenv("SOURCE_TEST_A", "A")
        monkeypatch.setenv("SOURCE_TEST_B", "B")

        @il.source(config=EnvConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                assert self.config.a == "A"
                assert self.config.b == "B"
                return "value"

        # Should load config from environment automatically
        source_instance = MySource()
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_override_takes_precedence(self, monkeypatch):
        """Test that explicit config override takes precedence over env vars."""
        from pydantic_settings import SettingsConfigDict

        class EnvConfig(il.Config):
            a: str

            model_config = SettingsConfigDict(env_prefix="SOURCE_TEST_")

        # Set environment variable
        monkeypatch.setenv("SOURCE_TEST_A", "A")

        @il.source(config=EnvConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                # Should use override, not env
                return self.config.a

        # Override should take precedence
        override_config = EnvConfig(a="from_override")
        source_instance = MySource(config=override_config)
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_mandatory_when_configured(self):
        """Test that config is mandatory when configured in source decorator."""

        class RequiredConfig(il.Config):
            required_field: str
            # No default value, must be provided

        @il.source(config=RequiredConfig)
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        # Should fail when config can't be inferred and no override provided
        with pytest.raises(ValueError):
            MySource()

    def test_config_not_mandatory_when_not_configured(self):
        """Test that config is not required when not configured in source."""

        @il.source  # No config specified
        class MySource:
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                # No config parameter when not configured
                return "value"

        # Should work fine without config
        source_instance = MySource()
        dag = il.DAG(source_instance)
        dag.materialize()

    def test_config_parameter_optional_in_signature(self):
        """Test that config parameter is optional in setup signature even when configured."""

        class OptionalConfig(il.Config):
            api_key: str = "default_key"

        @il.source(config=OptionalConfig)
        class MySource:
            # Config is configured but no setup method defined
            # This is okay - the class doesn't have to use the config
            @il.asset
            def asset1(self, context: il.ExecutionContext) -> str:
                return "value"

        # Should work fine
        source_instance = MySource()
        dag = il.DAG(source_instance)
        dag.materialize()
