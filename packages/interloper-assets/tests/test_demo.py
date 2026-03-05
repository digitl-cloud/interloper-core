"""Tests for the DemoSource."""

import interloper as il
import pytest
from interloper.serialization.source import SourceSpec

from interloper_assets.demo.source import DemoConfig, DemoSchema, DemoSource, partitioning


class TestDemoConfig:
    """DemoConfig defaults and overrides."""

    def test_default_hello(self):
        """Default hello value is 'world'."""
        config = DemoConfig()
        assert config.hello == "world"

    def test_custom_hello(self):
        """hello can be overridden at construction."""
        config = DemoConfig(hello="custom")
        assert config.hello == "custom"

    def test_is_il_config(self):
        """DemoConfig inherits from il.Config."""
        assert issubclass(DemoConfig, il.Config)


class TestDemoSchema:
    """DemoSchema fields."""

    def test_has_hello_field(self):
        """Schema declares a 'hello' string field."""
        schema = DemoSchema(hello="value")
        assert schema.hello == "value"

    def test_is_asset_schema(self):
        """DemoSchema inherits from il.AssetSchema."""
        assert issubclass(DemoSchema, il.AssetSchema)


class TestDemoSourceDefinition:
    """DemoSource as a SourceDefinition."""

    def test_is_source_definition(self):
        """DemoSource is a SourceDefinition instance."""
        assert isinstance(DemoSource, il.SourceDefinition)

    def test_name(self):
        """Source name is 'DemoSource'."""
        assert DemoSource.name == "DemoSource"

    def test_config_type(self):
        """Source config type is DemoConfig."""
        assert DemoSource.config is DemoConfig

    def test_tags(self):
        """Source is tagged with 'Testing'."""
        assert DemoSource.tags == ("Testing",)

    def test_asset_count(self):
        """Source defines exactly five assets."""
        assert len(DemoSource.asset_defs) == 5

    def test_asset_names(self):
        """Source defines assets a, b, c, d, e."""
        assert set(DemoSource.asset_defs.keys()) == {"a", "b", "c", "d", "e"}


class TestDemoAssetDefinitions:
    """Individual asset definitions on DemoSource."""

    @pytest.fixture()
    def defs(self):
        """Asset definitions dict."""
        return DemoSource.asset_defs

    def test_all_are_asset_definitions(self, defs):
        """Every entry is an AssetDefinition."""
        for asset_def in defs.values():
            assert isinstance(asset_def, il.AssetDefinition)

    def test_all_have_schema(self, defs):
        """Every asset uses DemoSchema."""
        for asset_def in defs.values():
            assert asset_def.schema is DemoSchema

    def test_all_have_partitioning(self, defs):
        """Every asset has time partitioning configured."""
        for asset_def in defs.values():
            assert asset_def.partitioning is partitioning

    def test_all_tagged_report(self, defs):
        """Every asset is tagged with 'Report'."""
        for asset_def in defs.values():
            assert "Report" in asset_def.tags


class TestDemoPartitioning:
    """Partitioning config for the demo source."""

    def test_is_time_partition_config(self):
        """Module-level partitioning is a TimePartitionConfig."""
        assert isinstance(partitioning, il.TimePartitionConfig)

    def test_column(self):
        """Partitioning column is 'date'."""
        assert partitioning.column == "date"

    def test_allow_window_false(self):
        """allow_window is disabled."""
        assert partitioning.allow_window is False


class TestDemoDagStructure:
    """DAG dependency edges inferred from method signatures."""

    @pytest.fixture()
    def defs(self):
        """Asset definitions dict."""
        return DemoSource.asset_defs

    def test_a_has_no_requires(self, defs):
        """Asset 'a' is the root and has no dependencies."""
        assert defs["a"].requires == {}

    def test_b_requires_a(self, defs):
        """Asset 'b' depends on 'a'."""
        assert "a" in defs["b"].requires
        assert defs["b"].requires["a"] == defs["a"].definition_key

    def test_c_requires_a(self, defs):
        """Asset 'c' depends on 'a'."""
        assert "a" in defs["c"].requires
        assert defs["c"].requires["a"] == defs["a"].definition_key

    def test_d_requires_a(self, defs):
        """Asset 'd' depends on 'a'."""
        assert "a" in defs["d"].requires
        assert defs["d"].requires["a"] == defs["a"].definition_key

    def test_e_requires_b_c_d(self, defs):
        """Asset 'e' depends on 'b', 'c', and 'd'."""
        requires = defs["e"].requires
        assert set(requires.keys()) == {"b", "c", "d"}
        assert requires["b"] == defs["b"].definition_key
        assert requires["c"] == defs["c"].definition_key
        assert requires["d"] == defs["d"].definition_key

    def test_dag_shape_fan_out_fan_in(self, defs):
        """DAG has fan-out from a and fan-in to e (diamond shape)."""
        # a has no deps
        assert len(defs["a"].requires) == 0
        # b, c, d each depend only on a
        for name in ("b", "c", "d"):
            assert list(defs[name].requires.keys()) == ["a"]
        # e depends on b, c, d
        assert len(defs["e"].requires) == 3


class TestDemoSourceInstantiation:
    """Instantiating DemoSource into a Source with config."""

    @pytest.fixture()
    def source(self):
        """Instantiate the demo source with default config."""
        return DemoSource(config=DemoConfig())

    def test_returns_source(self, source):
        """Calling the definition returns a Source."""
        assert isinstance(source, il.Source)

    def test_source_name(self, source):
        """Source name matches the definition."""
        assert source.name == "DemoSource"

    def test_asset_count(self, source):
        """Instantiated source has five assets."""
        assert len(source.assets) == 5

    def test_asset_names(self, source):
        """Instantiated source exposes a, b, c, d, e."""
        assert set(source.assets.keys()) == {"a", "b", "c", "d", "e"}

    def test_config_propagated(self, source):
        """Config is set on the source."""
        assert isinstance(source.config, DemoConfig)
        assert source.config.hello == "world"

    def test_custom_config(self):
        """Custom config is propagated."""
        source = DemoSource(config=DemoConfig(hello="custom"))
        assert source.config.hello == "custom"


class TestDemoSourceToSpec:
    """Serialization of DemoSource to a SourceSpec."""

    @pytest.fixture()
    def spec(self):
        """SourceSpec from an instantiated demo source."""
        source = DemoSource(config=DemoConfig())
        return source.to_spec()

    def test_spec_type(self, spec):
        """Spec is a SourceSpec."""
        assert isinstance(spec, SourceSpec)

    def test_spec_has_path(self, spec):
        """Spec contains a non-empty import path."""
        assert isinstance(spec.path, str)
        assert len(spec.path) > 0

    def test_spec_config_dict(self, spec):
        """Spec config is a serialized dict."""
        assert isinstance(spec.config, dict)
        assert spec.config["hello"] == "world"

    def test_spec_assets_list(self, spec):
        """Spec assets lists all five materializable assets."""
        assert isinstance(spec.assets, list)
        assert set(spec.assets) == {"a", "b", "c", "d", "e"}
