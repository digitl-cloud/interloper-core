"""Test DAGSpec serialization and reconstruction."""

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization import AssetSpec, DAGSpec


class TestDAGSpec:
    """Test DAGSpec serialization and reconstruction."""

    def test_dagspec_roundtrip(self):
        """Test DAGSpec roundtrip."""

        # Create a simple DAG
        @il.asset
        def asset_a():
            return "a"

        @il.asset
        def asset_b(asset_a):
            return f"b_{asset_a}"

        dag = il.DAG(asset_a(), asset_b())

        # Convert to spec
        spec = dag.to_spec()
        assert isinstance(spec, DAGSpec)
        assert len(spec.assets) == 2
        assert all(isinstance(asset_spec, AssetSpec) for asset_spec in spec.assets)

        # Test JSON serialization
        json_str = spec.model_dump_json()
        parsed_spec = DAGSpec.model_validate_json(json_str)
        assert len(parsed_spec.assets) == 2
        assert all(isinstance(asset_spec, AssetSpec) for asset_spec in parsed_spec.assets)

    def test_dagspec_validation(self):
        """Test DAGSpec Pydantic validation."""
        # Valid spec
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        spec = DAGSpec(assets=[asset_spec])
        assert len(spec.assets) == 1

        # Invalid asset_specs
        with pytest.raises(ValidationError):
            DAGSpec(assets="not_a_list")  # type: ignore[arg-type]

        # Missing required field
        with pytest.raises(ValidationError):
            DAGSpec()  # type: ignore[call-arg]

    def test_dagspec_json_serialization(self):
        """Test DAGSpec JSON serialization."""
        asset_spec = AssetSpec(path="myapp.assets.my_asset")
        spec = DAGSpec(assets=[asset_spec])

        # Convert to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Parse from JSON
        parsed = DAGSpec.model_validate_json(json_str)
        assert len(parsed.assets) == 1
        assert parsed.assets[0].path == asset_spec.path

