"""Tests for AssetSpec serialization and reconstruction."""

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization import AssetSpec, IOSpec

# Module-level source for serialization roundtrip tests
_closed_over_value = "secret_value"


@il.source(dataset="closure_source")
class ClosureTestSource:
    """Source at module level for serialization tests."""

    @il.asset
    def my_asset(self, context: il.ExecutionContext) -> str:
        return f"Asset has access to: {_closed_over_value}"


class TestAssetSpec:
    """Tests for AssetSpec serialization and reconstruction."""

    def test_assetspec_simple_roundtrip(self):
        """Test AssetSpec roundtrip with simple asset."""

        # Create a simple asset
        @il.asset
        def simple_asset():
            return "value"

        # Convert to spec
        spec = simple_asset().to_spec()
        assert isinstance(spec, AssetSpec)
        # Module path may be prefixed (e.g., packages.interloper.tests...)
        assert spec.path.split(".")[-1] == "simple_asset"
        assert "test_asset" in spec.path or "serialization" in spec.path

        # Test that the spec can be serialized to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Test that the spec can be parsed from JSON
        parsed_spec = AssetSpec.model_validate_json(json_str)
        assert parsed_spec.path == spec.path
        assert parsed_spec.io == spec.io

    def test_assetspec_with_config_roundtrip(self):
        """Test AssetSpec roundtrip with config."""

        class TestConfig(il.Config):
            api_key: str = "default_key"
            endpoint: str = "https://api.example.com"

        @il.asset(config=TestConfig)
        def config_asset(config):
            return f"config_{config.api_key}"

        # Convert to spec
        spec = config_asset().to_spec()
        assert spec.path.split(".")[-1] == "config_asset"
        assert "test_asset" in spec.path or "serialization" in spec.path

        # Test JSON serialization
        json_str = spec.model_dump_json()
        parsed_spec = AssetSpec.model_validate_json(json_str)
        assert parsed_spec.path == spec.path

    def test_assetspec_with_io_roundtrip(self):
        """Test AssetSpec roundtrip with IO."""
        file_io = il.FileIO(base_path="data")

        @il.asset
        def io_asset():
            return "value"

        # Convert to spec
        spec = io_asset(io=file_io).to_spec()
        assert spec.path.split(".")[-1] == "io_asset"
        assert "test_asset" in spec.path or "serialization" in spec.path
        assert isinstance(spec.io, IOSpec)
        assert spec.io.path == "interloper.io.file.FileIO"
        assert spec.io.init["base_path"] == "data"

        # Test JSON serialization
        json_str = spec.model_dump_json()
        parsed_spec = AssetSpec.model_validate_json(json_str)
        assert parsed_spec.path == spec.path
        assert isinstance(parsed_spec.io, IOSpec)

    def test_assetspec_with_multiple_io_roundtrip(self):
        """Test AssetSpec roundtrip with multiple IOs."""
        file_io1 = il.FileIO(base_path="data1")
        file_io2 = il.FileIO(base_path="data2")

        @il.asset
        def multi_io_asset():
            return "value"

        # Convert to spec
        spec = multi_io_asset(io={"io1": file_io1, "io2": file_io2}, default_io_key="io1").to_spec()
        assert spec.path.split(".")[-1] == "multi_io_asset"
        assert "test_asset" in spec.path or "serialization" in spec.path
        assert isinstance(spec.io, dict)
        assert "io1" in spec.io
        assert "io2" in spec.io
        assert isinstance(spec.io["io1"], IOSpec)
        assert isinstance(spec.io["io2"], IOSpec)

        # Test JSON serialization
        json_str = spec.model_dump_json()
        parsed_spec = AssetSpec.model_validate_json(json_str)
        assert parsed_spec.path == spec.path
        assert isinstance(parsed_spec.io, dict)
        assert "io1" in parsed_spec.io
        assert "io2" in parsed_spec.io

    def test_assetspec_validation(self):
        """Test AssetSpec Pydantic validation."""
        # Valid spec
        spec = AssetSpec(path="myapp.assets.my_asset")
        assert spec.path == "myapp.assets.my_asset"
        assert spec.io is None

        # Invalid path (None should be invalid)
        with pytest.raises(ValidationError):
            AssetSpec(path=None)  # type: ignore[arg-type]

        # Missing required field
        with pytest.raises(ValidationError):
            AssetSpec()  # type: ignore[call-arg]

    def test_assetspec_json_serialization(self):
        """Test AssetSpec JSON serialization."""
        spec = AssetSpec(path="myapp.assets.my_asset")

        # Convert to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Parse from JSON
        parsed = AssetSpec.model_validate_json(json_str)
        assert parsed.path == spec.path


class TestSourcedAssetSerialization:
    """Tests for serializing assets that belong to sources."""

    def test_asset_from_source_serialization(self):
        """Asset from a source can be serialized."""
        source_instance = ClosureTestSource()
        asset_instance = source_instance.assets["my_asset"]

        spec = asset_instance.to_spec()
        assert ":" in spec.path
        assert "ClosureTestSource:my_asset" in spec.path

    def test_asset_from_source_reconstruction(self):
        """Reconstructed sourced asset preserves closure."""
        source_instance = ClosureTestSource()
        asset_instance = source_instance.assets["my_asset"]

        spec = asset_instance.to_spec()
        reconstructed = spec.reconstruct()
        result = reconstructed.run()
        assert result == "Asset has access to: secret_value"

