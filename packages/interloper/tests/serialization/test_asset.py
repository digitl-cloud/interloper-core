"""Test AssetSpec serialization and reconstruction."""

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization import AssetSpec, IOSpec


class TestAssetSpec:
    """Test AssetSpec serialization and reconstruction."""

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
        from pydantic_settings import BaseSettings

        class TestConfig(BaseSettings):
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

        @il.asset(io=file_io)
        def io_asset():
            return "value"

        # Convert to spec
        spec = io_asset().to_spec()
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

        @il.asset(
            io={"io1": file_io1, "io2": file_io2},
            default_io_key="io1",
        )
        def multi_io_asset():
            return "value"

        # Convert to spec
        spec = multi_io_asset().to_spec()
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

