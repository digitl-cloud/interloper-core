"""Test ComponentInstanceSpec for IO serialization."""

from pathlib import Path

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization.base import ComponentDefinitionSpec, ComponentInstanceSpec


class TestIOSpec:
    """Test ComponentInstanceSpec serialization and reconstruction for IO."""

    def test_iospec_roundtrip(self, tmp_path: Path):
        """Test ComponentInstanceSpec roundtrip (to_spec + reconstruct)."""
        # Create a FileIO instance
        file_io = il.FileIO(base_path=str(tmp_path))

        # Convert to spec
        spec = file_io.to_spec()
        assert isinstance(spec, ComponentInstanceSpec)
        assert spec.path == "interloper.io.file.FileIO"
        assert spec.config == {"base_path": str(tmp_path)}
        assert spec.init == {"key": "file"}

        # Reconstruct
        reconstructed = spec.reconstruct()
        assert isinstance(reconstructed, il.FileIO)
        assert reconstructed.base_path == str(tmp_path)

    def test_iospec_validation(self):
        """Test ComponentInstanceSpec Pydantic validation."""
        # Valid spec with config as first-class field
        spec = ComponentInstanceSpec(path="interloper.io.file.FileIO", config={"base_path": "/tmp"})
        assert spec.path == "interloper.io.file.FileIO"
        assert spec.config == {"base_path": "/tmp"}

        # Invalid path (None should be invalid)
        with pytest.raises(ValidationError):
            ComponentInstanceSpec(path=None, init={})  # type: ignore[arg-type]

        # Missing required field
        with pytest.raises(ValidationError):
            ComponentInstanceSpec(init={})  # type: ignore[call-arg]

    def test_iospec_json_serialization(self):
        """Test ComponentInstanceSpec JSON serialization."""
        spec = ComponentInstanceSpec(path="interloper.io.file.FileIO", config={"base_path": "/tmp"})

        # Convert to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Parse from JSON
        parsed = ComponentInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.config == spec.config


class TestIODefinitionSpec:
    """Test ComponentDefinitionSpec creation and serialization for IO."""

    def test_creation(self):
        """ComponentDefinitionSpec can be created with all fields."""
        spec = ComponentDefinitionSpec(
            key="PostgreSQL",
            label="PostgreSQL",
            description="PostgreSQL database IO",
            tags=["Database"],
            config_schema={"type": "object", "properties": {"host": {"type": "string"}}},
        )
        assert spec.key == "PostgreSQL"
        assert spec.label == "PostgreSQL"
        assert spec.description == "PostgreSQL database IO"
        assert spec.tags == ["Database"]
        assert spec.config_schema is not None

    def test_defaults(self):
        """Optional fields default correctly."""
        spec = ComponentDefinitionSpec(key="MemoryIO", label="Memory")
        assert spec.description == ""
        assert spec.tags == []
        assert spec.config_schema is None

    def test_json_roundtrip(self):
        """ComponentDefinitionSpec survives JSON serialization."""
        spec = ComponentDefinitionSpec(
            key="BigQuery",
            label="BigQuery",
            tags=["Cloud", "Database"],
        )
        json_str = spec.model_dump_json()
        parsed = ComponentDefinitionSpec.model_validate_json(json_str)
        assert parsed.key == spec.key
        assert parsed.label == spec.label
        assert parsed.tags == spec.tags

    def test_with_config_schema(self):
        """config_schema stores a JSON Schema dict."""
        schema = {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer", "default": 5432},
            },
            "required": ["host"],
        }
        spec = ComponentDefinitionSpec(key="PostgreSQL", label="PostgreSQL", config_schema=schema)
        assert spec.config_schema == schema
        assert spec.config_schema["properties"]["port"]["default"] == 5432
