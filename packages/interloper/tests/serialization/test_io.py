"""Test IOInstanceSpec serialization and reconstruction."""

from pathlib import Path

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.serialization import IOInstanceSpec


class TestIOSpec:
    """Test IOInstanceSpec serialization and reconstruction."""

    def test_iospec_roundtrip(self, tmp_path: Path):
        """Test IOInstanceSpec roundtrip (to_spec + reconstruct)."""
        # Create a FileIO instance
        file_io = il.FileIO(base_path=str(tmp_path))

        # Convert to spec
        spec = file_io.to_spec()
        assert isinstance(spec, IOInstanceSpec)
        assert spec.path == "interloper.io.file.FileIO"
        assert spec.init == {"base_path": str(tmp_path)}

        # Reconstruct
        reconstructed = spec.reconstruct()
        assert isinstance(reconstructed, il.FileIO)
        assert reconstructed.base_path == str(tmp_path)

    def test_iospec_validation(self):
        """Test IOInstanceSpec Pydantic validation."""
        # Valid spec
        spec = IOInstanceSpec(path="interloper.io.file.FileIO", init={"base_path": "/tmp"})
        assert spec.path == "interloper.io.file.FileIO"
        assert spec.init == {"base_path": "/tmp"}

        # Invalid path (None should be invalid)
        with pytest.raises(ValidationError):
            IOInstanceSpec(path=None, init={})  # type: ignore[arg-type]

        # Missing required field
        with pytest.raises(ValidationError):
            IOInstanceSpec(init={})  # type: ignore[call-arg]

    def test_iospec_json_serialization(self):
        """Test IOInstanceSpec JSON serialization."""
        spec = IOInstanceSpec(path="interloper.io.file.FileIO", init={"base_path": "/tmp"})

        # Convert to JSON
        json_str = spec.model_dump_json()
        assert isinstance(json_str, str)

        # Parse from JSON
        parsed = IOInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.init == spec.init

