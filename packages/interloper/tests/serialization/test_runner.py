"""Tests for runner ComponentInstanceSpec serialization and reconstruction."""

import pytest

from interloper.runners.serial import SerialRunner
from interloper.serialization.base import ComponentInstanceSpec


class TestRunnerSpec:
    """Test ComponentInstanceSpec creation, serialization, and reconstruction for runners."""

    @pytest.fixture()
    def spec(self) -> ComponentInstanceSpec:
        """A ComponentInstanceSpec pointing at SerialRunner.

        Returns:
            ComponentInstanceSpec fixture.
        """
        return ComponentInstanceSpec(path="interloper.runners.serial.SerialRunner")

    @pytest.fixture()
    def spec_with_init(self) -> ComponentInstanceSpec:
        """A ComponentInstanceSpec with init kwargs.

        Returns:
            ComponentInstanceSpec fixture with init kwargs.
        """
        return ComponentInstanceSpec(
            path="interloper.runners.serial.SerialRunner",
            init={"key": "value"},
        )

    def test_creation_with_path(self, spec: ComponentInstanceSpec):
        """ComponentInstanceSpec stores the import path."""
        assert spec.path == "interloper.runners.serial.SerialRunner"

    def test_creation_defaults_init_to_empty_dict(self, spec: ComponentInstanceSpec):
        """ComponentInstanceSpec defaults init to an empty dict."""
        assert spec.init == {}

    def test_creation_with_init(self, spec_with_init: ComponentInstanceSpec):
        """ComponentInstanceSpec stores init kwargs."""
        assert spec_with_init.init == {"key": "value"}

    def test_json_roundtrip(self, spec: ComponentInstanceSpec):
        """ComponentInstanceSpec survives JSON serialization and deserialization."""
        json_str = spec.model_dump_json()
        parsed = ComponentInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.init == spec.init

    def test_json_roundtrip_with_init(self, spec_with_init: ComponentInstanceSpec):
        """ComponentInstanceSpec with init kwargs survives JSON roundtrip."""
        json_str = spec_with_init.model_dump_json()
        parsed = ComponentInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec_with_init.path
        assert parsed.init == spec_with_init.init

    def test_reconstruct_creates_instance(self, spec: ComponentInstanceSpec):
        """reconstruct() creates a SerialRunner instance from the spec."""
        result = spec.reconstruct()
        assert isinstance(result, SerialRunner)
