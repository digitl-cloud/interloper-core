"""Tests for RunnerInstanceSpec serialization and reconstruction."""

import pytest

from interloper.runners.serial import SerialRunner
from interloper.serialization.runner import RunnerInstanceSpec


class TestRunnerSpec:
    """Test RunnerInstanceSpec creation, serialization, and reconstruction."""

    @pytest.fixture()
    def spec(self) -> RunnerInstanceSpec:
        """A RunnerInstanceSpec pointing at SerialRunner.

        Returns:
            RunnerInstanceSpec fixture.
        """
        return RunnerInstanceSpec(path="interloper.runners.serial.SerialRunner")

    @pytest.fixture()
    def spec_with_init(self) -> RunnerInstanceSpec:
        """A RunnerInstanceSpec with init kwargs.

        Returns:
            RunnerInstanceSpec fixture with init kwargs.
        """
        return RunnerInstanceSpec(
            path="interloper.runners.serial.SerialRunner",
            init={"key": "value"},
        )

    def test_creation_with_path(self, spec: RunnerInstanceSpec):
        """RunnerInstanceSpec stores the import path."""
        assert spec.path == "interloper.runners.serial.SerialRunner"

    def test_creation_defaults_init_to_empty_dict(self, spec: RunnerInstanceSpec):
        """RunnerInstanceSpec defaults init to an empty dict."""
        assert spec.init == {}

    def test_creation_with_init(self, spec_with_init: RunnerInstanceSpec):
        """RunnerInstanceSpec stores init kwargs."""
        assert spec_with_init.init == {"key": "value"}

    def test_json_roundtrip(self, spec: RunnerInstanceSpec):
        """RunnerInstanceSpec survives JSON serialization and deserialization."""
        json_str = spec.model_dump_json()
        parsed = RunnerInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.init == spec.init

    def test_json_roundtrip_with_init(self, spec_with_init: RunnerInstanceSpec):
        """RunnerInstanceSpec with init kwargs survives JSON roundtrip."""
        json_str = spec_with_init.model_dump_json()
        parsed = RunnerInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec_with_init.path
        assert parsed.init == spec_with_init.init

    def test_reconstruct_creates_instance(self, spec: RunnerInstanceSpec):
        """reconstruct() creates a SerialRunner instance from the spec."""
        result = spec.reconstruct()
        assert isinstance(result, SerialRunner)
