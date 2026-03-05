"""Tests for RunnerSpec serialization and reconstruction."""

import pytest

from interloper.runners.serial import SerialRunner
from interloper.serialization.runner import RunnerSpec


class TestRunnerSpec:
    """Test RunnerSpec creation, serialization, and reconstruction."""

    @pytest.fixture()
    def spec(self) -> RunnerSpec:
        """A RunnerSpec pointing at SerialRunner.

        Returns:
            RunnerSpec fixture.
        """
        return RunnerSpec(path="interloper.runners.serial.SerialRunner")

    @pytest.fixture()
    def spec_with_init(self) -> RunnerSpec:
        """A RunnerSpec with init kwargs.

        Returns:
            RunnerSpec fixture with init kwargs.
        """
        return RunnerSpec(
            path="interloper.runners.serial.SerialRunner",
            init={"key": "value"},
        )

    def test_creation_with_path(self, spec: RunnerSpec):
        """RunnerSpec stores the import path."""
        assert spec.path == "interloper.runners.serial.SerialRunner"

    def test_creation_defaults_init_to_empty_dict(self, spec: RunnerSpec):
        """RunnerSpec defaults init to an empty dict."""
        assert spec.init == {}

    def test_creation_with_init(self, spec_with_init: RunnerSpec):
        """RunnerSpec stores init kwargs."""
        assert spec_with_init.init == {"key": "value"}

    def test_json_roundtrip(self, spec: RunnerSpec):
        """RunnerSpec survives JSON serialization and deserialization."""
        json_str = spec.model_dump_json()
        parsed = RunnerSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.init == spec.init

    def test_json_roundtrip_with_init(self, spec_with_init: RunnerSpec):
        """RunnerSpec with init kwargs survives JSON roundtrip."""
        json_str = spec_with_init.model_dump_json()
        parsed = RunnerSpec.model_validate_json(json_str)
        assert parsed.path == spec_with_init.path
        assert parsed.init == spec_with_init.init

    def test_reconstruct_creates_instance(self, spec: RunnerSpec):
        """reconstruct() creates a SerialRunner instance from the spec."""
        result = spec.reconstruct()
        assert isinstance(result, SerialRunner)
