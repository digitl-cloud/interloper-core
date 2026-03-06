"""Tests for BackfillerInstanceSpec serialization and reconstruction."""

import pytest

from interloper.backfillers.serial import SerialBackfiller
from interloper.serialization.backfiller import BackfillerInstanceSpec


class TestBackfillerSpec:
    """Test BackfillerInstanceSpec creation, serialization, and reconstruction."""

    @pytest.fixture()
    def spec(self) -> BackfillerInstanceSpec:
        """A BackfillerInstanceSpec pointing at SerialBackfiller.

        Returns:
            BackfillerInstanceSpec fixture.
        """
        return BackfillerInstanceSpec(path="interloper.backfillers.serial.SerialBackfiller")

    @pytest.fixture()
    def spec_with_init(self) -> BackfillerInstanceSpec:
        """A BackfillerInstanceSpec with init kwargs.

        Returns:
            BackfillerInstanceSpec fixture with init kwargs.
        """
        return BackfillerInstanceSpec(
            path="interloper.backfillers.serial.SerialBackfiller",
            init={"key": "value"},
        )

    def test_creation_with_path(self, spec: BackfillerInstanceSpec):
        """BackfillerInstanceSpec stores the import path."""
        assert spec.path == "interloper.backfillers.serial.SerialBackfiller"

    def test_creation_defaults_init_to_empty_dict(self, spec: BackfillerInstanceSpec):
        """BackfillerInstanceSpec defaults init to an empty dict."""
        assert spec.init == {}

    def test_creation_with_init(self, spec_with_init: BackfillerInstanceSpec):
        """BackfillerInstanceSpec stores init kwargs."""
        assert spec_with_init.init == {"key": "value"}

    def test_json_roundtrip(self, spec: BackfillerInstanceSpec):
        """BackfillerInstanceSpec survives JSON serialization and deserialization."""
        json_str = spec.model_dump_json()
        parsed = BackfillerInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec.path
        assert parsed.init == spec.init

    def test_json_roundtrip_with_init(self, spec_with_init: BackfillerInstanceSpec):
        """BackfillerInstanceSpec with init kwargs survives JSON roundtrip."""
        json_str = spec_with_init.model_dump_json()
        parsed = BackfillerInstanceSpec.model_validate_json(json_str)
        assert parsed.path == spec_with_init.path
        assert parsed.init == spec_with_init.init

    def test_reconstruct_creates_instance(self, spec: BackfillerInstanceSpec):
        """reconstruct() creates a SerialBackfiller instance from the spec."""
        result = spec.reconstruct()
        assert isinstance(result, SerialBackfiller)
