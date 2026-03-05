"""Tests for ConfigSpec serialization and reconstruction."""

import pytest

from interloper.serialization import AssetSpec, DAGSpec
from interloper.serialization.backfiller import BackfillerSpec
from interloper.serialization.config import ConfigSpec
from interloper.serialization.io import IOSpec
from interloper.serialization.runner import RunnerSpec


class TestConfigSpec:
    """Test ConfigSpec creation, optional defaults, and serialization."""

    @pytest.fixture()
    def dag_spec(self) -> DAGSpec:
        """A minimal DAGSpec with one asset.

        Returns:
            DAGSpec fixture.
        """
        return DAGSpec(assets=[AssetSpec(path="interloper.assets.decorator.asset")])

    @pytest.fixture()
    def spec(self, dag_spec: DAGSpec) -> ConfigSpec:
        """A ConfigSpec with only the required dag field.

        Returns:
            ConfigSpec fixture.
        """
        return ConfigSpec(dag=dag_spec)

    @pytest.fixture()
    def full_spec(self, dag_spec: DAGSpec) -> ConfigSpec:
        """A ConfigSpec with all optional fields populated.

        Returns:
            ConfigSpec fixture with all fields set.
        """
        return ConfigSpec(
            dag=dag_spec,
            backfiller=BackfillerSpec(path="interloper.backfillers.serial.SerialBackfiller"),
            runner=RunnerSpec(path="interloper.runners.serial.SerialRunner"),
            io={"default": IOSpec(path="interloper.io.file.FileIO", init={"base_path": "/tmp"})},
        )

    def test_creation_with_required_dag(self, spec: ConfigSpec):
        """ConfigSpec can be created with only the required dag field."""
        assert len(spec.dag.assets) == 1
        assert spec.dag.assets[0].path == "interloper.assets.decorator.asset"

    def test_optional_backfiller_defaults_to_none(self, spec: ConfigSpec):
        """Backfiller defaults to None when not provided."""
        assert spec.backfiller is None

    def test_optional_runner_defaults_to_none(self, spec: ConfigSpec):
        """Runner defaults to None when not provided."""
        assert spec.runner is None

    def test_optional_io_defaults_to_empty_dict(self, spec: ConfigSpec):
        """Io defaults to an empty dict when not provided."""
        assert spec.io == {}

    def test_creation_with_all_fields(self, full_spec: ConfigSpec):
        """ConfigSpec stores all optional fields when provided."""
        assert full_spec.backfiller is not None
        assert full_spec.runner is not None
        assert "default" in full_spec.io

    def test_json_roundtrip_minimal(self, spec: ConfigSpec):
        """Minimal ConfigSpec survives JSON roundtrip."""
        json_str = spec.model_dump_json()
        parsed = ConfigSpec.model_validate_json(json_str)
        assert parsed.backfiller is None
        assert parsed.runner is None
        assert parsed.io == {}
        assert len(parsed.dag.assets) == 1

    def test_json_roundtrip_full(self, full_spec: ConfigSpec):
        """Fully populated ConfigSpec survives JSON roundtrip."""
        json_str = full_spec.model_dump_json()
        parsed = ConfigSpec.model_validate_json(json_str)
        assert parsed.backfiller is not None
        assert parsed.backfiller.path == full_spec.backfiller.path
        assert parsed.runner is not None
        assert parsed.runner.path == full_spec.runner.path
        assert "default" in parsed.io
        assert parsed.io["default"].path == full_spec.io["default"].path
        assert len(parsed.dag.assets) == len(full_spec.dag.assets)
