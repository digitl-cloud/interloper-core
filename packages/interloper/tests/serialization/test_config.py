"""Tests for ConfigInstanceSpec serialization and reconstruction."""

import pytest

from interloper.serialization import AssetInstanceSpec, DAGInstanceSpec
from interloper.serialization.base import ComponentInstanceSpec
from interloper.serialization.config import ConfigInstanceSpec


class TestConfigSpec:
    """Test ConfigInstanceSpec creation, optional defaults, and serialization."""

    @pytest.fixture()
    def dag_spec(self) -> DAGInstanceSpec:
        """A minimal DAGInstanceSpec with one asset.

        Returns:
            DAGInstanceSpec fixture.
        """
        return DAGInstanceSpec(assets=[AssetInstanceSpec(path="interloper.assets.decorator.asset")])

    @pytest.fixture()
    def spec(self, dag_spec: DAGInstanceSpec) -> ConfigInstanceSpec:
        """A ConfigInstanceSpec with only the required dag field.

        Returns:
            ConfigInstanceSpec fixture.
        """
        return ConfigInstanceSpec(dag=dag_spec)

    @pytest.fixture()
    def full_spec(self, dag_spec: DAGInstanceSpec) -> ConfigInstanceSpec:
        """A ConfigInstanceSpec with all optional fields populated.

        Returns:
            ConfigInstanceSpec fixture with all fields set.
        """
        return ConfigInstanceSpec(
            dag=dag_spec,
            backfiller=ComponentInstanceSpec(path="interloper.backfillers.serial.SerialBackfiller"),
            runner=ComponentInstanceSpec(path="interloper.runners.serial.SerialRunner"),
            destinations=[
                ComponentInstanceSpec(
                    path="interloper.destination.file.FileDestination",
                    config={"base_path": "/tmp"},
                    init={"key": "default"},
                ),
            ],
        )

    def test_creation_with_required_dag(self, spec: ConfigInstanceSpec):
        """ConfigInstanceSpec can be created with only the required dag field."""
        assert len(spec.dag.assets) == 1
        assert spec.dag.assets[0].path == "interloper.assets.decorator.asset"

    def test_optional_backfiller_defaults_to_none(self, spec: ConfigInstanceSpec):
        """ComponentInstanceSpec defaults to None when not provided."""
        assert spec.backfiller is None

    def test_optional_runner_defaults_to_none(self, spec: ConfigInstanceSpec):
        """ComponentInstanceSpec defaults to None when not provided."""
        assert spec.runner is None

    def test_optional_destinations_defaults_to_empty_list(self, spec: ConfigInstanceSpec):
        """ComponentInstanceSpec defaults to an empty list when not provided."""
        assert spec.destinations == []

    def test_creation_with_all_fields(self, full_spec: ConfigInstanceSpec):
        """ConfigInstanceSpec stores all optional fields when provided."""
        assert full_spec.backfiller is not None
        assert full_spec.runner is not None
        assert len(full_spec.destinations) == 1
        assert full_spec.destinations[0].init.get("key") == "default"

    def test_json_roundtrip_minimal(self, spec: ConfigInstanceSpec):
        """Minimal ConfigInstanceSpec survives JSON roundtrip."""
        json_str = spec.model_dump_json()
        parsed = ConfigInstanceSpec.model_validate_json(json_str)
        assert parsed.backfiller is None
        assert parsed.runner is None
        assert parsed.destinations == []
        assert len(parsed.dag.assets) == 1

    def test_json_roundtrip_full(self, full_spec: ConfigInstanceSpec):
        """Fully populated ConfigInstanceSpec survives JSON roundtrip."""
        json_str = full_spec.model_dump_json()
        parsed = ConfigInstanceSpec.model_validate_json(json_str)
        assert parsed.backfiller is not None
        assert parsed.backfiller.path == full_spec.backfiller.path
        assert parsed.runner is not None
        assert parsed.runner.path == full_spec.runner.path
        assert len(parsed.destinations) == 1
        assert parsed.destinations[0].path == full_spec.destinations[0].path
        assert parsed.destinations[0].init.get("key") == "default"
        assert len(parsed.dag.assets) == len(full_spec.dag.assets)
