"""Tests for CLI configuration."""

import interloper as il
from interloper.cli.config import Config
from interloper.serialization.config import ConfigSpec


class TestConfig:
    """Tests for the Config dataclass."""

    def test_creation_with_dag(self):
        """Config requires a DAG."""
        @il.asset
        def a(context: il.ExecutionContext) -> list[dict]:
            return [{"v": 1}]

        dag = il.DAG(a())
        config = Config(dag=dag)
        assert config.dag is dag
        assert config.backfiller is None
        assert config.runner is None
        assert config.io == {}

    def test_creation_with_all_fields(self):
        """Config accepts all optional fields."""
        @il.asset
        def a(context: il.ExecutionContext) -> list[dict]:
            return [{"v": 1}]

        dag = il.DAG(a())
        runner = il.SerialRunner()
        backfiller = il.SerialBackfiller()
        io = {"default": il.MemoryIO()}

        config = Config(dag=dag, runner=runner, backfiller=backfiller, io=io)
        assert config.runner is runner
        assert config.backfiller is backfiller
        assert config.io == io

    def test_to_spec(self):
        """to_spec() returns a ConfigSpec."""
        @il.asset
        def a(context: il.ExecutionContext) -> list[dict]:
            return [{"v": 1}]

        dag = il.DAG(a())
        config = Config(dag=dag)
        spec = config.to_spec()
        assert isinstance(spec, ConfigSpec)
        assert spec.dag is not None
        assert spec.backfiller is None
        assert spec.runner is None

    def test_to_json(self):
        """to_json() returns a valid JSON string."""
        @il.asset
        def a(context: il.ExecutionContext) -> list[dict]:
            return [{"v": 1}]

        dag = il.DAG(a())
        config = Config(dag=dag)
        json_str = config.to_json()
        assert isinstance(json_str, str)
        assert "assets" in json_str

    def test_frozen(self):
        """Config is frozen (immutable)."""
        import pytest

        @il.asset
        def a(context: il.ExecutionContext) -> list[dict]:
            return [{"v": 1}]

        config = Config(dag=il.DAG(a()))
        with pytest.raises(AttributeError):
            config.runner = il.SerialRunner()  # type: ignore[misc]
