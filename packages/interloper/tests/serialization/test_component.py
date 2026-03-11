"""Tests for ComponentInstanceSpec, ComponentDefinitionSpec, and Component."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from interloper.serialization.base import (
    Component,
    ComponentDefinitionSpec,
    ComponentInstanceSpec,
    HasInstanceSpec,
    reconstruct_components,
    reconstruct_config,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class DummyConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432


class SimpleComponent(Component):
    """A simple test component."""


class BaseModelComponent(Component):
    """A BaseModel test component."""

    host: str = "localhost"
    port: int = 5432


# ---------------------------------------------------------------------------
# ComponentInstanceSpec tests
# ---------------------------------------------------------------------------


class TestComponentInstanceSpec:
    def test_roundtrip_config_only(self) -> None:
        spec = ComponentInstanceSpec(
            path="test.module.MyClass",
            config={"host": "localhost", "port": 5432},
        )
        assert spec.path == "test.module.MyClass"
        assert spec.config == {"host": "localhost", "port": 5432}
        assert spec.init == {}

    def test_roundtrip_init_only(self) -> None:
        spec = ComponentInstanceSpec(
            path="test.module.MyClass",
            init={"max_workers": 4},
        )
        assert spec.path == "test.module.MyClass"
        assert spec.config is None
        assert spec.init == {"max_workers": 4}

    def test_roundtrip_both(self) -> None:
        spec = ComponentInstanceSpec(
            path="test.module.MyClass",
            config={"host": "localhost"},
            init={"key": "pg"},
        )
        assert spec.path == "test.module.MyClass"
        assert spec.config == {"host": "localhost"}
        assert spec.init == {"key": "pg"}

    def test_roundtrip_neither(self) -> None:
        spec = ComponentInstanceSpec(path="test.module.MyClass")
        assert spec.path == "test.module.MyClass"
        assert spec.config is None
        assert spec.init == {}

    def test_json_roundtrip(self) -> None:
        spec = ComponentInstanceSpec(
            path="test.module.MyClass",
            config={"host": "localhost"},
            init={"key": "pg"},
        )
        json_str = spec.model_dump_json()
        restored = ComponentInstanceSpec.model_validate_json(json_str)
        assert restored == spec


# ---------------------------------------------------------------------------
# Component.to_spec() tests
# ---------------------------------------------------------------------------


class TestComponentToSpec:
    def test_basemodel_component(self) -> None:
        comp = BaseModelComponent(host="db.example.com", port=3306, key="mydb")
        spec = comp.to_spec()
        assert spec.config == {"host": "db.example.com", "port": 3306}
        assert spec.init == {"key": "mydb"}

    def test_basemodel_component_no_key(self) -> None:
        comp = BaseModelComponent()
        spec = comp.to_spec()
        assert spec.config == {"host": "localhost", "port": 5432}
        assert spec.init == {}


# ---------------------------------------------------------------------------
# Component.definition_spec() tests
# ---------------------------------------------------------------------------


class TestComponentDefinitionSpec:
    def test_simple_definition_spec(self) -> None:
        """Simple components produce definition specs with JSON schema."""
        spec = SimpleComponent.definition_spec()
        assert isinstance(spec, ComponentDefinitionSpec)
        assert spec.key == "SimpleComponent"
        assert spec.label == "SimpleComponent"
        assert spec.description == "A simple test component."
        assert spec.config_schema is not None
        assert "properties" in spec.config_schema
        assert "key" in spec.config_schema["properties"]

    def test_basemodel_definition_spec(self) -> None:
        """BaseModel components produce definition specs with JSON schema."""
        spec = BaseModelComponent.definition_spec()
        assert isinstance(spec, ComponentDefinitionSpec)
        assert spec.key == "BaseModelComponent"
        assert spec.config_schema is not None
        assert "properties" in spec.config_schema
        assert "host" in spec.config_schema["properties"]
        assert "port" in spec.config_schema["properties"]

    def test_definition_spec_json_roundtrip(self) -> None:
        spec = BaseModelComponent.definition_spec()
        json_str = spec.model_dump_json()
        restored = ComponentDefinitionSpec.model_validate_json(json_str)
        assert restored.key == spec.key
        assert restored.config_schema == spec.config_schema

    def test_label_strips_io_suffix(self) -> None:
        """Label is derived by stripping IO suffix in definition_spec()."""

        class BigQueryIO(Component):
            pass

        spec = BigQueryIO.definition_spec()
        assert spec.label == "BigQuery"

    def test_label_strips_runner_suffix(self) -> None:
        """Label is derived by stripping Runner suffix in definition_spec()."""

        class FastRunner(Component):
            pass

        spec = FastRunner.definition_spec()
        assert spec.label == "Fast"

    def test_label_strips_backfiller_suffix(self) -> None:
        """Label is derived by stripping Backfiller suffix in definition_spec()."""

        class DockerBackfiller(Component):
            pass

        spec = DockerBackfiller.definition_spec()
        assert spec.label == "Docker"

    def test_label_no_suffix_uses_classname(self) -> None:
        """Class without known suffix falls back to class name."""

        class SomeThing(Component):
            pass

        spec = SomeThing.definition_spec()
        assert spec.label == "SomeThing"


# ---------------------------------------------------------------------------
# Shared reconstruction helpers
# ---------------------------------------------------------------------------


class TestReconstructComponents:
    def test_none(self) -> None:
        assert reconstruct_components(None) is None

    def test_single_spec(self) -> None:
        spec = ComponentInstanceSpec(path="interloper.io.memory.MemoryIO", init={"key": "mem"})
        result = reconstruct_components(spec)
        assert result is not None
        assert not isinstance(result, list)
        assert result.key == "mem"

    def test_list_of_specs(self) -> None:
        specs = [
            ComponentInstanceSpec(path="interloper.io.memory.MemoryIO", init={"key": "mem1"}),
            ComponentInstanceSpec(path="interloper.io.memory.MemoryIO", init={"key": "mem2"}),
        ]
        result = reconstruct_components(specs)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].key == "mem1"
        assert result[1].key == "mem2"


class TestReconstructConfig:
    def test_none_definition_config(self) -> None:
        class FakeDefinition:
            config = None

        assert reconstruct_config(FakeDefinition(), {"host": "localhost"}) is None

    def test_none_data(self) -> None:
        class FakeDefinition:
            config = DummyConfig

        assert reconstruct_config(FakeDefinition(), None) is None

    def test_valid(self) -> None:
        class FakeDefinition:
            config = DummyConfig

        result = reconstruct_config(FakeDefinition(), {"host": "db.example.com", "port": 3306})
        assert isinstance(result, DummyConfig)
        assert result.host == "db.example.com"
        assert result.port == 3306


class TestSerializable:
    def test_not_generic(self) -> None:
        """Serializable should not be generic — it was simplified."""

        @dataclass
        class MySerializable(HasInstanceSpec):
            def to_spec(self) -> ComponentInstanceSpec:
                return ComponentInstanceSpec(path=self.path)

        obj = MySerializable()
        spec = obj.to_spec()
        assert spec.path.endswith("MySerializable")
