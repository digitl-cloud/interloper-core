"""Tests for schema inference and validation."""

from typing import Any

import pytest
from pydantic import BaseModel

from interloper.errors import SchemaError
from interloper.schema import infer_schema, validate_schema
from interloper.schema.base import _resolve_field_type

# ---------------------------------------------------------------------------
# infer_schema
# ---------------------------------------------------------------------------


class TestInferSchema:
    """Tests for infer_schema()."""

    def test_simple_rows(self):
        rows = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        schema = infer_schema(rows)
        assert issubclass(schema, BaseModel)
        fields = schema.model_fields
        assert "name" in fields
        assert "age" in fields

    def test_inferred_model_validates_source_data(self):
        rows = [{"name": "alice", "age": 30}]
        schema = infer_schema(rows)
        instance = schema.model_validate(rows[0])
        assert instance.name == "alice"
        assert instance.age == 30

    def test_mixed_int_float_widens_to_float(self):
        rows = [{"value": 1}, {"value": 2.5}]
        schema = infer_schema(rows)
        fields = schema.model_fields
        # Should be float | None
        assert fields["value"].annotation == float | None

    def test_all_none_values_uses_any(self):
        rows = [{"x": None}, {"x": None}]
        schema = infer_schema(rows)
        fields = schema.model_fields
        assert fields["x"].annotation == Any | None

    def test_heterogeneous_uses_any(self):
        rows = [{"x": "hello"}, {"x": 42}, {"x": True}]
        schema = infer_schema(rows)
        fields = schema.model_fields
        assert fields["x"].annotation == Any | None

    def test_empty_raises(self):
        with pytest.raises(SchemaError, match="empty"):
            infer_schema([])

    def test_custom_name(self):
        schema = infer_schema([{"a": 1}], name="MySchema")
        assert schema.__name__ == "MySchema"

    def test_optional_fields_have_none_default(self):
        rows = [{"a": 1}]
        schema = infer_schema(rows)
        instance = schema.model_validate({})
        assert instance.a is None


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------


class TestValidateSchema:
    """Tests for validate_schema()."""

    def test_valid_rows_pass(self):
        class Schema(BaseModel):
            name: str
            age: int | None = None

        rows = [{"name": "alice", "age": 30}, {"name": "bob"}]
        validate_schema(rows, Schema)  # should not raise

    def test_invalid_row_raises(self):
        class Schema(BaseModel):
            name: str

        with pytest.raises(SchemaError, match="row 1"):
            validate_schema([{"name": "alice"}, {"name": 123}], Schema)

    def test_type_coercion(self):
        """Pydantic coerces compatible types by default."""

        class Schema(BaseModel):
            value: float

        rows = [{"value": 1}]
        validate_schema(rows, Schema)  # int → float should work

    def test_empty_rows_pass(self):
        class Schema(BaseModel):
            name: str

        validate_schema([], Schema)  # no rows to validate


# ---------------------------------------------------------------------------
# _resolve_field_type
# ---------------------------------------------------------------------------


class TestResolveFieldType:
    """Tests for _resolve_field_type()."""

    def test_empty_returns_any(self):
        assert _resolve_field_type(set()) is Any

    def test_single_type(self):
        assert _resolve_field_type({str}) is str
        assert _resolve_field_type({int}) is int

    def test_int_float_widens(self):
        assert _resolve_field_type({int, float}) is float

    def test_multiple_incompatible(self):
        assert _resolve_field_type({str, int, list}) is Any
