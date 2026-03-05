"""Tests for schema inference, validation, and reconciliation."""

from typing import Any

import pytest
from pydantic import BaseModel

from interloper.errors import SchemaError
from interloper.schema import infer_schema, reconcile_schema, validate_schema
from interloper.schema.base import _resolve_field_type

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class IntSchema(BaseModel):
    v: int


class FloatSchema(BaseModel):
    v: float


class FullSchema(BaseModel):
    name: str
    age: int | None = None


# ---------------------------------------------------------------------------
# infer_schema
# ---------------------------------------------------------------------------


class TestInferSchema:
    """Tests for infer_schema()."""

    def test_simple_rows(self):
        """Infers fields from uniform rows."""
        rows = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        schema = infer_schema(rows)
        assert issubclass(schema, BaseModel)
        fields = schema.model_fields
        assert "name" in fields
        assert "age" in fields

    def test_inferred_model_validates_source_data(self):
        """Inferred model validates its source data."""
        rows = [{"name": "alice", "age": 30}]
        schema = infer_schema(rows)
        instance = schema.model_validate(rows[0])
        assert instance.name == "alice"
        assert instance.age == 30

    def test_mixed_int_float_widens_to_float(self):
        """Mixed int/float column widens to float."""
        rows = [{"value": 1}, {"value": 2.5}]
        schema = infer_schema(rows)
        assert schema.model_fields["value"].annotation == float | None

    def test_all_none_values_uses_any(self):
        """All-None column resolves to Any."""
        rows = [{"x": None}, {"x": None}]
        schema = infer_schema(rows)
        assert schema.model_fields["x"].annotation == Any | None

    def test_heterogeneous_uses_any(self):
        """Incompatible types resolve to Any."""
        rows = [{"x": "hello"}, {"x": 42}, {"x": True}]
        schema = infer_schema(rows)
        assert schema.model_fields["x"].annotation == Any | None

    def test_empty_raises(self):
        """Empty rows raise SchemaError."""
        with pytest.raises(SchemaError, match="empty"):
            infer_schema([])

    def test_custom_name(self):
        """Custom schema name is applied."""
        schema = infer_schema([{"a": 1}], name="MySchema")
        assert schema.__name__ == "MySchema"

    def test_optional_fields_have_none_default(self):
        """Inferred fields default to None."""
        schema = infer_schema([{"a": 1}])
        instance = schema.model_validate({})
        assert instance.a is None


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------


class TestValidateSchema:
    """Tests for validate_schema()."""

    def test_valid_rows_pass(self):
        """Valid rows pass without error."""
        class Schema(BaseModel):
            name: str
            age: int | None = None

        validate_schema([{"name": "alice", "age": 30}, {"name": "bob"}], Schema)

    def test_invalid_row_raises(self):
        """Invalid row raises SchemaError with row index."""
        class Schema(BaseModel):
            name: str

        with pytest.raises(SchemaError, match="row 1"):
            validate_schema([{"name": "alice"}, {"name": 123}], Schema)

    def test_type_coercion(self):
        """Pydantic coerces compatible types."""
        class Schema(BaseModel):
            value: float

        validate_schema([{"value": 1}], Schema)

    def test_empty_rows_pass(self):
        """Empty rows pass validation."""
        class Schema(BaseModel):
            name: str

        validate_schema([], Schema)


# ---------------------------------------------------------------------------
# reconcile_schema
# ---------------------------------------------------------------------------


class TestReconcileSchema:
    """Tests for the reconcile_schema() function."""

    def test_coerces_str_to_int(self):
        """String values coerced to int."""
        assert reconcile_schema([{"v": "123"}], IntSchema) == [{"v": 123}]

    def test_coerces_int_to_float(self):
        """Int values coerced to float."""
        assert reconcile_schema([{"v": 1}], FloatSchema) == [{"v": 1.0}]

    def test_drops_extra_columns(self):
        """Extra columns dropped."""
        result = reconcile_schema([{"name": "alice", "age": 30, "extra": "drop_me"}], FullSchema)
        assert result == [{"name": "alice", "age": 30}]
        assert "extra" not in result[0]

    def test_missing_optional_field_uses_default(self):
        """Missing optional field filled with default."""
        result = reconcile_schema([{"name": "alice"}], FullSchema)
        assert result == [{"name": "alice", "age": None}]

    def test_missing_nullable_field_filled_with_none(self):
        """Missing nullable field (no default) filled with None."""
        class NullableSchema(BaseModel):
            name: str
            tag: str | None

        result = reconcile_schema([{"name": "alice"}], NullableSchema)
        assert result == [{"name": "alice", "tag": None}]

    def test_missing_required_non_nullable_field_fails(self):
        """Missing required non-nullable field raises SchemaError."""
        class StrictSchema(BaseModel):
            name: str
            age: int

        with pytest.raises(SchemaError, match="Reconciliation failed"):
            reconcile_schema([{"name": "alice"}], StrictSchema)

    def test_preserves_matching_types(self):
        """Matching types pass through unchanged."""
        result = reconcile_schema([{"name": "alice", "age": 30}], FullSchema)
        assert result == [{"name": "alice", "age": 30}]

    def test_fails_on_incompatible(self):
        """Incompatible type raises SchemaError."""
        with pytest.raises(SchemaError, match="Reconciliation failed"):
            reconcile_schema([{"v": "abc"}], IntSchema)

    def test_error_includes_row_index(self):
        """Error message includes the failing row index."""
        with pytest.raises(SchemaError, match="row 1"):
            reconcile_schema([{"v": "1"}, {"v": "abc"}], IntSchema)

    def test_empty_rows(self):
        """Empty rows return empty list."""
        assert reconcile_schema([], IntSchema) == []


# ---------------------------------------------------------------------------
# _resolve_field_type
# ---------------------------------------------------------------------------


class TestResolveFieldType:
    """Tests for _resolve_field_type()."""

    def test_empty_returns_any(self):
        """Empty type set returns Any."""
        assert _resolve_field_type(set()) is Any

    def test_single_type(self):
        """Single type returned directly."""
        assert _resolve_field_type({str}) is str
        assert _resolve_field_type({int}) is int

    def test_int_float_widens(self):
        """Int+float widens to float."""
        assert _resolve_field_type({int, float}) is float

    def test_multiple_incompatible(self):
        """Multiple incompatible types resolve to Any."""
        assert _resolve_field_type({str, int, list}) is Any
