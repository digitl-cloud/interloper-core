"""Tests for reconcile_schema() and Normalizer.reconcile()."""

import pytest
from pydantic import BaseModel

from interloper.errors import SchemaError
from interloper.normalizer.base import Normalizer
from interloper.normalizer.schema import reconcile_schema

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


class IntSchema(BaseModel):
    v: int


class FloatSchema(BaseModel):
    v: float


class FullSchema(BaseModel):
    name: str
    age: int | None = None


# ---------------------------------------------------------------------------
# reconcile_schema() unit tests
# ---------------------------------------------------------------------------


class TestReconcileSchema:
    """Unit tests for the reconcile_schema() function."""

    def test_coerces_str_to_int(self):
        rows = [{"v": "123"}]
        result = reconcile_schema(rows, IntSchema)
        assert result == [{"v": 123}]

    def test_coerces_int_to_float(self):
        rows = [{"v": 1}]
        result = reconcile_schema(rows, FloatSchema)
        assert result == [{"v": 1.0}]

    def test_drops_extra_columns(self):
        rows = [{"name": "alice", "age": 30, "extra": "drop_me"}]
        result = reconcile_schema(rows, FullSchema)
        assert result == [{"name": "alice", "age": 30}]
        assert "extra" not in result[0]

    def test_missing_optional_field_uses_default(self):
        rows = [{"name": "alice"}]
        result = reconcile_schema(rows, FullSchema)
        assert result == [{"name": "alice", "age": None}]

    def test_missing_nullable_field_filled_with_none(self):
        """Missing nullable field (no default) is filled with None."""

        class NullableSchema(BaseModel):
            name: str
            tag: str | None  # nullable but no default

        rows = [{"name": "alice"}]
        result = reconcile_schema(rows, NullableSchema)
        assert result == [{"name": "alice", "tag": None}]

    def test_missing_required_non_nullable_field_fails(self):
        """Missing field that is required and non-nullable should fail."""

        class StrictSchema(BaseModel):
            name: str
            age: int  # required, no default, not nullable

        rows = [{"name": "alice"}]
        with pytest.raises(SchemaError, match="Reconciliation failed"):
            reconcile_schema(rows, StrictSchema)

    def test_preserves_matching_types(self):
        rows = [{"name": "alice", "age": 30}]
        result = reconcile_schema(rows, FullSchema)
        assert result == [{"name": "alice", "age": 30}]

    def test_fails_on_incompatible(self):
        rows = [{"v": "abc"}]
        with pytest.raises(SchemaError, match="Reconciliation failed"):
            reconcile_schema(rows, IntSchema)

    def test_error_includes_row_index(self):
        rows = [{"v": "1"}, {"v": "abc"}]
        with pytest.raises(SchemaError, match="row 1"):
            reconcile_schema(rows, IntSchema)

    def test_empty_rows(self):
        result = reconcile_schema([], IntSchema)
        assert result == []


# ---------------------------------------------------------------------------
# Normalizer.reconcile() method tests
# ---------------------------------------------------------------------------


class TestNormalizerReconcile:
    """Tests for Normalizer.reconcile() method delegation."""

    def test_reconcile_delegates(self):
        n = Normalizer()
        rows = [{"v": "42"}]
        result = n.reconcile(rows, IntSchema)
        assert result == [{"v": 42}]

    def test_reconcile_drops_extras(self):
        n = Normalizer()
        rows = [{"name": "alice", "age": 30, "email": "a@b.com"}]
        result = n.reconcile(rows, FullSchema)
        assert result == [{"name": "alice", "age": 30}]
