"""Tests for the Normalizer class."""

import pytest
from pydantic import BaseModel

from interloper.errors import NormalizerError, SchemaError
from interloper.normalizer.base import Normalizer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class SampleModel(BaseModel):
    """Sample model."""

    user_name: str
    age: int


# ---------------------------------------------------------------------------
# Type coercion (_coerce)
# ---------------------------------------------------------------------------


class TestCoerce:
    """Tests for Normalizer._coerce() type coercion."""

    def setup_method(self):
        self.n = Normalizer(normalize_columns=False, fill_missing=False, infer=False)

    def test_list_dict_passthrough(self):
        data = [{"a": 1}, {"b": 2}]
        assert self.n._coerce(data) is data

    def test_single_dict_wrapped(self):
        assert self.n._coerce({"a": 1}) == [{"a": 1}]

    def test_pydantic_model_dumped(self):
        m = SampleModel(user_name="alice", age=30)
        assert self.n._coerce(m) == [{"user_name": "alice", "age": 30}]

    def test_list_pydantic_models(self):
        models = [SampleModel(user_name="a", age=1), SampleModel(user_name="b", age=2)]
        assert self.n._coerce(models) == [
            {"user_name": "a", "age": 1},
            {"user_name": "b", "age": 2},
        ]

    def test_generator_consumed(self):
        def gen():
            yield {"x": 1}
            yield {"x": 2}

        assert self.n._coerce(gen()) == [{"x": 1}, {"x": 2}]

    def test_none_returns_empty(self):
        assert self.n._coerce(None) == []

    def test_empty_list_returns_empty(self):
        assert self.n._coerce([]) == []

    def test_unsupported_type_raises(self):
        with pytest.raises(NormalizerError, match="does not support type"):
            self.n._coerce(42)

    def test_list_of_unsupported_raises(self):
        with pytest.raises(NormalizerError, match="list\\[int\\]"):
            self.n._coerce([1, 2, 3])


# ---------------------------------------------------------------------------
# Flatten
# ---------------------------------------------------------------------------


class TestFlatten:
    """Tests for Normalizer._flatten_dict()."""

    def test_flat_dict_noop(self):
        n = Normalizer(flatten_max_level=None)
        assert n._flatten_dict({"a": 1, "b": 2}) == {"a": 1, "b": 2}

    def test_nested_dict(self):
        n = Normalizer(flatten_max_level=None)
        assert n._flatten_dict({"a": {"b": 1, "c": 2}}) == {"a_b": 1, "a_c": 2}

    def test_custom_separator(self):
        n = Normalizer(flatten_max_level=None, flatten_separator=".")
        assert n._flatten_dict({"a": {"b": 1}}) == {"a.b": 1}

    def test_max_level_limits_depth(self):
        n = Normalizer(flatten_max_level=1)
        data = {"a": {"b": {"c": 1}}}
        result = n._flatten_dict(data)
        # Level 0: a -> dict, flattened. Level 1: a_b -> dict, NOT flattened (level >= max_level)
        assert result == {"a_b": {"c": 1}}

    def test_deeply_nested(self):
        n = Normalizer(flatten_max_level=None)
        data = {"a": {"b": {"c": {"d": 1}}}}
        assert n._flatten_dict(data) == {"a_b_c_d": 1}


# ---------------------------------------------------------------------------
# Column normalization
# ---------------------------------------------------------------------------


class TestColumnName:
    """Tests for Normalizer.column_name()."""

    def test_camel_case(self):
        n = Normalizer()
        assert n.column_name("userName") == "user_name"

    def test_pascal_case(self):
        n = Normalizer()
        assert n.column_name("UserName") == "user_name"

    def test_already_snake(self):
        n = Normalizer()
        assert n.column_name("user_name") == "user_name"

    def test_hyphens(self):
        n = Normalizer()
        assert n.column_name("user-name") == "user_name"

    def test_spaces(self):
        n = Normalizer()
        assert n.column_name("user name") == "user_name"

    def test_special_characters(self):
        n = Normalizer()
        assert n.column_name("cost%") == "cost"


# ---------------------------------------------------------------------------
# Fill missing
# ---------------------------------------------------------------------------


class TestFillMissing:
    """Tests for Normalizer._fill_missing_keys()."""

    def test_uniform_noop(self):
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        assert Normalizer._fill_missing_keys(rows) == rows

    def test_missing_keys_filled(self):
        rows = [{"a": 1}, {"b": 2}]
        result = Normalizer._fill_missing_keys(rows)
        assert result == [{"a": 1, "b": None}, {"a": None, "b": 2}]

    def test_empty_list(self):
        assert Normalizer._fill_missing_keys([]) == []

    def test_single_row(self):
        rows = [{"a": 1, "b": 2}]
        assert Normalizer._fill_missing_keys(rows) == rows

    def test_preserves_key_order(self):
        rows = [{"b": 1, "a": 2}, {"c": 3}]
        result = Normalizer._fill_missing_keys(rows)
        # Keys should appear in order of first encounter: b, a, c
        assert list(result[0].keys()) == ["b", "a", "c"]


# ---------------------------------------------------------------------------
# Full pipeline (normalize)
# ---------------------------------------------------------------------------


class TestNormalize:
    """Tests for the full Normalizer.normalize() pipeline."""

    def test_default_options(self):
        data = [{"UserName": "alice"}, {"Age": 30}]
        n = Normalizer()
        result = n.normalize(data)
        assert result == [
            {"user_name": "alice", "age": None},
            {"user_name": None, "age": 30},
        ]

    def test_all_disabled(self):
        n = Normalizer(
            normalize_columns=False,
            fill_missing=False,
            infer=False,
        )
        data = [{"A": 1}]
        result = n.normalize(data)
        assert result == [{"A": 1}]

    def test_flatten_then_normalize_columns(self):
        """Operations should apply in order: flatten -> normalize -> fill."""
        n = Normalizer(flatten_max_level=None, infer=False)
        data = [{"userData": {"firstName": "alice"}}]
        result = n.normalize(data)
        # After flatten: {"userData_firstName": "alice"}
        # After normalize: {"user_data_first_name": "alice"}
        assert result == [{"user_data_first_name": "alice"}]

    def test_empty_data(self):
        n = Normalizer()
        result = n.normalize([])
        assert result == []

    def test_dict_becomes_list_dict(self):
        n = Normalizer(normalize_columns=False, fill_missing=False, infer=False)
        result = n.normalize({"a": 1})
        assert result == [{"a": 1}]

    def test_pydantic_model_becomes_list_dict(self):
        n = Normalizer(normalize_columns=False, fill_missing=False, infer=False)
        result = n.normalize(SampleModel(user_name="alice", age=30))
        assert result == [{"user_name": "alice", "age": 30}]


# ---------------------------------------------------------------------------
# Schema inference and validation
# ---------------------------------------------------------------------------


class TestInferSchema:
    """Tests for Normalizer.infer_schema()."""

    def test_infers_schema(self):
        n = Normalizer()
        data = [{"name": "alice", "age": 30}]
        schema = n.infer_schema(data)
        assert issubclass(schema, BaseModel)
        assert "name" in schema.model_fields
        assert "age" in schema.model_fields

    def test_empty_data_raises(self):
        n = Normalizer()
        with pytest.raises(SchemaError, match="Cannot infer schema from empty data"):
            n.infer_schema([])


class TestValidateSchema:
    """Tests for Normalizer.validate_schema()."""

    def test_valid_data_passes(self):
        class Schema(BaseModel):
            name: str
            age: int | None = None

        n = Normalizer()
        n.validate_schema([{"name": "alice", "age": 30}], Schema)  # should not raise

    def test_invalid_data_raises(self):
        class StrictSchema(BaseModel):
            name: str

        n = Normalizer()
        with pytest.raises(SchemaError, match="Schema validation failed"):
            n.validate_schema([{"name": 123}], StrictSchema)
