"""Tests for DataAdapter and RowAdapter."""

import pytest

from interloper.errors import AdapterError
from interloper.io.adapter import DataAdapter, RowAdapter


class TestRowAdapter:
    """Tests for RowAdapter identity adapter."""

    @pytest.fixture()
    def adapter(self):
        """Return a RowAdapter instance."""
        return RowAdapter()

    def test_to_rows_passes_through_list(self, adapter):
        """to_rows returns list data unchanged."""
        data = [{"a": 1}, {"a": 2}]
        assert adapter.to_rows(data) is data

    def test_to_rows_raises_for_non_list(self, adapter):
        """to_rows raises AdapterError when data is not a list."""
        with pytest.raises(AdapterError, match="RowAdapter expects list"):
            adapter.to_rows({"a": 1})

    def test_to_rows_raises_for_string(self, adapter):
        """to_rows raises AdapterError for string input."""
        with pytest.raises(AdapterError):
            adapter.to_rows("not a list")

    def test_from_rows_passes_through(self, adapter):
        """from_rows returns rows unchanged."""
        rows = [{"x": 10}]
        assert adapter.from_rows(rows) is rows

    def test_path_returns_import_path(self, adapter):
        """Path property returns the fully-qualified import path."""
        assert adapter.path == "interloper.io.adapter.RowAdapter"

    def test_to_rows_empty_list(self, adapter):
        """to_rows accepts an empty list."""
        data = []
        assert adapter.to_rows(data) == []


class TestDataAdapter:
    """Tests for custom DataAdapter subclasses."""

    def test_custom_adapter_to_rows(self):
        """A custom subclass can convert typed data to rows."""

        class DictAdapter(DataAdapter):
            def to_rows(self, data):
                return [data]

            def from_rows(self, rows):
                return rows[0]

        adapter = DictAdapter()
        result = adapter.to_rows({"key": "value"})
        assert result == [{"key": "value"}]

    def test_custom_adapter_from_rows(self):
        """A custom subclass can convert rows back to typed data."""

        class DictAdapter(DataAdapter):
            def to_rows(self, data):
                return [data]

            def from_rows(self, rows):
                return rows[0]

        adapter = DictAdapter()
        result = adapter.from_rows([{"key": "value"}])
        assert result == {"key": "value"}

    def test_custom_adapter_path(self):
        """Path returns the fully-qualified import path of the subclass."""

        class DictAdapter(DataAdapter):
            def to_rows(self, data):
                return [data]

            def from_rows(self, rows):
                return rows[0]

        adapter = DictAdapter()
        # Locally-defined classes include the test module path
        assert "DictAdapter" in adapter.path

    def test_abstract_methods_required(self):
        """DataAdapter cannot be instantiated without implementing abstract methods."""
        with pytest.raises(TypeError):
            DataAdapter()
