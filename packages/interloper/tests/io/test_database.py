"""Tests for DatabaseIO and WriteDisposition."""

import datetime as dt
from unittest.mock import MagicMock

import pytest

from interloper.errors import AdapterError
from interloper.io.adapter import RowAdapter
from interloper.io.context import IOContext
from interloper.io.database import DatabaseIO, WriteDisposition
from interloper.partitioning.time import TimePartition, TimePartitionConfig, TimePartitionWindow


class StubDatabaseIO(DatabaseIO):
    """Concrete DatabaseIO that records calls to abstract methods."""

    def __init__(self, **kwargs):
        """Initialize with call-tracking state."""
        super().__init__(**kwargs)
        self.calls: list[tuple] = []

    def _insert(self, table, schema, rows):
        self.calls.append(("insert", table, schema, rows))

    def _delete_all(self, table, schema):
        self.calls.append(("delete_all", table, schema))

    def _delete_partition(self, table, schema, column, value):
        self.calls.append(("delete_partition", table, schema, column, value))

    def _select_all(self, table, schema):
        self.calls.append(("select_all", table, schema))
        return [{"v": 1}]

    def _select_partition(self, table, schema, column, value):
        self.calls.append(("select_partition", table, schema, column, value))
        return [{"v": 1}]

    def to_spec(self):
        pass  # not needed for these tests


def _make_asset(*, name="my_table", dataset="my_schema", partitioning=None):
    """Create a mock asset with the given attributes.

    Returns:
        Mock asset with name, dataset, and partitioning.
    """
    asset = MagicMock()
    asset.name = name
    asset.dataset = dataset
    asset.partitioning = partitioning
    return asset


class TestWriteDisposition:
    """Tests for WriteDisposition enum."""

    def test_replace_value(self):
        """REPLACE has the string value 'replace'."""
        assert WriteDisposition.REPLACE.value == "replace"

    def test_append_value(self):
        """APPEND has the string value 'append'."""
        assert WriteDisposition.APPEND.value == "append"


class TestDatabaseIO:
    """Tests for DatabaseIO dispatch logic."""

    @pytest.fixture()
    def stub(self):
        """Return a StubDatabaseIO with default settings."""
        return StubDatabaseIO()

    @pytest.fixture()
    def unpartitioned_ctx(self):
        """Return an IOContext for an unpartitioned asset."""
        asset = _make_asset()
        return IOContext(asset=asset, partition_or_window=None)

    @pytest.fixture()
    def partitioned_ctx(self):
        """Return an IOContext for a single-partition asset."""
        asset = _make_asset(partitioning=TimePartitionConfig(column="date"))
        partition = TimePartition(dt.date(2025, 1, 1))
        return IOContext(asset=asset, partition_or_window=partition)

    @pytest.fixture()
    def window_ctx(self):
        """Return an IOContext with a partition window."""
        asset = _make_asset(partitioning=TimePartitionConfig(column="date"))
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))
        return IOContext(asset=asset, partition_or_window=window)

    # ---- init defaults ----

    def test_init_defaults(self):
        """Default init uses APPEND, chunk_size=1000, no adapter."""
        db = StubDatabaseIO()
        assert db.write_disposition is WriteDisposition.APPEND
        assert db.chunk_size == 1000
        assert db.adapter is None

    # ---- write: append / no partition ----

    def test_write_append_no_partition(self, stub, unpartitioned_ctx):
        """Append write without partitioning calls _insert only."""
        stub.write(unpartitioned_ctx, [{"a": 1}])
        assert len(stub.calls) == 1
        assert stub.calls[0][0] == "insert"

    # ---- write: replace / no partition ----

    def test_write_replace_no_partition(self, unpartitioned_ctx):
        """Replace write without partitioning calls _delete_all then _insert."""
        db = StubDatabaseIO(write_disposition=WriteDisposition.REPLACE)
        db.write(unpartitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 2
        assert db.calls[0][0] == "delete_all"
        assert db.calls[1][0] == "insert"

    # ---- write: append / single partition ----

    def test_write_append_single_partition(self, stub, partitioned_ctx):
        """Append write with a single partition calls _insert only."""
        stub.write(partitioned_ctx, [{"a": 1}])
        assert len(stub.calls) == 1
        assert stub.calls[0][0] == "insert"

    # ---- write: replace / single partition ----

    def test_write_replace_single_partition(self, partitioned_ctx):
        """Replace write with a single partition calls _delete_partition then _insert."""
        db = StubDatabaseIO(write_disposition=WriteDisposition.REPLACE)
        db.write(partitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 2
        assert db.calls[0] == (
            "delete_partition", "my_table", "my_schema", "date", "2025-01-01",
        )
        assert db.calls[1][0] == "insert"

    # ---- write: replace / partition window ----

    def test_write_replace_partition_window(self, window_ctx):
        """Replace write with a window calls _delete_partition per partition, then _insert once."""
        db = StubDatabaseIO(write_disposition=WriteDisposition.REPLACE)
        db.write(window_ctx, [{"a": 1}])
        # Two delete_partition calls + one insert
        assert len(db.calls) == 3
        assert db.calls[0][0] == "delete_partition"
        assert db.calls[1][0] == "delete_partition"
        assert db.calls[2][0] == "insert"

    # ---- write: empty data ----

    def test_write_empty_data_noop(self, stub, unpartitioned_ctx):
        """Writing an empty list produces no database calls."""
        stub.write(unpartitioned_ctx, [])
        assert stub.calls == []

    # ---- read: no partition ----

    def test_read_no_partition(self, stub, unpartitioned_ctx):
        """Read without partitioning calls _select_all."""
        result = stub.read(unpartitioned_ctx)
        assert len(stub.calls) == 1
        assert stub.calls[0] == ("select_all", "my_table", "my_schema")
        assert result == [{"v": 1}]

    # ---- read: single partition ----

    def test_read_single_partition(self, stub, partitioned_ctx):
        """Read with a single partition calls _select_partition."""
        result = stub.read(partitioned_ctx)
        assert len(stub.calls) == 1
        assert stub.calls[0] == (
            "select_partition", "my_table", "my_schema", "date", "2025-01-01",
        )
        assert result == [{"v": 1}]

    # ---- read: partition window ----

    def test_read_partition_window(self, stub, window_ctx):
        """Read with a window calls _select_partition for each and returns a list."""
        result = stub.read(window_ctx)
        assert len(stub.calls) == 2
        assert stub.calls[0][0] == "select_partition"
        assert stub.calls[1][0] == "select_partition"
        # One result per partition
        assert result == [[{"v": 1}], [{"v": 1}]]

    # ---- _to_rows without adapter ----

    def test_to_rows_without_adapter_accepts_list(self, stub):
        """_to_rows without an adapter passes through list data."""
        data = [{"a": 1}]
        assert stub._to_rows(data) is data

    def test_to_rows_without_adapter_rejects_non_list(self, stub):
        """_to_rows without an adapter raises AdapterError for non-list data."""
        with pytest.raises(AdapterError, match="No adapter configured"):
            stub._to_rows({"a": 1})

    # ---- _base_init_kwargs ----

    def test_base_init_kwargs_defaults(self, stub):
        """_base_init_kwargs returns correct dict for default settings."""
        kwargs = stub._base_init_kwargs()
        assert kwargs == {
            "write_disposition": "append",
            "chunk_size": 1000,
        }

    def test_base_init_kwargs_with_adapter(self):
        """_base_init_kwargs includes adapter path when an adapter is set."""
        db = StubDatabaseIO(adapter=RowAdapter())
        kwargs = db._base_init_kwargs()
        assert kwargs["adapter"] == "interloper.io.adapter.RowAdapter"
        assert kwargs["write_disposition"] == "append"
        assert kwargs["chunk_size"] == 1000

    def test_base_init_kwargs_custom_settings(self):
        """_base_init_kwargs reflects non-default write_disposition and chunk_size."""
        db = StubDatabaseIO(write_disposition=WriteDisposition.REPLACE, chunk_size=500)
        kwargs = db._base_init_kwargs()
        assert kwargs == {
            "write_disposition": "replace",
            "chunk_size": 500,
        }
