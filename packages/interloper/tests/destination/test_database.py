"""Tests for DatabaseDestination and WriteDisposition."""

import datetime as dt
import warnings
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from interloper.destination.adapter import DataAdapter, RowAdapter
from interloper.destination.context import DestinationContext
from interloper.destination.database import DatabaseDestination, WriteDisposition
from interloper.errors import AdapterError
from interloper.partitioning.time import TimePartition, TimePartitionConfig, TimePartitionWindow


class StubDatabaseDestination(DatabaseDestination):
    """Concrete DatabaseDestination that records calls to abstract methods."""

    calls: list[tuple] = Field(default_factory=list, exclude=True)

    def _insert(self, context, table, schema, rows):
        self.calls.append(("insert", table, schema, rows))

    def _delete_all(self, context, table, schema):
        self.calls.append(("delete_all", table, schema))

    def _delete_partition(self, context, table, schema, column, value):
        self.calls.append(("delete_partition", table, schema, column, value))

    def _select_all(self, context, table, schema):
        self.calls.append(("select_all", table, schema))
        return [{"v": 1}]

    def _select_partition(self, context, table, schema, column, value):
        self.calls.append(("select_partition", table, schema, column, value))
        return [{"v": 1}]

    def _count_by_partition(self, context, table, schema, column):
        self.calls.append(("count_by_partition", table, schema, column))
        return {}


def _make_asset(*, name="my_table", dataset="my_schema", partitioning=None):
    """Create a mock asset with the given attributes.

    Returns:
        Mock asset with key, dataset, and partitioning.
    """
    asset = MagicMock()
    asset.key = name
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


class TestDatabaseDestination:
    """Tests for DatabaseDestination dispatch logic."""

    @pytest.fixture()
    def stub(self):
        """Return a StubDatabaseDestination with default settings."""
        return StubDatabaseDestination()

    @pytest.fixture()
    def unpartitioned_ctx(self):
        """Return a DestinationContext for an unpartitioned asset."""
        asset = _make_asset()
        return DestinationContext(asset=asset, partition_or_window=None)

    @pytest.fixture()
    def partitioned_ctx(self):
        """Return a DestinationContext for a single-partition asset."""
        asset = _make_asset(partitioning=TimePartitionConfig(column="date"))
        partition = TimePartition(dt.date(2025, 1, 1))
        return DestinationContext(asset=asset, partition_or_window=partition)

    @pytest.fixture()
    def window_ctx(self):
        """Return a DestinationContext with a partition window."""
        asset = _make_asset(partitioning=TimePartitionConfig(column="date"))
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))
        return DestinationContext(asset=asset, partition_or_window=window)

    # ---- init defaults ----

    def test_init_defaults(self):
        """Default init uses REPLACE, chunk_size=1000, no adapter."""
        db = StubDatabaseDestination()
        assert db.write_disposition is WriteDisposition.REPLACE
        assert db.chunk_size == 1000
        assert db.adapter is None

    # ---- write: append / no partition ----

    def test_write_append_no_partition(self, unpartitioned_ctx):
        """Append write without partitioning calls _insert only."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.APPEND)
        db.write(unpartitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 1
        assert db.calls[0][0] == "insert"

    # ---- write: replace / no partition ----

    def test_write_replace_no_partition(self, unpartitioned_ctx):
        """Replace write without partitioning calls _delete_all then _insert."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.REPLACE)
        db.write(unpartitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 2
        assert db.calls[0][0] == "delete_all"
        assert db.calls[1][0] == "insert"

    # ---- write: append / single partition ----

    def test_write_append_single_partition(self, partitioned_ctx):
        """Append write with a single partition calls _insert only."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.APPEND)
        db.write(partitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 1
        assert db.calls[0][0] == "insert"

    # ---- write: replace / single partition ----

    def test_write_replace_single_partition(self, partitioned_ctx):
        """Replace write with a single partition calls _delete_partition then _insert."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.REPLACE)
        db.write(partitioned_ctx, [{"a": 1}])
        assert len(db.calls) == 2
        assert db.calls[0] == (
            "delete_partition", "my_table", "my_schema", "date", "2025-01-01",
        )
        assert db.calls[1][0] == "insert"

    # ---- write: replace / partition window ----

    def test_write_replace_partition_window(self, window_ctx):
        """Replace write with a window calls _delete_partition per partition, then _insert once."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.REPLACE)
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
        with pytest.raises(AdapterError, match="No adapter on"):
            stub._to_rows({"a": 1})

    # ---- to_spec / model_dump serialization ----

    def test_to_spec_defaults(self, stub):
        """to_spec returns correct spec for default settings."""
        spec = stub.to_spec()
        assert spec.config == {
            "write_disposition": "replace",
            "chunk_size": 1000,
        }

    def test_to_spec_with_adapter(self):
        """to_spec includes adapter specs when an adapter is set."""
        db = StubDatabaseDestination(adapter=RowAdapter(), write_disposition=WriteDisposition.REPLACE)
        spec = db.to_spec()
        assert spec.config["adapter"] == [{"path": "interloper.destination.adapter.RowAdapter"}]
        assert spec.config["write_disposition"] == "replace"
        assert spec.config["chunk_size"] == 1000

    def test_to_spec_with_adapter_list(self):
        """to_spec includes all adapter specs when a list is set."""
        db = StubDatabaseDestination(adapter=[RowAdapter()])
        spec = db.to_spec()
        assert spec.config["adapter"] == [{"path": "interloper.destination.adapter.RowAdapter"}]

    def test_to_spec_custom_settings(self):
        """to_spec reflects non-default write_disposition and chunk_size."""
        db = StubDatabaseDestination(write_disposition=WriteDisposition.REPLACE, chunk_size=500)
        spec = db.to_spec()
        assert spec.config == {
            "write_disposition": "replace",
            "chunk_size": 500,
        }


class TestAdapterChain:
    """Tests for adapter list (chain) support on DatabaseDestination."""

    def test_single_adapter_normalized_to_list(self):
        """A single adapter is normalized to a list in model_post_init."""
        db = StubDatabaseDestination(adapter=RowAdapter())
        assert isinstance(db.adapter, list)
        assert len(db.adapter) == 1
        assert isinstance(db.adapter[0], RowAdapter)

    def test_string_adapter_resolved_to_list(self):
        """A string adapter is imported and normalized to a list."""
        db = StubDatabaseDestination(adapter="interloper.destination.adapter.RowAdapter")
        assert isinstance(db.adapter, list)
        assert len(db.adapter) == 1
        assert isinstance(db.adapter[0], RowAdapter)

    def test_list_of_strings_resolved(self):
        """A list of strings is resolved to a list of adapter instances."""
        db = StubDatabaseDestination(adapter=["interloper.destination.adapter.RowAdapter"])
        assert isinstance(db.adapter, list)
        assert len(db.adapter) == 1
        assert isinstance(db.adapter[0], RowAdapter)

    def test_to_rows_tries_adapters_in_order(self):
        """_to_rows tries each adapter until one succeeds."""

        class DictAdapter(DataAdapter):
            def to_rows(self, data):
                if not isinstance(data, dict):
                    raise AdapterError("expected dict")
                return [data]

            def from_rows(self, rows):
                return rows[0]

        db = StubDatabaseDestination(adapter=[RowAdapter(), DictAdapter()])

        # list[dict] -> handled by RowAdapter (first)
        rows = db._to_rows([{"a": 1}])
        assert rows == [{"a": 1}]

        # dict -> RowAdapter raises, DictAdapter handles it
        rows = db._to_rows({"b": 2})
        assert rows == [{"b": 2}]

    def test_to_rows_falls_back_to_list_passthrough(self):
        """_to_rows falls back to list[dict] passthrough when no adapter handles it."""

        class StrictAdapter(DataAdapter):
            def to_rows(self, data):
                raise AdapterError("nope")

            def from_rows(self, rows):
                return rows

        db = StubDatabaseDestination(adapter=[StrictAdapter()])
        # Falls back to list[dict] passthrough
        data = [{"a": 1}]
        assert db._to_rows(data) is data

    def test_to_rows_raises_when_nothing_handles(self):
        """_to_rows raises AdapterError when no adapter handles and data is not list."""

        class StrictAdapter(DataAdapter):
            def to_rows(self, data):
                raise AdapterError("nope")

            def from_rows(self, rows):
                return rows

        db = StubDatabaseDestination(adapter=[StrictAdapter()])
        with pytest.raises(AdapterError, match="StrictAdapter"):
            db._to_rows({"a": 1})

    def test_from_rows_uses_first_adapter(self):
        """_from_rows uses the first adapter in the list."""

        class WrappingAdapter(DataAdapter):
            def to_rows(self, data):
                return data

            def from_rows(self, rows):
                return {"wrapped": rows}

        db = StubDatabaseDestination(adapter=[WrappingAdapter(), RowAdapter()])
        result = db._from_rows([{"a": 1}])
        assert result == {"wrapped": [{"a": 1}]}

    def test_key_derived_with_adapter(self):
        """DatabaseDestination key derivation works even with adapter set (super() called)."""
        db = StubDatabaseDestination(adapter=RowAdapter())
        assert db.key == "stubdatabase"


class TestPartitionColumnWriteWarning:
    """Warn at write-time when partition column is missing from row data."""

    def test_warns_when_partition_column_missing(self):
        """A warning is emitted when writing partitioned data without the partition column."""
        db = StubDatabaseDestination()
        partitioning = TimePartitionConfig(column="date")
        asset = _make_asset(partitioning=partitioning)
        partition = TimePartition(dt.date(2025, 1, 1))
        ctx = DestinationContext(asset=asset, partition_or_window=partition)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            db.write(ctx, [{"value": 1}])

        assert len(w) == 1
        assert "Partition column 'date'" in str(w[0].message)
        assert "['value']" in str(w[0].message)

    def test_no_warning_when_partition_column_present(self):
        """No warning when the partition column is included in the row data."""
        db = StubDatabaseDestination()
        partitioning = TimePartitionConfig(column="date")
        asset = _make_asset(partitioning=partitioning)
        partition = TimePartition(dt.date(2025, 1, 1))
        ctx = DestinationContext(asset=asset, partition_or_window=partition)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            db.write(ctx, [{"date": "2025-01-01", "value": 1}])

        assert len(w) == 0

    def test_no_warning_for_unpartitioned_write(self):
        """No warning when writing without partitioning."""
        db = StubDatabaseDestination()
        asset = _make_asset()
        ctx = DestinationContext(asset=asset)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            db.write(ctx, [{"value": 1}])

        assert len(w) == 0
