"""Tests for partition_row_counts across IO backends and Asset."""

from __future__ import annotations

import datetime as dt

import pytest
from interloper_sql import SqliteIO

import interloper as il
from interloper.io.context import IOContext

# ------------------------------------------------------------------
# Asset.partition_row_counts
# ------------------------------------------------------------------


class TestAssetPartitionRowCounts:
    """Tests for Asset.partition_row_counts()."""

    def test_with_sqlite(self):
        io = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = IOContext(asset=asset, partition_or_window=p1)
        ctx2 = IOContext(asset=asset, partition_or_window=p2)
        io.write(ctx1, [{"ds": "2025-01-01", "v": 1}, {"ds": "2025-01-01", "v": 2}])
        io.write(ctx2, [{"ds": "2025-01-02", "v": 3}])

        counts = asset.partition_row_counts()
        assert counts["2025-01-01"] == 2
        assert counts["2025-01-02"] == 1

    def test_non_partitioned_raises(self):
        io = SqliteIO(database=":memory:")

        @il.asset
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"id": 1}]

        asset = my_asset(io=io)
        with pytest.raises(il.PartitionError, match="not partitioned"):
            asset.partition_row_counts()

    def test_multi_io_explicit_key(self):
        io1 = SqliteIO(database=":memory:")
        io2 = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io={"primary": io1, "backup": io2})

        # Write only to primary
        p1 = il.TimePartition(dt.date(2025, 1, 1))
        ctx = IOContext(asset=asset, partition_or_window=p1)
        io1.write(ctx, [{"ds": "2025-01-01", "v": 1}])

        counts = asset.partition_row_counts(io_key="primary")
        assert counts["2025-01-01"] == 1

    def test_multi_io_invalid_key_raises(self):
        io = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io={"primary": io})
        with pytest.raises(il.ConfigError, match="IO key 'missing'"):
            asset.partition_row_counts(io_key="missing")

    def test_empty_table_raises(self):
        """Table doesn't exist yet — should raise."""
        io = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)
        with pytest.raises(il.TableNotFoundError):
            asset.partition_row_counts()


# ------------------------------------------------------------------
# MemoryIO.partition_row_counts
# ------------------------------------------------------------------


class TestMemoryIOPartitionRowCounts:
    """Tests for MemoryIO.partition_row_counts()."""

    def setup_method(self):
        """Clear memory storage before each test."""
        il.MemoryIO.clear()

    def test_counts_partitions(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset()  # defaults to MemoryIO

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = IOContext(asset=asset, partition_or_window=p1)
        ctx2 = IOContext(asset=asset, partition_or_window=p2)
        asset.io.write(ctx1, [{"ds": "2025-01-01"}, {"ds": "2025-01-01"}])
        asset.io.write(ctx2, [{"ds": "2025-01-02"}])

        counts = asset.partition_row_counts()
        assert counts["2025-01-01"] == 2
        assert counts["2025-01-02"] == 1

    def test_empty_returns_empty_dict(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset()
        counts = asset.partition_row_counts()
        assert counts == {}


# ------------------------------------------------------------------
# FileIO.partition_row_counts
# ------------------------------------------------------------------


class TestFileIOPartitionRowCounts:
    """Tests for FileIO.partition_row_counts()."""

    def test_counts_partitions(self, tmp_path):
        io = il.FileIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = IOContext(asset=asset, partition_or_window=p1)
        ctx2 = IOContext(asset=asset, partition_or_window=p2)
        io.write(ctx1, [{"ds": "2025-01-01"}, {"ds": "2025-01-01"}, {"ds": "2025-01-01"}])
        io.write(ctx2, [{"ds": "2025-01-02"}])

        counts = asset.partition_row_counts()
        assert counts["2025-01-01"] == 3
        assert counts["2025-01-02"] == 1

    def test_no_directory_returns_empty(self, tmp_path):
        io = il.FileIO(str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)
        counts = asset.partition_row_counts()
        assert counts == {}


# ------------------------------------------------------------------
# SqliteIO.partition_row_counts (via DatabaseIO)
# ------------------------------------------------------------------


class TestSqliteIOPartitionRowCounts:
    """Tests for SqlIO._count_by_partition via SqliteIO."""

    def test_counts_multiple_partitions(self):
        io = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)

        p1 = il.TimePartition(dt.date(2025, 3, 1))
        p2 = il.TimePartition(dt.date(2025, 3, 2))
        p3 = il.TimePartition(dt.date(2025, 3, 3))
        for p, rows in [
            (p1, [{"ds": "2025-03-01", "v": i} for i in range(5)]),
            (p2, [{"ds": "2025-03-02", "v": i} for i in range(3)]),
            (p3, [{"ds": "2025-03-03", "v": 0}]),
        ]:
            ctx = IOContext(asset=asset, partition_or_window=p)
            io.write(ctx, rows)

        counts = asset.partition_row_counts()
        assert counts == {"2025-03-01": 5, "2025-03-02": 3, "2025-03-03": 1}

    def test_single_partition(self):
        io = SqliteIO(database=":memory:")

        @il.asset(partitioning=il.PartitionConfig(column="region"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(io=io)

        ctx = IOContext(asset=asset)
        io.write(
            ctx,
            [
                {"region": "US", "v": 1},
                {"region": "US", "v": 2},
                {"region": "EU", "v": 3},
            ],
        )

        counts = asset.partition_row_counts()
        assert counts["US"] == 2
        assert counts["EU"] == 1
