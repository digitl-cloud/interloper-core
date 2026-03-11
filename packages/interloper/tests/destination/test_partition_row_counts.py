"""Tests for partition_row_counts across Destination backends and Asset."""

from __future__ import annotations

import datetime as dt

import pytest
from interloper_sql import SqliteDestination

import interloper as il
from interloper.destination.context import DestinationContext

# ------------------------------------------------------------------
# Asset.partition_row_counts
# ------------------------------------------------------------------


class TestAssetPartitionRowCounts:
    """Tests for Asset.partition_row_counts()."""

    def test_with_sqlite(self):
        dest = SqliteDestination(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = DestinationContext(asset=asset, partition_or_window=p1)
        ctx2 = DestinationContext(asset=asset, partition_or_window=p2)
        dest.write(ctx1, [{"ds": "2025-01-01", "v": 1}, {"ds": "2025-01-01", "v": 2}])
        dest.write(ctx2, [{"ds": "2025-01-02", "v": 3}])

        counts = asset.partition_row_counts()
        assert counts["2025-01-01"] == 2
        assert counts["2025-01-02"] == 1

    def test_non_partitioned_raises(self):
        dest = SqliteDestination(database=":memory:")

        @il.asset
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"id": 1}]

        asset = my_asset(destination=dest)
        with pytest.raises(il.PartitionError, match="not partitioned"):
            asset.partition_row_counts()

    def test_multi_destination_explicit_key(self):
        dest1 = SqliteDestination(key="primary", database=":memory:")
        dest2 = SqliteDestination(key="backup", database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=[dest1, dest2], default_destination_key="primary")

        # Write only to primary
        p1 = il.TimePartition(dt.date(2025, 1, 1))
        ctx = DestinationContext(asset=asset, partition_or_window=p1)
        dest1.write(ctx, [{"ds": "2025-01-01", "v": 1}])

        counts = asset.partition_row_counts(destination_key="primary")
        assert counts["2025-01-01"] == 1

    def test_multi_destination_invalid_key_raises(self):
        dest1 = SqliteDestination(key="primary", database=":memory:")
        dest2 = SqliteDestination(key="secondary", database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=[dest1, dest2], default_destination_key="primary")
        with pytest.raises(il.ConfigError, match="Destination key 'missing'"):
            asset.partition_row_counts(destination_key="missing")

    def test_empty_table_raises(self):
        """Table doesn't exist yet -- should raise."""
        dest = SqliteDestination(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)
        with pytest.raises(il.TableNotFoundError):
            asset.partition_row_counts()


# ------------------------------------------------------------------
# MemoryDestination.partition_row_counts
# ------------------------------------------------------------------


class TestMemoryDestinationPartitionRowCounts:
    """Tests for MemoryDestination.partition_row_counts()."""

    def setup_method(self):
        """Clear memory storage before each test."""
        il.MemoryDestination.clear()

    def test_counts_partitions(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset()  # defaults to MemoryDestination

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = DestinationContext(asset=asset, partition_or_window=p1)
        ctx2 = DestinationContext(asset=asset, partition_or_window=p2)
        asset.destination.write(ctx1, [{"ds": "2025-01-01"}, {"ds": "2025-01-01"}])
        asset.destination.write(ctx2, [{"ds": "2025-01-02"}])

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
# FileDestination.partition_row_counts
# ------------------------------------------------------------------


class TestFileDestinationPartitionRowCounts:
    """Tests for FileDestination.partition_row_counts()."""

    def test_counts_partitions(self, tmp_path):
        dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)

        p1 = il.TimePartition(dt.date(2025, 1, 1))
        p2 = il.TimePartition(dt.date(2025, 1, 2))
        ctx1 = DestinationContext(asset=asset, partition_or_window=p1)
        ctx2 = DestinationContext(asset=asset, partition_or_window=p2)
        dest.write(ctx1, [{"ds": "2025-01-01"}, {"ds": "2025-01-01"}, {"ds": "2025-01-01"}])
        dest.write(ctx2, [{"ds": "2025-01-02"}])

        counts = asset.partition_row_counts()
        assert counts["2025-01-01"] == 3
        assert counts["2025-01-02"] == 1

    def test_no_directory_returns_empty(self, tmp_path):
        dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)
        counts = asset.partition_row_counts()
        assert counts == {}


# ------------------------------------------------------------------
# SqliteDestination.partition_row_counts (via DatabaseDestination)
# ------------------------------------------------------------------


class TestSqliteDestinationPartitionRowCounts:
    """Tests for SqlDestination._count_by_partition via SqliteDestination."""

    def test_counts_multiple_partitions(self):
        dest = SqliteDestination(database=":memory:")

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)

        p1 = il.TimePartition(dt.date(2025, 3, 1))
        p2 = il.TimePartition(dt.date(2025, 3, 2))
        p3 = il.TimePartition(dt.date(2025, 3, 3))
        for p, rows in [
            (p1, [{"ds": "2025-03-01", "v": i} for i in range(5)]),
            (p2, [{"ds": "2025-03-02", "v": i} for i in range(3)]),
            (p3, [{"ds": "2025-03-03", "v": 0}]),
        ]:
            ctx = DestinationContext(asset=asset, partition_or_window=p)
            dest.write(ctx, rows)

        counts = asset.partition_row_counts()
        assert counts == {"2025-03-01": 5, "2025-03-02": 3, "2025-03-03": 1}

    def test_single_partition(self):
        dest = SqliteDestination(database=":memory:")

        @il.asset(partitioning=il.PartitionConfig(column="region"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        asset = my_asset(destination=dest)

        ctx = DestinationContext(asset=asset)
        dest.write(
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
