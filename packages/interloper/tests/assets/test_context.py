"""Tests for ExecutionContext."""

import datetime as dt

import pytest

import interloper as il


class TestExecutionContext:
    """Tests for ExecutionContext."""

    def test_initialization(self):
        """Test ExecutionContext initialization."""
        context = il.ExecutionContext(il.AssetInstanceKey("my_asset"))
        assert context._partition_or_window is None
        assert context.asset_key == "my_asset"

    def test_with_partition_value(self):
        """Test ExecutionContext with partition value."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=partition)
        assert context._partition_or_window == partition

    def test_with_partition_window(self):
        """Test ExecutionContext with partition window."""
        window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=window)
        assert context._partition_or_window == window

    def test_partition_date_with_time_partitioning(self):
        """Test partition_date property with time partitioning."""
        partitioning = il.TimePartitionConfig(column="date")
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.ExecutionContext(
            asset_key="my_asset",
            partition_or_window=partition,
            partitioning=partitioning,
        )
        assert context.partition_date == dt.date(2025, 1, 1)

    def test_partition_date_without_partition(self):
        """Test partition_date property without partition raises error."""
        partitioning = il.TimePartitionConfig(column="date")
        context = il.ExecutionContext(asset_key="my_asset", partitioning=partitioning)

        with pytest.raises(AttributeError, match="`context.partition_date` is not available, no partition provided."):
            context.partition_date

    def test_partition_date_without_time_partitioning(self):
        """Test partition_date property without time partitioning raises error."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=partition)

        with pytest.raises(AttributeError, match="asset is not partitioned"):
            context.partition_date

    def test_partition_date_window_with_allow_window(self):
        """Test partition_date_window with allow_window=True."""
        partitioning = il.TimePartitionConfig(column="date", allow_window=True)
        window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        context = il.ExecutionContext(
            asset_key="my_asset",
            partition_or_window=window,
            partitioning=partitioning,
        )
        assert context.partition_date_window == (dt.date(2025, 1, 1), dt.date(2025, 1, 7))

    def test_partition_date_window_without_allow_window(self):
        """Test partition_date_window without allow_window raises error."""
        partitioning = il.TimePartitionConfig(column="date", allow_window=False)
        window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        context = il.ExecutionContext(
            asset_key="my_asset",
            partition_or_window=window,
            partitioning=partitioning,
        )

        with pytest.raises(AttributeError, match="asset does not allow windows"):
            context.partition_date_window

    def test_partition_date_window_without_window(self):
        """Test partition_date_window without window raises error."""
        partitioning = il.TimePartitionConfig(column="date", allow_window=True)
        context = il.ExecutionContext(asset_key="my_asset", partitioning=partitioning)

        with pytest.raises(AttributeError, match="no partition provided"):
            context.partition_date_window

    def test_partition_date_window_without_time_partitioning(self):
        """Test partition_date_window without time partitioning raises error."""
        window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=window)

        with pytest.raises(AttributeError, match="asset is not partitioned"):
            context.partition_date_window

    def test_all_parameters(self):
        """Test ExecutionContext with all parameters."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        partitioning = il.TimePartitionConfig(column="date")

        context = il.ExecutionContext(
            asset_key="my_asset",
            partition_or_window=partition,
            partitioning=partitioning,
        )

        assert context._partition_or_window == partition
        assert context.partition_date == dt.date(2025, 1, 1)
        assert context.asset_key == "my_asset"

    def test_partition_date_with_partition_window_raises_error(self):
        """Test that partition_date raises error when context has a partition window."""
        partitioning = il.TimePartitionConfig(column="date")
        window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=window, partitioning=partitioning)

        with pytest.raises(AttributeError, match="Context currently holds a partition window"):
            context.partition_date

    def test_partition_date_window_with_single_partition_returns_tuple_of_same_date(self):
        """Test that partition_date_window returns a tuple of the same date when context has a single partition."""
        partitioning = il.TimePartitionConfig(column="date", allow_window=True)
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.ExecutionContext(asset_key="my_asset", partition_or_window=partition, partitioning=partitioning)
        assert context.partition_date_window == (dt.date(2025, 1, 1), dt.date(2025, 1, 1))
