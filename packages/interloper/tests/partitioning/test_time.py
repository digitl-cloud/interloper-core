"""Tests for TimePartitionConfig."""

import interloper as il


class TestTimePartitionConfig:
    """Tests for TimePartitionConfig."""

    def test_initialization(self):
        """Test TimePartitionConfig initialization."""
        config = il.TimePartitionConfig(column="date")
        assert config.column == "date"
        assert config.allow_window is False

    def test_default_allow_window(self):
        """Test default allow_window value."""
        config = il.TimePartitionConfig(column="date")
        assert config.allow_window is False

    def test_allow_window_true(self):
        """Test allow_window=True."""
        config = il.TimePartitionConfig(column="date", allow_window=True)
        assert config.allow_window is True

    def test_allow_window_false(self):
        """Test allow_window=False."""
        config = il.TimePartitionConfig(column="date", allow_window=False)
        assert config.allow_window is False

    def test_column_name(self):
        """Test custom column name."""
        config = il.TimePartitionConfig(column="timestamp")
        assert config.column == "timestamp"

    def test_inherits_from_partition_config(self):
        """Test that TimePartitionConfig inherits from PartitionConfig."""
        config = il.TimePartitionConfig(column="date")
        assert isinstance(config, il.PartitionConfig)

    def test_with_asset_decorator(self):
        """Test TimePartitionConfig with asset decorator."""
        config = il.TimePartitionConfig(column="date")

        @il.asset(partitioning=config)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        assert my_asset.partitioning == config

    def test_with_asset_allow_window(self):
        """Test TimePartitionConfig with allow_window in asset."""
        config = il.TimePartitionConfig(column="date", allow_window=True)

        @il.asset(partitioning=config)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            start, end = context.partition_date_window
            return [{"start": start, "end": end}]

        assert my_asset.partitioning.allow_window is True

