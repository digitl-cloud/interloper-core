"""Tests for SerialBackfiller."""

import datetime as dt

import interloper as il


class TestSerialBackfiller:
    """Tests for SerialBackfiller."""

    def test_initialization(self):
        """Test SerialBackfiller initialization."""
        backfiller = il.SerialBackfiller()
        assert isinstance(backfiller, il.SerialBackfiller)
        assert isinstance(backfiller, il.Backfiller)

    def test_windowed_false_splits_window_into_partition_runs(self, tmp_path):
        """Backfill with windowed=False runs each partition separately."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=False))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        backfiller = il.SerialBackfiller(runner=il.SerialRunner())
        result = backfiller.backfill(
            dag=dag,
            partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3)),
            windowed=False,
        )

        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.run_executions) == 3
        assert len(result.completed_partitions) == 3

    def test_windowed_true_fails_when_window_not_allowed(self, tmp_path):
        """Backfill with windowed=True fails if assets do not allow windows."""

        @il.asset(partitioning=il.TimePartitionConfig(column="date", allow_window=False))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date}]

        dag = il.DAG(my_asset(io=il.FileIO(tmp_path)))
        backfiller = il.SerialBackfiller(runner=il.SerialRunner())
        result = backfiller.backfill(
            dag=dag,
            partition_or_window=il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3)),
            windowed=True,
        )

        assert result.status == il.ExecutionStatus.FAILED
        assert len(result.failed_partitions) == 1

