"""Tests for backfiller results."""

import datetime as dt

import interloper as il
from interloper.backfillers.results import RunExecutionInfo


class TestRunExecutionInfo:
    """Tests for RunExecutionInfo."""

    def test_initialization(self):
        """Test RunExecutionInfo initialization."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        info = RunExecutionInfo(
            partition_or_window=partition,
            status=il.ExecutionStatus.QUEUED,
        )
        assert info.partition_or_window == partition
        assert info.status == il.ExecutionStatus.QUEUED
        assert info.start_time is None
        assert info.end_time is None
        assert info.error is None
        assert info.result is None

    def test_mark_running(self):
        """Test mark_running method."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        info = RunExecutionInfo(
            partition_or_window=partition,
            status=il.ExecutionStatus.QUEUED,
        )
        info.mark_running()
        assert info.status == il.ExecutionStatus.RUNNING
        assert info.start_time is not None

    def test_mark_completed(self):
        """Test mark_completed method."""
        partition = il.TimePartition(dt.date(2025, 1, 1))
        info = RunExecutionInfo(
            partition_or_window=partition,
            status=il.ExecutionStatus.RUNNING,
        )
        run_result = il.RunResult(partition_or_window=partition)
        info.mark_completed(run_result)
        assert info.status == il.ExecutionStatus.COMPLETED
        assert info.result == run_result
        assert info.end_time is not None


class TestBackfillResult:
    """Tests for BackfillResult."""

    def test_initialization(self):
        """Test BackfillResult initialization."""
        partition1 = il.TimePartition(dt.date(2025, 1, 1))
        partition2 = il.TimePartition(dt.date(2025, 1, 2))
        run_executions = {
            partition1: RunExecutionInfo(partition1, il.ExecutionStatus.COMPLETED),
            partition2: RunExecutionInfo(partition2, il.ExecutionStatus.COMPLETED),
        }
        result = il.BackfillResult(
            status=il.ExecutionStatus.COMPLETED,
            run_executions=run_executions,
        )
        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.run_executions) == 2

    def test_completed_partitions(self):
        """Test completed_partitions property."""
        partition1 = il.TimePartition(dt.date(2025, 1, 1))
        partition2 = il.TimePartition(dt.date(2025, 1, 2))
        run_executions = {
            partition1: RunExecutionInfo(partition1, il.ExecutionStatus.COMPLETED),
            partition2: RunExecutionInfo(partition2, il.ExecutionStatus.FAILED),
        }
        result = il.BackfillResult(
            status=il.ExecutionStatus.FAILED,
            run_executions=run_executions,
        )
        assert result.completed_partitions == [partition1]

    def test_failed_partitions(self):
        """Test failed_partitions property."""
        partition1 = il.TimePartition(dt.date(2025, 1, 1))
        partition2 = il.TimePartition(dt.date(2025, 1, 2))
        run_executions = {
            partition1: RunExecutionInfo(partition1, il.ExecutionStatus.COMPLETED),
            partition2: RunExecutionInfo(partition2, il.ExecutionStatus.FAILED),
        }
        result = il.BackfillResult(
            status=il.ExecutionStatus.FAILED,
            run_executions=run_executions,
        )
        assert result.failed_partitions == [partition2]

