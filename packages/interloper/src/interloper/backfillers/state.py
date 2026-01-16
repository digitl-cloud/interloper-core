"""State management for backfill execution."""

from __future__ import annotations

import datetime as dt
import uuid

from interloper.backfillers.results import RunExecutionInfo
from interloper.events.base import Event, EventType, emit
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import ExecutionStatus, RunResult


class BackfillState:
    """Tracks run executions for a backfill operation.

    Manages the state of multiple runs (partition executions) within a backfill,
    similar to how RunState tracks asset executions within a run.
    """

    def __init__(
        self,
        partitions: list[Partition | PartitionWindow | None],
        *,
        backfill_id: str | None = None,
    ):
        """Initialize the backfill state.

        Args:
            partitions: The partitions to backfill
            backfill_id: Optional backfill ID. If not provided, a UUID will be generated automatically.
        """
        self.partitions = partitions
        self.backfill_id: str = backfill_id if backfill_id is not None else str(uuid.uuid4())
        self.run_executions: dict[Partition | PartitionWindow | None, RunExecutionInfo] = {}
        self.start_time: dt.datetime | None = None
        self.end_time: dt.datetime | None = None

        self._initialize_runs()

    def _initialize_runs(self) -> None:
        """Initialize all runs as QUEUED."""
        for partition in self.partitions:
            self.run_executions[partition] = RunExecutionInfo(
                partition_or_window=partition,
                status=ExecutionStatus.QUEUED,
            )

    def runs_with_status(self, status: ExecutionStatus) -> list[RunExecutionInfo]:
        """Return all runs with the given execution status.

        Args:
            status: The desired ExecutionStatus to filter runs.

        Returns:
            List of RunExecutionInfo objects with the specified status.
        """
        return [run for run in self.run_executions.values() if run.status == status]

    @property
    def completed_runs(self) -> list[RunExecutionInfo]:
        """Get all runs that have completed successfully.

        Returns:
            List of runs that have completed successfully
        """
        return self.runs_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_runs(self) -> list[RunExecutionInfo]:
        """Get all runs that have failed.

        Returns:
            List of runs that have failed
        """
        return self.runs_with_status(ExecutionStatus.FAILED)

    @property
    def elapsed_time(self) -> float | None:
        """Get the elapsed time of the backfill execution.

        Returns:
            Elapsed time in seconds
        """
        return (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None

    def is_backfill_complete(self) -> bool:
        """Check if all runs are completed or failed.

        Returns:
            True if backfill is complete, False otherwise
        """
        return all(
            run.status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED) for run in self.run_executions.values()
        )

    def start_backfill(self) -> None:
        """Start backfill execution and emit BACKFILL_STARTED event."""
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.end_time = None

        emit(
            Event(
                type=EventType.BACKFILL_STARTED,
                backfill_id=self.backfill_id,
            )
        )

    def end_backfill(
        self,
        status: ExecutionStatus,
        error: str | None = None,
    ) -> dict[Partition | PartitionWindow | None, RunExecutionInfo]:
        """End backfill execution.

        Args:
            status: The final status of the backfill
            error: Optional error message if the backfill failed

        Returns:
            Copy of the run_executions dictionary
        """
        self.end_time = dt.datetime.now(dt.timezone.utc)

        emit(
            Event(
                type=EventType.BACKFILL_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.BACKFILL_FAILED,
                backfill_id=self.backfill_id,
                error=error,
            )
        )

        return self.run_executions.copy()

    def mark_run_running(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Mark a run as currently running.

        Args:
            partition_or_window: The partition or window to mark as running
        """
        self.run_executions[partition_or_window].mark_running()

    def mark_run_completed(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        run_result: RunResult,
    ) -> None:
        """Mark a run as completed.

        Args:
            partition_or_window: The partition or window to mark as completed
            run_result: The result of the run
        """
        self.run_executions[partition_or_window].mark_completed(run_result)

    def mark_run_failed(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        error: str,
        run_result: RunResult | None = None,
    ) -> None:
        """Mark a run as failed.

        Args:
            partition_or_window: The partition or window to mark as failed
            error: Error message describing the failure
            run_result: Optional run result if available
        """
        self.run_executions[partition_or_window].mark_failed(error, run_result)

    def mark_run_cancelled(
        self,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Mark a run as cancelled.

        Args:
            partition_or_window: The partition or window to mark as cancelled
        """
        self.run_executions[partition_or_window].mark_cancelled()
