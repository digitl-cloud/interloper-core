"""State management for backfill execution."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from interloper.backfillers.results import RunExecutionInfo
from interloper.events.base import EventType, emit
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
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize the backfill state.

        Args:
            partitions: The partitions to backfill
            metadata: Arbitrary metadata dict (e.g. backfill_id).
                      If backfill_id is not provided, a UUID will be generated automatically.
        """
        self.partitions = partitions
        self.metadata: dict[str, Any] = metadata or {}
        if "backfill_id" not in self.metadata:
            self.metadata["backfill_id"] = str(uuid.uuid4())
        self.run_executions: dict[Partition | PartitionWindow | None, RunExecutionInfo] = {}
        self.start_time: dt.datetime | None = None
        self.end_time: dt.datetime | None = None

        self._initialize_runs()

    @property
    def backfill_id(self) -> str:
        """Unique identifier for this backfill, auto-generated if not provided in metadata."""
        return self.metadata["backfill_id"]

    def _initialize_runs(self) -> None:
        """Initialize all runs as QUEUED."""
        for partition in self.partitions:
            self.run_executions[partition] = RunExecutionInfo(
                partition_or_window=partition,
                status=ExecutionStatus.QUEUED,
            )

    def runs_with_status(self, status: ExecutionStatus) -> list[RunExecutionInfo]:
        """Return all runs matching the given status."""
        return [run for run in self.run_executions.values() if run.status == status]

    @property
    def completed_runs(self) -> list[RunExecutionInfo]:
        """All runs that completed successfully."""
        return self.runs_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_runs(self) -> list[RunExecutionInfo]:
        """All runs that failed."""
        return self.runs_with_status(ExecutionStatus.FAILED)

    @property
    def elapsed_time(self) -> float | None:
        """Elapsed wall-clock time in seconds, or None if not yet finished."""
        return (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None

    def is_backfill_complete(self) -> bool:
        """Check if all runs have either completed or failed."""
        return all(
            run.status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED) for run in self.run_executions.values()
        )

    def start_backfill(self) -> None:
        """Start backfill execution and emit BACKFILL_STARTED event."""
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.end_time = None

        emit(EventType.BACKFILL_STARTED, metadata={**self.metadata})

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

        event_type = EventType.BACKFILL_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.BACKFILL_FAILED
        emit(event_type, metadata={**self.metadata, "error": error})

        return self.run_executions.copy()

    def mark_run_running(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Mark a run as currently running."""
        self.run_executions[partition_or_window].mark_running()

    def mark_run_completed(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        run_result: RunResult,
    ) -> None:
        """Mark a run as completed with its result."""
        self.run_executions[partition_or_window].mark_completed(run_result)

    def mark_run_failed(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        error: str,
        run_result: RunResult | None = None,
    ) -> None:
        """Mark a run as failed with an error message."""
        self.run_executions[partition_or_window].mark_failed(error, run_result)

    def mark_run_cancelled(
        self,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Mark a run as cancelled."""
        self.run_executions[partition_or_window].mark_cancelled()
