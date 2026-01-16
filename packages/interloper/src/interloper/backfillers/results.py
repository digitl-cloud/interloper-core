"""Execution result classes for backfillers."""

import datetime as dt
from dataclasses import dataclass
from typing import Any

from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import ExecutionStatus, RunResult


@dataclass
class RunExecutionInfo:
    """Execution information for a single run (partition execution)."""

    partition_or_window: Partition | PartitionWindow | None
    status: ExecutionStatus
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    error: str | None = None
    result: RunResult | None = None

    @property
    def execution_time(self) -> float | None:
        """Computed execution time in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def mark_running(self) -> None:
        """Mark the run as running."""
        self.status = ExecutionStatus.RUNNING
        self.start_time = dt.datetime.now(dt.timezone.utc)

    def mark_completed(self, run_result: RunResult) -> None:
        """Mark the run as completed."""
        self.status = ExecutionStatus.COMPLETED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.result = run_result

    def mark_failed(self, error: str, run_result: RunResult | None = None) -> None:
        """Mark the run as failed."""
        self.status = ExecutionStatus.FAILED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.error = error
        self.result = run_result

    def mark_cancelled(self) -> None:
        """Mark the run as cancelled."""
        self.status = ExecutionStatus.CANCELLED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time": self.execution_time,
            "error": self.error,
        }


@dataclass
class BackfillResult:
    """Result of executing a backfill across multiple partitions."""

    status: ExecutionStatus
    run_executions: dict[Partition | PartitionWindow | None, RunExecutionInfo]
    execution_time: float = 0.0

    @property
    def completed_partitions(self) -> list[Partition | PartitionWindow | None]:
        """List of runs that completed successfully."""
        return [partition for partition, run in self.run_executions.items() if run.status == ExecutionStatus.COMPLETED]

    @property
    def failed_partitions(self) -> list[Partition | PartitionWindow | None]:
        """List of partitions that failed."""
        return [partition for partition, run in self.run_executions.items() if run.status == ExecutionStatus.FAILED]

    def __str__(self) -> str:
        """Human-friendly summary string when printed.

        Example:
            BackfillResult(range=2025-01-01→2025-01-07, total=7, success=6, failed=1, time=12.45s)
        """
        parts: list[str] = [
            f"status={self.status.value}",
            f"completed={len(self.completed_partitions)}",
            f"failed={len(self.failed_partitions)}",
            f"time={self.execution_time:.2f}s",
        ]

        return "BackfillResult(" + ", ".join(parts) + ")"
