"""Result types for asset and DAG execution."""

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from interloper.assets.keys import AssetInstanceKey
from interloper.partitioning.base import Partition, PartitionWindow


class ExecutionStatus(str, Enum):
    """Execution status for assets and runs."""

    QUEUED = "queued"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class AssetExecutionInfo:
    """Execution information for a single asset."""

    asset_key: AssetInstanceKey
    status: ExecutionStatus
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    error: str | None = None

    @property
    def execution_time(self) -> float | None:
        """Computed execution time in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def mark_running(self) -> None:
        """Mark the asset as running."""
        self.status = ExecutionStatus.RUNNING
        self.start_time = dt.datetime.now(dt.timezone.utc)

    def mark_completed(self) -> None:
        """Mark the asset as completed."""
        self.status = ExecutionStatus.COMPLETED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def mark_failed(self, error: str) -> None:
        """Mark the asset as failed."""
        self.status = ExecutionStatus.FAILED
        self.end_time = dt.datetime.now(dt.timezone.utc)
        self.error = error

    def mark_cancelled(self) -> None:
        """Mark the asset as cancelled."""
        self.status = ExecutionStatus.CANCELLED
        self.end_time = dt.datetime.now(dt.timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            A dict representation of this execution info.
        """
        return {
            "asset_key": self.asset_key,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time": self.execution_time,
            "error": self.error,
        }


@dataclass
class RunResult:
    """Result of a single DAG execution (one partition, window, or unpartitioned)."""

    partition_or_window: Partition | PartitionWindow | None = None
    status: ExecutionStatus = ExecutionStatus.COMPLETED
    asset_executions: dict[AssetInstanceKey, AssetExecutionInfo] = field(default_factory=dict)
    execution_time: float = 0.0

    # Backward compatibility
    @property
    def completed_assets(self) -> list[AssetInstanceKey]:
        """List of asset keys that completed successfully."""
        return [k for k, v in self.asset_executions.items() if v.status == ExecutionStatus.COMPLETED]

    @property
    def failed_assets(self) -> list[AssetInstanceKey]:
        """List of asset keys that failed."""
        return [k for k, v in self.asset_executions.items() if v.status == ExecutionStatus.FAILED]

    def __str__(self) -> str:
        """Human-friendly summary string when printed.

        Returns:
            A formatted summary of this run result.

        Example:
            RunResult(status=completed, partition=2025-01-01, executed=5, failed=0, time=2.34s)
        """
        identifier: str
        if self.partition_or_window is None:
            identifier = "partition=None"
        elif isinstance(self.partition_or_window, PartitionWindow):
            identifier = f"window={self.partition_or_window}"
        else:
            identifier = f"partition={self.partition_or_window}"

        completed_count = len(self.completed_assets)
        failed_count = len(self.failed_assets)

        parts: list[str] = [
            f"status={self.status.value}",
            identifier,
            f"completed={completed_count}",
            f"failed={failed_count}",
            f"time={self.execution_time:.2f}s",
        ]

        # If there are failures, include a short list of failed asset names
        if failed_count > 0:
            # limit to first 5 to keep it digestible
            failed_preview = ", ".join(self.failed_assets[:5])
            if failed_count > 5:
                failed_preview += f" +{failed_count - 5} more"
            parts.append(f"failed_assets=[{failed_preview}]")

        return "RunResult(" + ", ".join(parts) + ")"

