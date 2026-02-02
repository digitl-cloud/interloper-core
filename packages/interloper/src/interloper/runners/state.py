"""Dynamic scheduler for asset execution."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from interloper.assets.base import Asset
from interloper.dag.base import DAG
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import AssetExecutionInfo, ExecutionStatus


class RunState:
    """Tracks asset states and finds schedulable assets.

    Provides dynamic scheduling by continuously finding assets that are ready
    to execute based on completed dependencies, rather than being constrained
    to DAG levels.
    """

    def __init__(
        self,
        dag: DAG,
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize the state.

        Args:
            dag: The DAG to track
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
                      If run_id is not provided, a UUID will be generated automatically.
        """
        self.dag = dag
        self.metadata: dict[str, Any] = metadata or {}
        if "run_id" not in self.metadata:
            self.metadata["run_id"] = str(uuid.uuid4())
        self.asset_executions: dict[str, AssetExecutionInfo] = {}
        self.partition_or_window: Partition | PartitionWindow | None = None
        self.start_time: dt.datetime | None = None
        self.end_time: dt.datetime | None = None

        self._initialize_assets()

    @property
    def run_id(self) -> str:
        """The run ID."""
        return self.metadata["run_id"]

    @property
    def backfill_id(self) -> str | None:
        """The backfill ID, if set."""
        return self.metadata.get("backfill_id")

    def _initialize_assets(self) -> None:
        """Initialize all assets as QUEUED, mark those with no predecessors as READY."""
        # First pass to mark all assets as QUEUED or SKIPPED
        for asset in self.dag.assets:
            if not asset.materializable:
                status = ExecutionStatus.SKIPPED
            else:
                status = ExecutionStatus.QUEUED

            self.asset_executions[asset.key] = AssetExecutionInfo(asset_key=asset.key, status=status)

        # Second pass to find ready assets
        for asset in self.queued_assets:
            has_parents = self.dag.predecessors[asset.key] is not None
            all_parents_skipped = all(
                self.asset_executions[pred].status == ExecutionStatus.SKIPPED
                for pred in self.dag.predecessors[asset.key]
            )
            if not has_parents or all_parents_skipped:
                self.asset_executions[asset.key].status = ExecutionStatus.READY

    @property
    def queued_assets(self) -> list[Asset]:
        """Get all assets that are queued.

        Returns:
            List of assets that are queued
        """
        return self.assets_with_status(ExecutionStatus.QUEUED)

    @property
    def ready_assets(self) -> list[Asset]:
        """Get all assets that are ready to be scheduled.

        Returns:
            List of assets that can be executed immediately
        """
        return self.assets_with_status(ExecutionStatus.READY)

    @property
    def running_assets(self) -> list[Asset]:
        """Get all assets that are currently running.

        Returns:
            List of assets that are currently running
        """
        return self.assets_with_status(ExecutionStatus.RUNNING)

    @property
    def completed_assets(self) -> list[Asset]:
        """Get set of completed asset names.

        Returns:
            Set of asset names that have completed successfully
        """
        return self.assets_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_assets(self) -> list[Asset]:
        """Get set of failed asset names.

        Returns:
            Set of asset names that have failed
        """
        return self.assets_with_status(ExecutionStatus.FAILED)

    @property
    def elapsed_time(self) -> float | None:
        """Get the elapsed time of the execution.

        Returns:
            Elapsed time in seconds
        """
        return (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None

    def assets_with_status(self, status: ExecutionStatus) -> list[Asset]:
        """Return all assets with the given execution status.

        Args:
            status: The desired ExecutionStatus to filter assets.

        Returns:
            List of Asset objects with the specified status.
        """
        return [asset for asset in self.dag.assets if self.asset_executions[asset.key].status == status]

    def is_run_complete(self) -> bool:
        """Check if all assets are completed or failed.

        Returns:
            True if execution is complete, False otherwise
        """
        return all(
            exec_info.status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.SKIPPED)
            for exec_info in self.asset_executions.values()
        )

    def start_run(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Start DAG execution and emit RUN_STARTED event.

        Args:
            partition_or_window: Either a Partition or PartitionWindow object
        """
        self.partition_or_window = partition_or_window
        self.start_time = dt.datetime.now()
        self.end_time = None

        metadata = {
            **self.metadata,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.RUN_STARTED, metadata=metadata)

    def end_run(self, status: ExecutionStatus, error: str | None = None) -> dict[str, AssetExecutionInfo]:
        """End DAG execution, emit RUN_COMPLETED/FAILED event, return asset_executions."""
        self.end_time = dt.datetime.now()

        event_type = EventType.RUN_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.RUN_FAILED
        metadata = {
            **self.metadata,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
            "error": error,
        }
        emit(event_type, metadata=metadata)

        return self.asset_executions.copy()

    def mark_asset_running(self, asset: Asset) -> None:
        """Mark an asset as currently running and emit ASSET_STARTED event.

        Args:
            asset: The asset to mark as running
        """
        self.asset_executions[asset.key].mark_running()

        metadata = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.ASSET_STARTED, metadata=metadata)

    def mark_asset_completed(self, asset: Asset) -> None:
        """Mark an asset as completed and emit ASSET_COMPLETED event.

        Args:
            asset: The asset to mark as completed
        """
        self.asset_executions[asset.key].mark_completed()

        metadata = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.ASSET_COMPLETED, metadata=metadata)

        self._update_dependent_assets(asset.key)

    def mark_asset_failed(self, asset: Asset, error: str) -> None:
        """Mark an asset as failed and emit ASSET_FAILED event.

        Args:
            asset: The asset to mark as failed
            error: Error message describing the failure
        """
        self.asset_executions[asset.key].mark_failed(error)

        metadata = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.ASSET_FAILED, metadata=metadata)

        self._propagate_failure(asset.key)

    def mark_asset_cancelled(self, asset: Asset) -> None:
        """Mark an asset as cancelled and emit ASSET_CANCELLED event.

        Args:
            asset: The asset to mark as cancelled
        """
        self.asset_executions[asset.key].mark_cancelled()

    def _update_dependent_assets(self, completed_asset: str) -> None:
        """Check if any dependent assets are now ready.

        Uses successors to efficiently find assets that depend on the completed one.

        Args:
            completed_asset: Name of the asset that just completed
        """
        # Use successors to get all assets that depend on the completed one
        for successor in self.dag.successors.get(completed_asset, []):
            asset = self.dag.asset_map[successor]

            # Check if all predecessors are completed
            predecessors = self.dag.predecessors[successor]
            all_preds_completed = all(pred in [asset.key for asset in self.completed_assets] for pred in predecessors)

            if self.asset_executions[asset.key].status == ExecutionStatus.QUEUED and all_preds_completed:
                self.asset_executions[asset.key].status = ExecutionStatus.READY

    def _propagate_failure(self, failed_asset: str) -> None:
        """Mark dependents as FAILED if they depend (directly or indirectly) on a failed asset.

        Uses successors to efficiently propagate failures down the graph.
        This ensures the scheduler reaches a terminal state when some branches fail.
        """
        # Use successors to get all assets that depend on the failed one
        for successor in self.dag.successors.get(failed_asset, []):
            asset = self.dag.asset_map[successor]

            if self.asset_executions[asset.key].status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED):
                continue

            self.asset_executions[asset.key].status = ExecutionStatus.FAILED
            # Recursively propagate failure down the graph
            self._propagate_failure(successor)
