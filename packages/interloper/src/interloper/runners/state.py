"""Run state tracking and dynamic asset scheduling."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from interloper.assets.base import Asset
from interloper.assets.keys import AssetInstanceKey
from interloper.dag.base import DAG
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import AssetExecutionInfo, ExecutionStatus


class RunState:
    """Tracks asset execution state and determines which assets are ready to run.

    Used by runners to dynamically schedule assets based on dependency
    completion rather than static DAG levels.
    """

    def __init__(
        self,
        dag: DAG,
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize the run state.

        Args:
            dag: The DAG to track.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).
                A run_id is generated automatically if not provided.
        """
        self.dag = dag
        self.metadata: dict[str, Any] = metadata or {}
        if "run_id" not in self.metadata:
            self.metadata["run_id"] = str(uuid.uuid4())
        self.asset_executions: dict[AssetInstanceKey, AssetExecutionInfo] = {}
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

            self.asset_executions[asset.instance_key] = AssetExecutionInfo(asset_key=asset.instance_key, status=status)

        # Second pass to find ready assets
        for asset in self.queued_assets:
            has_parents = self.dag.predecessors[asset.instance_key] is not None
            all_parents_skipped = all(
                self.asset_executions[pred].status == ExecutionStatus.SKIPPED
                for pred in self.dag.predecessors[asset.instance_key]
            )
            if not has_parents or all_parents_skipped:
                self.asset_executions[asset.instance_key].status = ExecutionStatus.READY

    @property
    def queued_assets(self) -> list[Asset]:
        """List of assets waiting to be scheduled."""
        return self.assets_with_status(ExecutionStatus.QUEUED)

    @property
    def ready_assets(self) -> list[Asset]:
        """List of assets whose dependencies are met and can be executed."""
        return self.assets_with_status(ExecutionStatus.READY)

    @property
    def running_assets(self) -> list[Asset]:
        """List of assets currently being executed."""
        return self.assets_with_status(ExecutionStatus.RUNNING)

    @property
    def completed_assets(self) -> list[Asset]:
        """List of assets that completed successfully."""
        return self.assets_with_status(ExecutionStatus.COMPLETED)

    @property
    def failed_assets(self) -> list[Asset]:
        """List of assets that failed."""
        return self.assets_with_status(ExecutionStatus.FAILED)

    @property
    def elapsed_time(self) -> float | None:
        """Elapsed wall-clock time of the run in seconds, or None if not finished."""
        return (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else None

    def assets_with_status(self, status: ExecutionStatus) -> list[Asset]:
        """Return all assets matching the given execution status."""
        return [asset for asset in self.dag.assets if self.asset_executions[asset.instance_key].status == status]

    def is_run_complete(self) -> bool:
        """Check whether every asset has reached a terminal state.

        Returns:
            True if all assets are completed, failed, or skipped.
        """
        return all(
            exec_info.status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.SKIPPED)
            for exec_info in self.asset_executions.values()
        )

    def start_run(self, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Record the run start time and emit a RUN_STARTED event."""
        self.partition_or_window = partition_or_window
        self.start_time = dt.datetime.now(dt.timezone.utc)
        self.end_time = None

        metadata = {
            **self.metadata,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.RUN_STARTED, metadata=metadata)

    def end_run(self, status: ExecutionStatus, error: str | None = None) -> dict[AssetInstanceKey, AssetExecutionInfo]:
        """Record the run end time, emit a terminal event, and return asset executions.

        Returns:
            A copy of the asset execution info dictionary.
        """
        self.end_time = dt.datetime.now(dt.timezone.utc)

        event_type = EventType.RUN_COMPLETED if status == ExecutionStatus.COMPLETED else EventType.RUN_FAILED
        metadata = {
            **self.metadata,
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
            "error": error,
        }
        emit(event_type, metadata=metadata)

        return self.asset_executions.copy()

    def mark_asset_running(self, asset: Asset) -> None:
        """Transition an asset to RUNNING and emit ASSET_STARTED."""
        self.asset_executions[asset.instance_key].mark_running()

        metadata = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.ASSET_STARTED, metadata=metadata)

    def mark_asset_completed(self, asset: Asset) -> None:
        """Transition an asset to COMPLETED, emit event, and promote ready dependents."""
        self.asset_executions[asset.instance_key].mark_completed()

        metadata = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
        }
        emit(EventType.ASSET_COMPLETED, metadata=metadata)

        self._update_dependent_assets(asset.instance_key)

    def mark_asset_failed(self, asset: Asset, error: str, tb: str | None = None) -> None:
        """Transition an asset to FAILED, emit event, and propagate failure to dependents.

        Args:
            asset: The asset that failed.
            error: Error message describing the failure.
            tb: Optional formatted traceback string.
        """
        self.asset_executions[asset.instance_key].mark_failed(error)

        metadata: dict[str, Any] = {
            **self.metadata,
            **get_asset_event_metadata(asset),
            "partition_or_window": str(self.partition_or_window) if self.partition_or_window is not None else None,
            "error": error,
        }
        if tb:
            metadata["traceback"] = tb
        emit(EventType.ASSET_FAILED, metadata=metadata)

        self._propagate_failure(asset.instance_key)

    def mark_asset_cancelled(self, asset: Asset) -> None:
        """Transition an asset to CANCELLED."""
        self.asset_executions[asset.instance_key].mark_cancelled()

    def _update_dependent_assets(self, completed_asset: AssetInstanceKey) -> None:
        """Promote queued successors to READY if all their dependencies are met."""
        # Use successors to get all assets that depend on the completed one
        for successor in self.dag.successors.get(completed_asset, []):
            asset = self.dag.asset_map[successor]

            # Check if all predecessors are completed
            predecessors = self.dag.predecessors[successor]
            completed_keys = [asset.instance_key for asset in self.completed_assets]
            all_preds_completed = all(pred in completed_keys for pred in predecessors)

            if self.asset_executions[asset.instance_key].status == ExecutionStatus.QUEUED and all_preds_completed:
                self.asset_executions[asset.instance_key].status = ExecutionStatus.READY

    def _propagate_failure(self, failed_asset: AssetInstanceKey) -> None:
        """Recursively mark all downstream dependents as FAILED."""
        # Use successors to get all assets that depend on the failed one
        for successor in self.dag.successors.get(failed_asset, []):
            asset = self.dag.asset_map[successor]

            if self.asset_executions[asset.instance_key].status in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED):
                continue

            self.asset_executions[asset.instance_key].status = ExecutionStatus.FAILED
            # Recursively propagate failure down the graph
            self._propagate_failure(successor)
