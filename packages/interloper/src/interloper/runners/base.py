"""Base runner class."""

from __future__ import annotations

import traceback
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from typing_extensions import Self

from interloper.assets.base import Asset
from interloper.dag.base import DAG
from interloper.events.base import Event, flush, subscribe, unsubscribe
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.runners.state import RunState
from interloper.serialization.base import Serializable
from interloper.serialization.runner import RunnerSpec

HandleT = TypeVar("HandleT")


class Runner(Serializable[RunnerSpec], Generic[HandleT]):
    """Abstract base class for all runners.

    Runners differ only in their orchestration strategy (sequential vs parallel).
    The actual execution logic is the same across all runners.
    """

    def __init__(
        self,
        fail_fast: bool = False,
        reraise: bool = True,
        on_event: Callable[[Event], None] | None = None,
    ):
        """Initialize the runner.

        Args:
            fail_fast: Whether to fail fast
            reraise: Whether to re-raise exceptions (takes precedence over fail_fast)
            on_event: Optional event handler. If provided, the runner can be used as a context manager
                to automatically subscribe/unsubscribe to events filtered by run_id.
        """
        self._fail_fast: bool = fail_fast
        self._reraise: bool = reraise
        self._state: RunState | None = None

        # Event handling
        self._on_event: Callable[[Event], None] | None = None
        self._subscribed_via_context_manager: bool = False

        if on_event is not None:

            def event_handler(event: Event) -> None:
                if self._state is not None and event.metadata.get("run_id") == self._state.run_id:
                    on_event(event)

            self._on_event = event_handler
            subscribe(event_handler)

    def __del__(self) -> None:
        """Clean up the runner."""
        if self._on_event is not None and not self._subscribed_via_context_manager:
            flush()
            unsubscribe(self._on_event)

    def __enter__(self) -> Self:
        """Enter the context manager.

        The subscription is already active from __init__ if on_event was provided.
        This method marks that cleanup should happen on exit.

        Returns:
            The runner instance
        """
        self._subscribed_via_context_manager = True
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        """Exit the context manager and unsubscribe from events.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self._on_event is not None and self._subscribed_via_context_manager:
            # Wait for any pending events to be processed before unsubscribing
            flush()
            unsubscribe(self._on_event)
            self._on_event = None
            self._subscribed_via_context_manager = False

    def _on_start(self) -> None:
        """Optional lifecycle hook before a run begins (e.g., create pools)."""
        pass

    def _on_end(self) -> None:
        """Optional lifecycle hook after a run ends (e.g., shutdown pools)."""
        pass

    @property
    def state(self) -> RunState:
        """Get the current state of the runner."""
        if self._state is None:
            raise RuntimeError("State not initialized")
        return self._state

    @property
    @abstractmethod
    def _capacity(self) -> int:
        """Maximum number of concurrent assets this runner can run."""
        raise NotImplementedError

    @abstractmethod
    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> HandleT:
        """Submit execution of an asset and return a handle for completion tracking.

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            A handle for the asset execution
        """
        raise NotImplementedError

    def _execute_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> Any:
        """Execute a single asset with full dependency resolution and state tracking.

        Delegates to Asset.materialize() which handles:
        - Context and config parameters
        - Upstream dependencies (loaded from IO via DAG)
        - Schema validation
        - Writing results to all configured IOs

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The output of the asset function
        """
        self.state.mark_asset_running(asset)

        try:
            # Adjust partition for non-partitioned assets
            if asset.partitioning is None:
                partition_or_window = None

            result = asset.materialize(
                partition_or_window=partition_or_window,
                dag=self.state.dag,
                metadata=self.state.metadata,
            )

            self.state.mark_asset_completed(asset)

            return result

        except Exception as e:
            self.state.mark_asset_failed(asset, str(e))

            # If reraise is True, always re-raise. Otherwise, re-raise only if fail_fast is True.
            if self._reraise or self._fail_fast:
                raise e

    @abstractmethod
    def _wait_any(self, handles: list[HandleT]) -> HandleT:
        """Block until any of the provided handles completes, and return it."""
        raise NotImplementedError

    @abstractmethod
    def _cancel_all(self, handles: list[HandleT]) -> None:
        """Best-effort cancellation of outstanding handles when failing fast."""
        raise NotImplementedError

    def run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Materialize the DAG using dynamic scheduling.

        Uses the scheduler to continuously find and execute ready assets
        until all assets are completed or failed.

        Args:
            dag: The DAG to execute
            partition_or_window: Either a Partition or PartitionWindow object
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            RunResult
        """
        self._state = RunState(dag, metadata=metadata)
        self.state.start_run(partition_or_window)

        try:
            self._on_start()

            # Dictionary to track the handles of the inflight assets
            inflight: dict[HandleT, Asset] = {}

            while not self.state.is_run_complete():
                # Fill capacity with any currently ready assets not yet submitted
                submitted_keys = {asset.key for asset in inflight.values()}
                ready_assets = self.state.ready_assets

                for asset in ready_assets:
                    if len(inflight) >= self._capacity:
                        break

                    if asset.key in submitted_keys:
                        continue

                    handle = self._submit_asset(asset, partition_or_window)
                    inflight[handle] = asset

                if not inflight:
                    # No work in-flight and not complete → invalid DAG or deadlock
                    raise RuntimeError(
                        "No assets ready but execution not complete. "
                        "This indicates a circular dependency or invalid DAG state."
                    )

                # Wait for one completion and then iterate to submit newly-ready work
                completed = self._wait_any(list(inflight.keys()))
                inflight.pop(completed)

            status = ExecutionStatus.FAILED if self.state.failed_assets else ExecutionStatus.COMPLETED
            asset_executions = self.state.end_run(status)

            return RunResult(
                partition_or_window=self.state.partition_or_window,
                status=status,
                asset_executions=asset_executions,
                execution_time=self.state.elapsed_time or 0,
            )

        except Exception as e:
            asset_executions = self.state.end_run(ExecutionStatus.FAILED, str(e))

            if self._reraise:
                raise e
            else:
                print(f"Exception in run {self.state.run_id}: {e}")
                print(traceback.format_exc())

            return RunResult(
                partition_or_window=self.state.partition_or_window,
                status=ExecutionStatus.FAILED,
                asset_executions=asset_executions,
                execution_time=self.state.elapsed_time or 0,
            )
        finally:
            try:
                self._on_end()
            except Exception:
                pass
