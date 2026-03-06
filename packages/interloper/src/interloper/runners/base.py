"""Abstract base runner and shared execution logic."""

from __future__ import annotations

import traceback
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from typing_extensions import Self

from interloper.assets.base import Asset
from interloper.dag.base import DAG
from interloper.errors import PartitionError, RunnerError
from interloper.events.base import Event, flush, subscribe, unsubscribe
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.runners.state import RunState
from interloper.serialization.base import Serializable
from interloper.serialization.runner import RunnerInstanceSpec

HandleT = TypeVar("HandleT")


class Runner(Serializable[RunnerInstanceSpec], Generic[HandleT]):
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
            fail_fast: Stop execution after the first asset failure.
            reraise: Re-raise exceptions to the caller (takes precedence over fail_fast).
            on_event: Event callback, filtered by run_id. When provided, the runner
                can be used as a context manager for automatic subscribe/unsubscribe.
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
        """Flush pending events and unsubscribe if not using context manager."""
        if self._on_event is not None and not self._subscribed_via_context_manager:
            flush()
            unsubscribe(self._on_event)

    def __enter__(self) -> Self:
        """Mark that event cleanup should happen on __exit__ rather than __del__.

        Returns:
            This runner instance for use in a ``with`` block.
        """
        self._subscribed_via_context_manager = True
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        """Flush pending events and unsubscribe from the event bus."""
        if self._on_event is not None and self._subscribed_via_context_manager:
            # Wait for any pending events to be processed before unsubscribing
            flush()
            unsubscribe(self._on_event)
            self._on_event = None
            self._subscribed_via_context_manager = False

    def _on_start(self) -> None:
        """Lifecycle hook called before a run begins (e.g. create pools)."""

    def _on_end(self) -> None:
        """Lifecycle hook called after a run ends (e.g. shutdown pools)."""

    @property
    def state(self) -> RunState:
        """The current run state.

        Raises:
            RunnerError: If state has not been initialized via ``run()``.
        """
        if self._state is None:
            raise RunnerError("State not initialized")
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
        """Submit an asset for execution and return a handle for completion tracking."""
        raise NotImplementedError

    def _execute_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> Any:
        """Execute a single asset with state tracking.

        Delegates to ``Asset.materialize()`` for dependency resolution, schema
        validation, and IO writes. Updates run state on success or failure.

        Returns:
            The materialization result, or None if the asset failed and reraise is False.
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
        except Exception as e:
            self.state.mark_asset_failed(asset, str(e), tb=traceback.format_exc())

            # If reraise is True, always re-raise. Otherwise, re-raise only if fail_fast is True.
            if self._reraise or self._fail_fast:
                raise
            return None
        return result

    @abstractmethod
    def _wait_any(self, handles: list[HandleT]) -> HandleT:
        """Block until any of the provided handles completes, and return it."""
        raise NotImplementedError

    @abstractmethod
    def _cancel_all(self, handles: list[HandleT]) -> None:
        """Best-effort cancellation of outstanding handles when failing fast."""
        raise NotImplementedError

    def _preflight_validation(self, dag: DAG, partition_or_window: Partition | PartitionWindow | None) -> None:
        """Run preflight validations before execution begins."""
        self._validate_partition_window_support(dag, partition_or_window)

    def _validate_partition_window_support(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Validate that all partitioned assets support windowed execution.

        Raises:
            PartitionError: If any partitioned asset has ``allow_window=False``.
        """
        if not isinstance(partition_or_window, PartitionWindow):
            return

        unsupported_assets = [
            asset.instance_key
            for asset in dag.assets
            if asset.materializable and asset.partitioning is not None and not asset.partitioning.allow_window
        ]
        if unsupported_assets:
            raise PartitionError(
                "Windowed runs require all partitioned assets to set allow_window=True. "
                "Unsupported assets: "
                f"{sorted(unsupported_assets)}. "
                "Use a partition window with backfill(windowed=False) to run one partition per run."
            )

    def run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """Materialize the DAG by dynamically scheduling ready assets until completion.

        Args:
            dag: The DAG to execute.
            partition_or_window: Partition or window to scope the run.
            metadata: Arbitrary metadata (e.g. run_id, backfill_id).

        Returns:
            A RunResult summarizing the execution outcome.

        Raises:
            RunnerError: If a deadlock or invalid DAG state is detected.
        """
        self._preflight_validation(dag, partition_or_window)
        self._state = RunState(dag, metadata=metadata)
        self.state.start_run(partition_or_window)

        try:
            self._on_start()

            # Dictionary to track the handles of the inflight assets
            inflight: dict[HandleT, Asset] = {}

            while not self.state.is_run_complete():
                # Fill capacity with any currently ready assets not yet submitted
                submitted_keys = {asset.instance_key for asset in inflight.values()}
                ready_assets = self.state.ready_assets

                for asset in ready_assets:
                    if len(inflight) >= self._capacity:
                        break

                    if asset.instance_key in submitted_keys:
                        continue

                    handle = self._submit_asset(asset, partition_or_window)
                    inflight[handle] = asset

                if not inflight:
                    # No work in-flight and not complete → invalid DAG or deadlock
                    raise RunnerError(
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
                raise
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
            except Exception as e:  # noqa: BLE001
                print(f"Error in runner shutdown hook: {e}")
