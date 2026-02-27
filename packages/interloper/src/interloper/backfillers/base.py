"""Backfiller abstractions for orchestrating full runs above runners.

Backfillers define where/how a run happens (in-process, Docker, Kubernetes),
while delegating per-asset scheduling and execution to a ``Runner``.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from typing_extensions import Self

from interloper.backfillers.results import BackfillResult
from interloper.backfillers.state import BackfillState
from interloper.dag.base import DAG
from interloper.events.base import Event, flush, subscribe, unsubscribe
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.runners.multi_thread import MultiThreadRunner
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.serialization.backfiller import BackfillerSpec
from interloper.serialization.base import Serializable

HandleT = TypeVar("HandleT")


class Backfiller(Serializable[BackfillerSpec], Generic[HandleT]):
    """Abstract base class for all backfillers.

    A backfiller is responsible for orchestrating the entire run (process/host/container),
    while a `Runner` handles the asset-level scheduling and concurrency model.
    """

    def __init__(
        self,
        runner: Runner | None = None,
        fail_fast: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        """Initialize the backfiller.

        Args:
            runner: The runner to use for each partition run.
            fail_fast: If True, stop the backfill on the first partition failure.
                If False (default), continue executing remaining partitions.
            on_event: Optional event handler filtered by backfill_id.
        """
        self.runner = runner or MultiThreadRunner()
        self._fail_fast = fail_fast
        self._state: BackfillState | None = None

        # Event handling
        self._on_event: Callable[[Event], None] | None = None
        self._subscribed_via_context_manager: bool = False

        if on_event is not None:

            def event_handler(event: Event) -> None:
                if self._state is not None and event.metadata.get("backfill_id") == self._state.backfill_id:
                    on_event(event)

            self._on_event = event_handler
            subscribe(event_handler)

    def __del__(self) -> None:
        """Clean up the backfiller."""
        if self._on_event is not None and not self._subscribed_via_context_manager:
            flush()
            unsubscribe(self._on_event)

    def __enter__(self) -> Self:
        """Enter the context manager.

        The subscription is already active from __init__ if on_event was provided.
        This method marks that cleanup should happen on exit.

        Returns:
            The backfiller instance
        """
        self._subscribed_via_context_manager = True
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None:
        """Exit the context manager and unsubscribe from events.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self._on_event is not None and self._subscribed_via_context_manager:
            flush()
            unsubscribe(self._on_event)
            self._on_event = None
            self._subscribed_via_context_manager = False

    def _on_start(self) -> None:
        """Optional lifecycle hook before a run begins (e.g., create pools)."""

    def _on_end(self) -> None:
        """Optional lifecycle hook after a run ends (e.g., shutdown pools)."""

    @property
    def state(self) -> BackfillState:
        """Get the current state of the backfiller."""
        if self._state is None:
            raise RuntimeError("State not initialized")
        return self._state

    @property
    @abstractmethod
    def _capacity(self) -> int:
        """Maximum number of concurrent runs this backfiller can execute."""
        raise NotImplementedError

    @abstractmethod
    def _submit_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> HandleT:
        """Submit execution of a run and return a handle for completion tracking.

        Args:
            dag: The DAG to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            A handle for the run execution (implementation-specific)
        """
        raise NotImplementedError

    def _execute_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> RunResult:
        """Execute a single partition run.

        When ``fail_fast`` is False, exceptions are caught and the run is
        marked as failed so the backfill loop can continue to the next
        partition.  When ``fail_fast`` is True, the exception propagates
        and terminates the backfill.
        """
        try:
            self.state.mark_run_running(partition_or_window)
            result = self.runner.run(dag, partition_or_window, metadata={"backfill_id": self.state.backfill_id})
            self.state.mark_run_completed(partition_or_window, result)
        except Exception as e:
            self.state.mark_run_failed(partition_or_window, str(e))
            if self._fail_fast:
                raise
            return RunResult(
                partition_or_window=partition_or_window,
                status=ExecutionStatus.FAILED,
                execution_time=0,
            )
        return result

    @abstractmethod
    def _wait_any(self, handles: list[HandleT]) -> HandleT:
        """Block until any of the provided handles completes, and return it."""
        raise NotImplementedError

    @abstractmethod
    def _cancel_all(self, handles: list[HandleT]) -> None:
        """Best-effort cancellation of outstanding handles when failing fast."""
        raise NotImplementedError

    def backfill(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        windowed: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> BackfillResult:
        """Execute a backfill."""
        if windowed and not isinstance(partition_or_window, PartitionWindow):
            raise ValueError("Windowed mode is only supported for windowed partitioning")

        inflight: dict[HandleT, Partition | PartitionWindow | None] = {}
        queued: list[Partition | PartitionWindow | None] = []

        # List all partitions to execute
        if partition_or_window is None or isinstance(partition_or_window, Partition) or windowed:
            queued.append(partition_or_window)
        else:
            assert isinstance(partition_or_window, PartitionWindow)
            queued.extend(partition_or_window)

        self._state = BackfillState(partitions=queued.copy(), metadata=metadata)
        self.state.start_backfill()

        try:
            self._on_start()

            while not self.state.is_backfill_complete():
                available_capacity = self._capacity - len(inflight)

                # Submit runs up to capacity
                for _ in range(min(available_capacity, len(queued))):
                    partition = queued.pop(0)
                    handle = self._submit_run(dag, partition)
                    inflight[handle] = partition

                if not inflight:
                    raise RuntimeError("Unexpected backfill state: no runs ready but execution not complete")

                # Wait for any handle to complete
                completed_handle = self._wait_any(list(inflight.keys()))
                partition = inflight.pop(completed_handle)

            status = ExecutionStatus.FAILED if self.state.failed_runs else ExecutionStatus.COMPLETED
            run_executions = self.state.end_backfill(status)

            return BackfillResult(
                status=status,
                run_executions=run_executions,
                execution_time=self.state.elapsed_time or 0,
            )

        except (RuntimeError, ValueError, TypeError) as e:
            import traceback

            error_msg = str(e) if e else f"{type(e).__name__} with no message"
            print(f"Exception in backfill: {error_msg}")
            traceback.print_exc()
            run_executions = self.state.end_backfill(ExecutionStatus.FAILED, error_msg)

            return BackfillResult(
                status=ExecutionStatus.FAILED,
                run_executions=run_executions,
                execution_time=self.state.elapsed_time or 0,
            )
        finally:
            try:
                self._on_end()
            except Exception as e:  # noqa: BLE001
                print(f"Error in backfiller shutdown hook: {e}")
