"""Multi-threaded runner."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any

from interloper.assets.base import Asset
from interloper.events.base import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.runner import RunnerSpec


class MultiThreadRunner(Runner[Future[Any]]):
    """Thread-based parallel runner.

    Executes independent assets in parallel using threads.
    Assets at the same dependency level can run concurrently.
    Default runner for dag.materialize().
    """

    def __init__(
        self,
        max_workers: int = 4,
        fail_fast: bool = True,
        reraise: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ):
        """Initialize the multi-thread runner.

        Args:
            max_workers: Maximum number of worker threads (default 4)
            fail_fast: Whether to fail fast
            reraise: Whether to re-raise exceptions (takes precedence over fail_fast)
            on_event: Optional event handler. If provided, the runner can be used as a context manager
                to automatically subscribe/unsubscribe to events filtered by run_id.
        """
        super().__init__(fail_fast, reraise, on_event)
        self._max_workers = max_workers
        self._pool = None

    @property
    def _capacity(self) -> int:
        return self._max_workers

    def _on_start(self) -> None:
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def _on_end(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Future:
        if self._pool is None:
            raise RuntimeError("Pool not initialized")

        return self._pool.submit(self._execute_asset, asset, partition_or_window)

    def _wait_any(self, handles: list[Future]) -> Future:
        done, _ = wait(handles, return_when=FIRST_COMPLETED)
        future = next(iter(done))
        # Only fail the run loop immediately when configured to fail fast or re-raise.
        # Otherwise, let the scheduler continue so independent branches can complete.
        try:
            future.result()
        except Exception:
            if self._fail_fast:
                self._cancel_all([h for h in handles if h is not future])
            if self._fail_fast or self._reraise:
                raise
        return future

    def _cancel_all(self, handles: list[Future]) -> None:
        for h in handles:
            try:
                h.cancel()
            except Exception:  # noqa: BLE001, S110
                pass

    def to_spec(self) -> RunnerSpec:
        """Convert to MultiThreadRunnerSpec spec."""
        return RunnerSpec(
            path=self.path,
            init={
                "max_workers": self._max_workers,
                "fail_fast": self._fail_fast,
                "reraise": self._reraise,
            },
        )
