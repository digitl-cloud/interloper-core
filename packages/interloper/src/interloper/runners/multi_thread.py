"""Multi-threaded runner."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any

from pydantic import PrivateAttr

from interloper.assets.base import Asset
from interloper.errors import RunnerError
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner


class MultiThreadRunner(Runner[Future[Any]]):
    """Thread-based parallel runner.

    Executes independent assets in parallel using threads.
    Assets at the same dependency level can run concurrently.
    Default runner for dag.materialize().
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _pool: ThreadPoolExecutor | None = PrivateAttr(default=None)

    @property
    def _capacity(self) -> int:
        return self.max_workers

    def _on_start(self) -> None:
        self._pool = ThreadPoolExecutor(max_workers=self.max_workers)

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
            raise RunnerError("Pool not initialized")

        return self._pool.submit(self._execute_asset, asset, partition_or_window)

    def _wait_any(self, handles: list[Future]) -> Future:
        done, _ = wait(handles, return_when=FIRST_COMPLETED)
        future = next(iter(done))
        # Only fail the run loop immediately when configured to fail fast or re-raise.
        # Otherwise, let the scheduler continue so independent branches can complete.
        try:
            future.result()
        except Exception:
            if self.fail_fast:
                self._cancel_all([h for h in handles if h is not future])
            if self.fail_fast or self.reraise:
                raise
        return future

    def _cancel_all(self, handles: list[Future]) -> None:
        for h in handles:
            try:
                h.cancel()
            except Exception:  # noqa: BLE001, S110
                pass
