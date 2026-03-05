"""Multi-process runner."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from typing import Any

from interloper.assets.base import Asset
from interloper.errors import RunnerError
from interloper.events.base import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.asset import AssetSpec
from interloper.serialization.dag import DAGSpec
from interloper.serialization.runner import RunnerSpec


class MultiProcessRunner(Runner[Future[Any]]):
    """Process-based parallel runner.

    Executes independent assets in parallel using processes.
    Uses serialization layer to avoid pickling complex objects.
    Best for CPU-bound workloads and true parallelism.
    """

    def __init__(
        self,
        max_workers: int = 4,
        fail_fast: bool = True,
        reraise: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ):
        """Initialize the multi-process runner.

        Args:
            max_workers: Maximum number of worker processes.
            fail_fast: Stop execution after the first asset failure.
            reraise: Re-raise exceptions to the caller (takes precedence over fail_fast).
            on_event: Event callback, filtered by run_id.
        """
        super().__init__(fail_fast=fail_fast, reraise=reraise, on_event=on_event)
        self._max_workers = max_workers
        self._pool = None

    @property
    def _capacity(self) -> int:
        return self._max_workers

    def _on_start(self) -> None:
        self._pool = ProcessPoolExecutor(max_workers=self._max_workers)

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

        future = self._pool.submit(
            execute_in_process,
            asset.to_spec(),
            self.state.dag.to_spec(),
            partition_or_window,
        )

        # Attach callback to update state
        def done_callback(future: Future) -> None:
            try:
                asset_key, success, error_msg = future.result()
                if success:
                    self.state.mark_asset_completed(asset)
                else:
                    self.state.mark_asset_failed(asset, error_msg or "Unknown error")
                    if error_msg:
                        print(f"Asset {asset_key} failed: {error_msg}")
            except Exception as e:  # noqa: BLE001
                print(f"Asset {asset.instance_key} failed with exception: {e}")
                self.state.mark_asset_failed(asset, str(e))

        future.add_done_callback(done_callback)
        return future

    def _wait_any(self, handles: list[Future]) -> Future:
        done, _ = wait(handles, return_when=FIRST_COMPLETED)
        future = next(iter(done))
        try:
            asset_key, success, error_msg = future.result()
            if not success and (self._fail_fast or self._reraise):
                if self._fail_fast:
                    self._cancel_all([h for h in handles if h is not future])
                raise RunnerError(f"Asset {asset_key} failed: {error_msg}")
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
        """Serialize to a RunnerSpec.

        Returns:
            A RunnerSpec for this multi-process runner.
        """
        return RunnerSpec(
            path=self.path,
            init={
                "max_workers": self._max_workers,
                "fail_fast": self._fail_fast,
                "reraise": self._reraise,
            },
        )


def execute_in_process(
    asset_spec: AssetSpec,
    dag_spec: DAGSpec,
    partition_or_window: Partition | PartitionWindow | None,
) -> tuple[str, bool, str | None]:
    """Execute a single asset in a worker process.

    Reconstructs the asset and DAG from their serialized specs to avoid
    pickling complex objects across process boundaries.

    Returns:
        Tuple of (asset_key, success, error_message_or_none).
    """
    # Reconstruct objects from specs
    asset = asset_spec.reconstruct()
    dag = dag_spec.reconstruct()

    try:
        # Adjust partition for non-partitioned assets
        if asset.partitioning is None:
            partition_or_window = None

        # Execute the asset
        asset.materialize(
            partition_or_window=partition_or_window,
            dag=dag,
        )
    except Exception as e:  # noqa: BLE001
        return (asset.instance_key, False, str(e))
    return (asset.instance_key, True, None)
