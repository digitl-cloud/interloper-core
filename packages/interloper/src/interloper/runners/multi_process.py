"""Multi-process runner."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from typing import Any

from pydantic import PrivateAttr

from interloper.assets.base import Asset
from interloper.errors import RunnerError
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.asset import AssetInstanceSpec
from interloper.serialization.dag import DAGInstanceSpec


class MultiProcessRunner(Runner[Future[Any]]):
    """Process-based parallel runner.

    Executes independent assets in parallel using processes.
    Uses serialization layer to avoid pickling complex objects.
    Best for CPU-bound workloads and true parallelism.
    """

    max_workers: int = 4
    fail_fast: bool = True
    reraise: bool = False

    _pool: ProcessPoolExecutor | None = PrivateAttr(default=None)

    @property
    def _capacity(self) -> int:
        return self.max_workers

    def _on_start(self) -> None:
        self._pool = ProcessPoolExecutor(max_workers=self.max_workers)

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
                print(f"Asset {asset.key} failed with exception: {e}")
                self.state.mark_asset_failed(asset, str(e))

        future.add_done_callback(done_callback)
        return future

    def _wait_any(self, handles: list[Future]) -> Future:
        done, _ = wait(handles, return_when=FIRST_COMPLETED)
        future = next(iter(done))
        try:
            asset_key, success, error_msg = future.result()
            if not success and (self.fail_fast or self.reraise):
                if self.fail_fast:
                    self._cancel_all([h for h in handles if h is not future])
                raise RunnerError(f"Asset {asset_key} failed: {error_msg}")
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


def execute_in_process(
    asset_spec: AssetInstanceSpec,
    dag_spec: DAGInstanceSpec,
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
        return (asset.key, False, str(e))
    return (asset.key, True, None)
