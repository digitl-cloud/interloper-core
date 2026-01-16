"""Multi-process runner."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait

from interloper.assets.base import Asset
from interloper.events.base import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.asset import AssetSpec
from interloper.serialization.dag import DAGSpec
from interloper.serialization.runner import RunnerSpec


class MultiProcessRunner(Runner):
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
            max_workers: Maximum number of worker processes (default 4)
            fail_fast: Whether to fail fast
            reraise: Whether to re-raise exceptions (takes precedence over fail_fast)
            on_event: Optional event handler. If provided, the runner can be used as a context manager
                to automatically subscribe/unsubscribe to events filtered by run_id.
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
            raise RuntimeError("Pool not initialized")

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
            except Exception as e:
                print(f"Asset {asset.key} failed with exception: {e}")
                self.state.mark_asset_failed(asset, str(e))

        future.add_done_callback(done_callback)
        return future

    def _wait_any(self, handles: list[Future]) -> Future:
        done, _ = wait(handles, return_when=FIRST_COMPLETED)
        future = next(iter(done))
        try:
            asset_key, success, error_msg = future.result()
            if not success and self._fail_fast:
                self._cancel_all([h for h in handles if h is not future])
                raise RuntimeError(f"Asset {asset_key} failed: {error_msg}")
        except Exception:
            if self._fail_fast:
                self._cancel_all([h for h in handles if h is not future])
            raise
        return future

    def _cancel_all(self, handles: list[Future]) -> None:
        for h in handles:
            try:
                h.cancel()
            except Exception:
                pass

    def to_spec(self) -> RunnerSpec:
        """Convert to MultiProcessRunnerSpec spec."""
        return RunnerSpec(
            path=self.path,
            init=dict(
                max_workers=self._max_workers,
                fail_fast=self._fail_fast,
                reraise=self._reraise,
            ),
        )


def execute_in_process(
    asset_spec: AssetSpec,
    dag_spec: DAGSpec,
    partition_or_window: Partition | PartitionWindow | None,
) -> tuple[str, bool, str | None]:
    """Execute a single asset in a worker process.

    Args:
        asset_spec: Serialized asset specification
        dag_spec: Serialized DAG specification
        partition_or_window: Either a Partition or PartitionWindow object (must be picklable)

    Returns:
        Tuple of (asset_name, success_bool, error_message)
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
        return (asset.key, True, None)
    except Exception as e:
        return (asset.key, False, str(e))
