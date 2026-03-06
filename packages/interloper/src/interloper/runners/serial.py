"""In-process sequential runner."""

from __future__ import annotations

from interloper.assets.base import Asset
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.runner import RunnerInstanceSpec


class SerialRunner(Runner[str]):
    """Sequential execution in the current process.

    Executes assets one at a time as they become ready.
    Best for debugging and deterministic execution.
    """

    @property
    def _capacity(self) -> int:
        return 1

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> str:
        self._execute_asset(asset, partition_or_window)
        return asset.instance_key

    def _wait_any(self, handles: list[str]) -> str:
        return handles[0]

    def _cancel_all(self, handles: list[str]) -> None:
        raise NotImplementedError("Not supported for serial runner")

    def to_spec(self) -> RunnerInstanceSpec:
        """Serialize to a RunnerSpec.

        Returns:
            A RunnerSpec for this serial runner.
        """
        return RunnerInstanceSpec(path=self.path)
