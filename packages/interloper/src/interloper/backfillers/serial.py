"""Serial backfiller."""

from interloper.backfillers.base import Backfiller
from interloper.dag.base import DAG
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.serialization.backfiller import BackfillerSpec


class SerialBackfiller(Backfiller[Partition | PartitionWindow | None]):
    """Serial backfiller."""

    @property
    def _capacity(self) -> int:
        return 1

    def _submit_run(
        self, dag: DAG, partition_or_window: Partition | PartitionWindow | None
    ) -> Partition | PartitionWindow | None:
        self._execute_run(dag, partition_or_window)
        return partition_or_window

    def _wait_any(self, handles: list[Partition | PartitionWindow | None]) -> Partition | PartitionWindow | None:
        return handles[0]

    def _cancel_all(self, handles: list[Partition | PartitionWindow | None]) -> None:
        raise NotImplementedError("Not supported for serial backfiller")

    def to_spec(self) -> BackfillerSpec:
        """Convert to SerialBackfillerSpec spec."""
        return BackfillerSpec(path=self.path)
