"""Serial (single-partition-at-a-time) backfiller implementation."""

from interloper.backfillers.base import Backfiller
from interloper.dag.base import DAG
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.serialization.backfiller import BackfillerSpec


class SerialBackfiller(Backfiller[Partition | PartitionWindow | None]):
    """Backfiller that executes partitions one at a time in sequence.

    Each partition run blocks until completion before the next one starts.
    Suitable for simple workloads or when parallelism is not desired.
    """

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
        """Convert to a serializable spec."""
        return BackfillerSpec(path=self.path)
