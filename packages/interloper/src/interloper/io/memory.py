"""In-memory IO backed by a class-level dict, mainly useful for testing."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.errors import DataNotFoundError
from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.io import IOSpec


class MemoryIO(IO):
    """IO that stores data in a class-level dict keyed by ``{dataset}/{name}/{partition}``.

    All instances share a single ``_storage`` dict so data written by one
    asset is visible to others.  Call :meth:`clear` between test runs.
    """

    _storage: ClassVar[dict[str, Any]] = {}

    def write(self, context: IOContext, data: Any) -> None:
        """Store data in memory under a path-style key.

        Args:
            context: IO context with asset and partition information.
            data: Data to store.
        """
        # No partitioning - write directly
        if context.partition_or_window is None:
            key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, None)
            self._storage[key] = data

        # Partition window - write for each partition
        elif isinstance(context.partition_or_window, PartitionWindow):
            for partition in context.partition_or_window:
                key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, partition)
                self._storage[key] = data

        # Single partition
        else:
            assert isinstance(context.partition_or_window, Partition)
            key = self._build_key(
                context.asset.name, context.asset.dataset, context.asset.partitioning, context.partition_or_window
            )
            self._storage[key] = data

    def read(self, context: IOContext) -> Any:
        """Retrieve data from memory.

        Args:
            context: IO context with asset and partition information.

        Returns:
            The stored data, or a list of results for partition windows.

        Raises:
            DataNotFoundError: If no data exists for the resolved key.
        """
        # No partitioning - read directly
        if context.partition_or_window is None:
            key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, None)
            if key not in self._storage:
                raise DataNotFoundError(f"No data found in memory for: {key}")
            return self._storage[key]

        # Partition window - read for each partition
        elif isinstance(context.partition_or_window, PartitionWindow):
            results = []
            for partition in context.partition_or_window:
                key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, partition)
                if key not in self._storage:
                    raise DataNotFoundError(f"No data found in memory for: {key}")
                results.append(self._storage[key])
            return results

        # Single partition
        else:
            assert isinstance(context.partition_or_window, Partition)
            key = self._build_key(
                context.asset.name, context.asset.dataset, context.asset.partitioning, context.partition_or_window
            )
            if key not in self._storage:
                raise DataNotFoundError(f"No data found in memory for: {key}")
            return self._storage[key]

    def _build_key(
        self,
        name: str,
        dataset: str | None,
        partitioning: PartitionConfig | None,
        partition: Partition | None,
    ) -> str:
        """Build a ``/``-joined storage key from the asset identity and partition.

        Returns:
            The constructed storage key string.
        """
        parts = []
        if dataset:
            parts.append(dataset)
        parts.append(name)

        if partitioning is not None and partition is not None:
            parts.append(f"{partitioning.column}={partition.id}")

        return "/".join(parts)

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by partition from in-memory storage.

        Scans ``_storage`` keys matching the asset and partition column prefix,
        counts items in each stored value (``len(data)`` for lists, ``1``
        otherwise).

        Args:
            context: IO context with asset and partition information.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        column = context.asset.partitioning.column
        prefix = self._build_key(context.asset.name, context.asset.dataset, None, None)
        partition_prefix = f"{prefix}/{column}="

        counts: dict[str, int] = {}
        for key, data in self._storage.items():
            if key.startswith(partition_prefix):
                partition_value = key[len(partition_prefix):]
                counts[partition_value] = len(data) if isinstance(data, list) else 1
        return counts

    def to_spec(self) -> IOSpec:
        """Convert to a serializable spec.

        Returns:
            The IOSpec representation of this MemoryIO.
        """
        return IOSpec(
            path=self.path,
            init={},
        )

    @classmethod
    def clear(cls) -> None:
        """Clear all stored data (useful for testing)."""
        cls._storage.clear()
