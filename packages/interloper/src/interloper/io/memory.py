"""Memory-based IO implementation."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.io import IOSpec


class MemoryIO(IO):
    """IO implementation for reading and writing to memory."""

    _storage: ClassVar[dict[str, Any]] = {}

    def write(self, context: IOContext, data: Any) -> None:
        """Write data to memory.

        Creates memory keys similar to FileIO paths:
        {dataset}/{asset_name} or {asset_name}
        If partitioned: {dataset}/{asset_name}/{partition_column}={partition_id}

        If partition_or_window is a PartitionWindow, writes data for each partition individually.

        Args:
            context: IO context with asset and partition information
            data: Data to write
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
        """Read data from memory.

        Reads from memory using the same key structure as write.

        If partition_or_window is a PartitionWindow, reads and returns data for each partition as a list.

        Args:
            context: IO context with asset and partition information

        Returns:
            The read data (or list of data if reading from a window)

        Raises:
            KeyError: If no data found for the given context
        """
        # No partitioning - read directly
        if context.partition_or_window is None:
            key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, None)
            if key not in self._storage:
                raise KeyError(f"No data found in memory for: {key}")
            return self._storage[key]

        # Partition window - read for each partition
        elif isinstance(context.partition_or_window, PartitionWindow):
            results = []
            for partition in context.partition_or_window:
                key = self._build_key(context.asset.name, context.asset.dataset, context.asset.partitioning, partition)
                if key not in self._storage:
                    raise KeyError(f"No data found in memory for: {key}")
                results.append(self._storage[key])
            return results

        # Single partition
        else:
            assert isinstance(context.partition_or_window, Partition)
            key = self._build_key(
                context.asset.name, context.asset.dataset, context.asset.partitioning, context.partition_or_window
            )
            if key not in self._storage:
                raise KeyError(f"No data found in memory for: {key}")
            return self._storage[key]

    def _build_key(
        self,
        name: str,
        dataset: str | None,
        partitioning: PartitionConfig | None,
        partition: Partition | None,
    ) -> str:
        """Build storage key from dataset, asset name, and partition.

        Args:
            name: The asset name
            dataset: The dataset name
            partitioning: The partitioning configuration
            partition: The partition or None

        Returns:
            String key for memory storage
        """
        parts = []
        if dataset:
            parts.append(dataset)
        parts.append(name)

        if partitioning is not None and partition is not None:
            parts.append(f"{partitioning.column}={partition.id}")

        return "/".join(parts)

    def to_spec(self) -> IOSpec:
        """Convert to serializable spec.

        Returns:
            IOSpec: The serializable spec (storage is global, not instance-specific)
        """
        return IOSpec(
            path=self.path,
            init={},
        )

    @classmethod
    def clear(cls) -> None:
        """Clear all stored data (useful for testing)."""
        cls._storage.clear()
