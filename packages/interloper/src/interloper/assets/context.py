"""Runtime context for asset execution."""

import datetime as dt
from typing import Any

from interloper.assets.keys import AssetInstanceKey
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.partitioning.time import TimePartitionConfig


class ExecutionContext:
    """Runtime context providing access to partitions and metadata."""

    def __init__(
        self,
        asset_key: AssetInstanceKey,
        partitioning: PartitionConfig | None = None,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize the context.

        Args:
            asset_key: The name of the asset being executed
            partitioning: The partitioning configuration
            partition_or_window: Either a Partition or PartitionWindow object
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id)
        """
        self._asset_key = asset_key
        self._partitioning = partitioning
        self._partition_or_window = partition_or_window
        self._metadata = metadata or {}

    @property
    def partition_date(self) -> dt.date:
        """The partition value as a datetime.date object.

        Only available for time-based partitioning.
        Raises an error if asset is not time-partitioned or no partition provided.
        """
        if self._partitioning is None:
            raise AttributeError("`context.partition_date` is not available, asset is not partitioned.")

        if self._partition_or_window is None:
            raise AttributeError("`context.partition_date` is not available, no partition provided.")

        if not isinstance(self._partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004 (AttributeError is desired here)
                "`context.partition_date` is not available, asset is not time-partitioned. "
                "Use `TimePartitionConfig` in the asset decorator."
            )

        if isinstance(self._partition_or_window, PartitionWindow):
            raise AttributeError(  # noqa: TRY004 (AttributeError is desired here)
                "`context.partition_date` is not available. "
                "Context currently holds a partition window, not a partition."
            )

        return self._partition_or_window.value

    @property
    def partition_date_window(self) -> tuple[dt.date, dt.date]:
        """A tuple of (start_date, end_date) representing a date range.

        Only available for TimePartitionConfig with allow_window=True.
        Raises an error if allow_window=False or asset is not time-partitioned.
        """
        if self._partitioning is None:
            raise AttributeError("`context.partition_date_window` is not available, asset is not partitioned.")

        if self._partition_or_window is None:
            raise AttributeError("`context.partition_date_window` is not available, no partition provided.")

        if not isinstance(self._partitioning, TimePartitionConfig):
            raise AttributeError(  # noqa: TRY004 (AttributeError is desired here)
                "`context.partition_date_window` is not available, asset is not time-partitioned. "
                "Use `TimePartitionConfig` in the asset decorator."
            )

        if not self._partitioning.allow_window:
            raise AttributeError(
                "`context.partition_date_window` is not available, asset does not allow windows. "
                "Set `allow_window=True` in `TimePartitionConfig` to enable windowed partitions."
            )

        if isinstance(self._partition_or_window, Partition):
            return (self._partition_or_window.value, self._partition_or_window.value)

        assert isinstance(self._partition_or_window, PartitionWindow)
        return (self._partition_or_window.start, self._partition_or_window.end)

    @property
    def asset_key(self) -> AssetInstanceKey:
        """The name of the current asset being executed."""
        return self._asset_key

    @property
    def metadata(self) -> dict[str, Any]:
        """Arbitrary metadata dict (e.g. run_id, backfill_id)."""
        return self._metadata
