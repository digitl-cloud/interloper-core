"""Runtime context passed to every destination read/write call."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from interloper.assets.context import EventLogger
from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.assets.base import Asset


class DestinationContext:
    """Context passed to :meth:`Destination.read` and :meth:`Destination.write`.

    Carries the target asset, optional partition scope, arbitrary metadata
    (e.g. ``run_id``, ``backfill_id``), and a logger so that destination
    implementations can resolve the correct storage location and emit log
    events without additional parameters.
    """

    def __init__(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Initialize the context.

        Args:
            asset: Asset being materialized.
            partition_or_window: Partition scope, or ``None`` for unpartitioned assets.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
        """
        self._asset = asset
        self._partition_or_window = partition_or_window
        self._metadata = metadata or {}
        self._logger: EventLogger | None = None

    @property
    def asset(self) -> Asset:
        """The asset being materialized."""
        return self._asset

    @property
    def partition_or_window(self) -> Partition | PartitionWindow | None:
        """Partition scope, or ``None`` for unpartitioned assets."""
        return self._partition_or_window

    @property
    def metadata(self) -> dict[str, Any]:
        """Arbitrary metadata dict (e.g. run_id, backfill_id)."""
        return self._metadata

    @property
    def logger(self) -> EventLogger:
        """Logger that emits messages as events on the event bus."""
        if self._logger is None:
            self._logger = EventLogger(self._asset.qualified_key, self._metadata)
        return self._logger
