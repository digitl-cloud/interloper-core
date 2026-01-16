"""IO context for IO operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.assets.base import Asset


@dataclass(frozen=True)
class IOContext:
    """Context information for IO operations.

    Attributes:
        asset: Asset being materialized
        partition_or_window: Either a Partition or PartitionWindow object
    """

    asset: Asset
    partition_or_window: Partition | PartitionWindow | None = None

