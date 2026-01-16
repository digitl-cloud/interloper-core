"""Partitioning configurations."""

from interloper.partitioning.base import (
    Partition,
    PartitionConfig,
    PartitionWindow,
)
from interloper.partitioning.time import (
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)

__all__ = [
    "Partition",
    "PartitionConfig",
    "PartitionWindow",
    "TimePartition",
    "TimePartitionConfig",
    "TimePartitionWindow",
]
