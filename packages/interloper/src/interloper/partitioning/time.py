"""Time-based partitioning configuration."""

from __future__ import annotations

import datetime as dt
from collections.abc import Generator
from dataclasses import dataclass

from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow


def date_range(start_date: dt.date, end_date: dt.date, reversed: bool = False) -> Generator[dt.date, None, None]:
    """Generate a range of dates.

    Args:
        start_date: The start date of the range.
        end_date: The end date of the range.
        reversed: Whether to reverse the range.

    Yields:
        The dates in the range.
    """
    if reversed:
        while end_date >= start_date:
            yield end_date
            end_date -= dt.timedelta(days=1)
    else:
        while start_date <= end_date:
            yield start_date
            start_date += dt.timedelta(days=1)


@dataclass(frozen=True)
class TimePartitionConfig(PartitionConfig):
    """The configuration for a time partition.

    Attributes:
        start_date: The start date of the partition.
    """

    start_date: dt.date | None = None


@dataclass(frozen=True)
class TimePartition(Partition):
    """A time-based partition of an asset.

    Attributes:
        value: The date of the partition.
    """

    value: dt.date

    def __repr__(self) -> str:
        """Return a string representation of the partition."""
        return self.value.isoformat()

    @property
    def id(self) -> str:
        """The unique identifier of the partition."""
        return self.value.isoformat()


@dataclass(frozen=True)
class TimePartitionWindow(PartitionWindow):
    """A window of time-based partitions.

    Attributes:
        start: The start date of the window.
        end: The end date of the window.
    """

    start: dt.date
    end: dt.date

    def __iter__(self) -> Generator[TimePartition, None, None]:
        """Iterate over the partitions in the window.

        Yields:
            The partitions in the window.
        """
        yield from self.iter_partitions()

    def __str__(self) -> str:
        """Return a string representation of the partition window."""
        return f"{self.start.isoformat()}:{self.end.isoformat()}"

    def __repr__(self) -> str:
        """Return a string representation of the partition window."""
        return f"{self.start.isoformat()} to {self.end.isoformat()}"

    def iter_partitions(self) -> Generator[TimePartition, None, None]:
        """Iterate over the partitions in the window.

        Yields:
            The partitions in the window.
        """
        for date in date_range(self.start, self.end, reversed=True):
            yield TimePartition(date)

    def partition_count(self) -> int:
        """The number of partitions in the window.

        Returns:
            The number of partitions in the window.
        """
        return (self.end - self.start).days + 1
