"""Base partitioning configuration."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionConfig:
    """The configuration for a partition.

    Attributes:
        column: The name of the partitioning column.
        allow_window: Whether to allow windowed partitions.
    """

    column: str
    allow_window: bool = False


@dataclass(frozen=True)
class Partition(ABC):
    """A partition of an asset.

    Attributes:
        value: The value of the partition.
    """

    value: Any

    @property
    def id(self) -> str:
        """The unique identifier of the partition."""
        return str(self.value)


@dataclass(frozen=True)
class PartitionWindow(ABC):
    """A window of partitions.

    Attributes:
        start: The start of the window.
        end: The end of the window.
    """

    start: Any
    end: Any

    @property
    def id(self) -> str:
        """The unique identifier of the window."""
        return f"{self.start}-{self.end}"

    @abstractmethod
    def __iter__(self) -> Generator[Partition, None, None]:
        """Iterate over the partitions in the window."""
        pass
