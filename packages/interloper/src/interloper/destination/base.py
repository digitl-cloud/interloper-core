"""Abstract base class defining the destination read/write interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from interloper.destination.context import DestinationContext
from interloper.errors import ConfigError
from interloper.serialization.base import Component
from interloper.utils.text import validate_key


class Destination(Component, ABC):
    """Abstract base class for destination implementations.

    Subclasses must implement :meth:`read`, :meth:`write`, and
    :meth:`partition_row_counts`.  Serialization is provided automatically
    by :class:`Component` via ``model_dump()``.

    The ``key`` field (inherited from :class:`Component`) is auto-derived
    from the class name by stripping the ``Destination`` suffix and lowercasing
    (e.g. ``FileDestination`` → ``"file"``).  It can be overridden explicitly
    when multiple instances of the same destination class are used together.
    """

    _singleton: ClassVar[Destination | None] = None

    def model_post_init(self, __context: Any, /) -> None:
        """Auto-derive key from class name if not explicitly set."""
        if not self.key:
            name = type(self).__name__
            if name.endswith("Destination") and len(name) > len("Destination"):
                name = name[: -len("Destination")]
            self.key = name.lower()
        validate_key(self.key)

    @classmethod
    def singleton(cls: type[Destination]) -> Destination:
        """Get the singleton instance of this destination subclass.

        Returns:
            The singleton instance of the specific destination subclass
        """
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    @abstractmethod
    def write(self, context: DestinationContext, data: Any) -> None:
        """Write data to the destination.

        Args:
            context: Destination context with asset and partition information
            data: Data to write
        """

    @abstractmethod
    def read(self, context: DestinationContext) -> Any:
        """Read data from the destination.

        Args:
            context: Destination context with asset and partition information

        Returns:
            The read data
        """

    @abstractmethod
    def partition_row_counts(self, context: DestinationContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        The partition column is read from ``context.asset.partitioning.column``.
        Each key in the returned dict is the string representation of a partition
        value; each value is the number of rows in that partition.

        Args:
            context: Destination context (uses ``asset.key``, ``asset.dataset``,
                and ``asset.partitioning``).

        Returns:
            Mapping from partition value (as string) to row count.
        """


def validate_destination_keys(destinations: list[Destination], owner: str) -> None:
    """Validate that all destination keys in a list are unique.

    Args:
        destinations: List of destination instances to check.
        owner: Key of the owning entity (for error messages).

    Raises:
        ConfigError: If duplicate keys are found.
    """
    seen: set[str] = set()
    for dest in destinations:
        if dest.key in seen:
            raise ConfigError(f"Duplicate destination key '{dest.key}' on '{owner}'")
        seen.add(dest.key)
