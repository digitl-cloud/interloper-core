"""Abstract base class defining the IO read/write interface."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from interloper.io.context import IOContext
from interloper.serialization.base import Serializable

if TYPE_CHECKING:
    from interloper.serialization.io import IOSpec


class IO(Serializable):
    """Abstract base class for IO implementations.

    Subclasses must implement :meth:`read`, :meth:`write`, and :meth:`to_spec`.
    A per-subclass :meth:`singleton` factory is provided for stateless backends.
    """

    _singleton: IO | None = None

    @classmethod
    def singleton(cls: type[IO]) -> IO:
        """Get the singleton instance of this IO subclass.

        Returns:
            The singleton instance of the specific IO subclass
        """
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    @abstractmethod
    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the destination.

        Args:
            context: IO context with asset and partition information
            data: Data to write
        """

    @abstractmethod
    def read(self, context: IOContext) -> Any:
        """Read data from the destination.

        Args:
            context: IO context with asset and partition information

        Returns:
            The read data
        """

    @abstractmethod
    def to_spec(self) -> IOSpec:
        """Convert to serializable spec."""
