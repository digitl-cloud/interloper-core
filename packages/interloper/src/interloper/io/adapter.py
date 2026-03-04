"""Data adapters for converting between typed formats and database rows."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from interloper.errors import AdapterError
from interloper.utils.imports import get_object_path

T = TypeVar("T")


class DataAdapter(ABC, Generic[T]):
    """Converts between a typed data format and database rows (``list[dict]``).

    A ``DataAdapter`` is the bridge between an asset's output type and the
    universal row format that any
    :class:`~interloper.io.database.DatabaseIO` works with internally.

    Subclasses implement two methods:

    * :meth:`to_rows` -- serialise typed data **into** rows (used during writes).
    * :meth:`from_rows` -- deserialise rows **back** to typed data (used during reads).

    Example::

        class MyAdapter(DataAdapter):
            def to_rows(self, data):
                return data.to_records()

            def from_rows(self, rows):
                return MyType.from_records(rows)
    """

    @property
    def path(self) -> str:
        """The fully-qualified import path of this adapter class.

        Used by :meth:`~interloper.io.database.DatabaseIO.to_spec` to persist the
        adapter choice in an :class:`~interloper.serialization.io.IOSpec`.

        Returns:
            Import path string (e.g. ``interloper.io.adapter.RowAdapter``)
        """
        return get_object_path(type(self))

    @abstractmethod
    def to_rows(self, data: T) -> list[dict[str, Any]]:
        """Convert typed data to a list of row dicts for database insertion.

        Args:
            data: Data in the adapter's native format

        Returns:
            Rows as list of dicts
        """

    @abstractmethod
    def from_rows(self, rows: list[dict[str, Any]]) -> T:
        """Convert database rows back to the typed format.

        Args:
            rows: Raw rows from the database

        Returns:
            Data in the adapter's native format
        """


class RowAdapter(DataAdapter[list[dict[str, Any]]]):
    """Identity adapter for ``list[dict]`` data.

    Passes rows through unchanged.  This is functionally equivalent to using
    no adapter at all, but can be used to be explicit about the expected data
    format.
    """

    def to_rows(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pass through row data unchanged.

        Args:
            data: Row data as list of dicts

        Returns:
            The same list of dicts

        Raises:
            AdapterError: If *data* is not a list
        """
        if not isinstance(data, list):
            raise AdapterError(
                f"RowAdapter expects list[dict], got {type(data).__name__}."
            )
        return data

    def from_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pass through row data unchanged.

        Args:
            rows: Raw rows from the database

        Returns:
            The same list of dicts
        """
        return rows
