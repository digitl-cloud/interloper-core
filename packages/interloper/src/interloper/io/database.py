"""Abstract base class for database-backed IO implementations."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING, Any

from interloper.errors import AdapterError
from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter


class WriteDisposition(str, Enum):
    """How a write operation should behave relative to existing data.

    Members:
        REPLACE: Delete existing rows (scoped to the active partition when
            partitioned) before inserting new data.
        APPEND: Insert new rows without touching existing data.
    """

    REPLACE = "replace"
    APPEND = "append"


class DatabaseIO(IO):
    """Abstract base class for database-backed IO implementations.

    Provides the partition-aware write/read dispatch logic that is common to any
    database backend (SQL, NoSQL, data-warehouse, etc.).  Subclasses only need to
    implement a small set of abstract hooks for the actual database operations.

    The target table name and schema are derived from the asset at call time
    (``asset.name`` → table, ``asset.dataset`` → schema) and passed as
    parameters to every hook.  The IO instance itself holds **no** table
    identity and can be safely shared across multiple assets.

    An optional :class:`~interloper.io.adapter.DataAdapter` can be provided to
    convert between the asset's data type (e.g. a custom data format) and the
    universal ``list[dict]`` row format used internally by every database hook.

    **Write flow** (``write_disposition=REPLACE``):

    1. Convert data to rows via :meth:`_to_rows` (delegates to the adapter).
    2. Open a transaction via :meth:`_transaction` (no-op by default).
    3. Delete the target rows (scoped to partition when partitioned).
    4. Insert the new rows.
    5. Commit / close the transaction.

    **Read flow**: dispatch on partition context, delegate to
    :meth:`_select_all` or :meth:`_select_partition`, then convert the result
    via :meth:`_from_rows`.

    Args:
        write_disposition: Controls whether existing rows are deleted before
            writing.  Defaults to :attr:`WriteDisposition.APPEND`.
        chunk_size: Hint for bulk-insert batch size (subclasses decide how to use it)
        adapter: Optional data adapter for type conversion between the asset's
            data format and ``list[dict]`` rows.  When *None*, data must be
            ``list[dict]`` and is passed through unchanged.
    """

    def __init__(
        self,
        write_disposition: WriteDisposition = WriteDisposition.APPEND,
        chunk_size: int = 1000,
        adapter: DataAdapter | str | None = None,
    ) -> None:
        """Initialize DatabaseIO.

        Args:
            write_disposition: Write behavior
            chunk_size: Hint for bulk-insert batch size
            adapter: Data adapter instance, import path string, or *None*
        """
        self.write_disposition = WriteDisposition(write_disposition)
        self.chunk_size = chunk_size

        # Accept an import-path string so that IOSpec reconstruction works
        resolved: DataAdapter | None = None
        if isinstance(adapter, str):
            from interloper.utils.imports import import_from_path

            resolved = import_from_path(adapter)()
        elif adapter is not None:
            resolved = adapter
        self.adapter = resolved

    # ------------------------------------------------------------------
    # Transaction hook
    # ------------------------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Context manager wrapping write operations.

        Override to provide transactional guarantees (e.g. SQL
        ``BEGIN … COMMIT``).  The default implementation is a no-op.

        Yields:
            None
        """
        yield

    # ------------------------------------------------------------------
    # Abstract database operations
    # ------------------------------------------------------------------

    @abstractmethod
    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows into the target table.

        Called inside a :meth:`_transaction` context during writes.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)
            rows: Row data as list of dicts
        """

    @abstractmethod
    def _delete_all(self, table: str, schema: str | None) -> None:
        """Delete all rows from the target table.

        Called inside a :meth:`_transaction` context during writes with
        :attr:`WriteDisposition.REPLACE` and no partition context.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)
        """

    @abstractmethod
    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a single partition value.

        Called inside a :meth:`_transaction` context during writes with
        :attr:`WriteDisposition.REPLACE`.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)
            column: Partition column name
            value: Partition value to match
        """

    @abstractmethod
    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the target table.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)

        Returns:
            All rows as list of dicts
        """

    @abstractmethod
    def _select_partition(
        self, table: str, schema: str | None, column: str, value: Any,
    ) -> list[dict[str, Any]]:
        """Select rows matching a single partition value.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)
            column: Partition column name
            value: Partition value to match

        Returns:
            Matching rows as list of dicts
        """

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @abstractmethod
    def _count_by_partition(
        self, table: str, schema: str | None, column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by the values of the given column.

        Args:
            table: Target table name (from ``asset.name``)
            schema: Database schema (from ``asset.dataset``)
            column: Column to group by.

        Returns:
            Mapping from partition value (as string) to row count.
        """

    def partition_row_counts(self, context: IOContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        Delegates to :meth:`_count_by_partition` using the table, schema, and
        partition column extracted from the context.

        Args:
            context: IO context with asset and partition information.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        return self._count_by_partition(
            context.asset.name,
            context.asset.dataset,
            context.asset.partitioning.column,
        )

    # ------------------------------------------------------------------
    # Data conversion
    # ------------------------------------------------------------------

    def _to_rows(self, data: Any) -> list[dict[str, Any]]:
        """Convert input data to a list of row dicts.

        If an :attr:`adapter` is configured, delegates to
        :meth:`DataAdapter.to_rows`.  Otherwise accepts ``list[dict]``
        directly.

        Args:
            data: Input data to convert

        Returns:
            Data as list of dicts

        Raises:
            AdapterError: If the data type is not supported
        """
        if self.adapter is not None:
            return self.adapter.to_rows(data)
        if isinstance(data, list):
            return data
        raise AdapterError(
            f"No adapter configured on {type(self).__name__} and data is not list[dict] "
            f"(got {type(data).__name__}). Either pass list[dict] or configure a DataAdapter."
        )

    def _from_rows(self, rows: list[dict[str, Any]]) -> Any:
        """Convert database rows back to the configured data format.

        If an :attr:`adapter` is configured, delegates to
        :meth:`DataAdapter.from_rows`.  Otherwise returns the raw
        ``list[dict]``.

        Args:
            rows: Raw rows from the database

        Returns:
            Data in the adapter's format, or raw ``list[dict]``
        """
        if self.adapter is not None:
            return self.adapter.from_rows(rows)
        return rows

    # ------------------------------------------------------------------
    # IO interface
    # ------------------------------------------------------------------

    def write(self, context: IOContext, data: Any) -> None:
        """Write data to the database table.

        With :attr:`WriteDisposition.REPLACE`, deletes matching rows (scoped to
        the active partition when partitioned) before inserting.  With
        :attr:`WriteDisposition.APPEND`, rows are inserted without any prior
        deletion.

        When the context contains a ``PartitionWindow``, rows for **every** partition
        in the window are deleted (if replacing) and the data is inserted once.

        Args:
            context: IO context with asset and partition information
            data: Data to write (type must match the configured adapter, or
                ``list[dict]`` when no adapter is set)
        """
        table = context.asset.name
        schema = context.asset.dataset
        rows = self._to_rows(data)

        if not rows:
            return

        replacing = self.write_disposition is WriteDisposition.REPLACE

        with self._transaction():
            # No partitioning
            if context.partition_or_window is None:
                if replacing:
                    self._delete_all(table, schema)
                self._insert(table, schema, rows)

            # Partition window -- delete each partition, insert once
            elif isinstance(context.partition_or_window, PartitionWindow):
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    for partition in context.partition_or_window:
                        self._delete_partition(table, schema, col, partition.id)
                self._insert(table, schema, rows)

            # Single partition
            else:
                assert isinstance(context.partition_or_window, Partition)
                assert context.asset.partitioning
                col = context.asset.partitioning.column
                if replacing:
                    self._delete_partition(table, schema, col, context.partition_or_window.id)
                self._insert(table, schema, rows)

    def read(self, context: IOContext) -> Any:
        """Read data from the database table.

        When reading with a ``PartitionWindow``, returns a list of results, one
        per partition (matching the convention of ``FileIO`` / ``MemoryIO``).

        The return type depends on the configured :attr:`adapter`:

        * No adapter → ``list[dict]`` (or ``list[list[dict]]`` for windows)
        * With adapter → ``T`` (or ``list[T]`` for windows)

        Args:
            context: IO context with asset and partition information

        Returns:
            Data in the adapter's format, or raw rows
        """
        table = context.asset.name
        schema = context.asset.dataset

        # No partitioning
        if context.partition_or_window is None:
            return self._from_rows(self._select_all(table, schema))

        # Partition window -- list of results per partition
        if isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            col = context.asset.partitioning.column
            return [
                self._from_rows(self._select_partition(table, schema, col, p.id))
                for p in context.partition_or_window
            ]

        # Single partition
        assert isinstance(context.partition_or_window, Partition)
        assert context.asset.partitioning
        col = context.asset.partitioning.column
        return self._from_rows(self._select_partition(table, schema, col, context.partition_or_window.id))

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    def _base_init_kwargs(self) -> dict[str, Any]:
        """Return the base constructor kwargs shared by all ``DatabaseIO`` subclasses.

        Subclasses should call this from :meth:`to_spec` and merge in their own
        kwargs (e.g. ``url``) to avoid repetition.

        Returns:
            Dict of kwargs suitable for :class:`~interloper.serialization.io.IOSpec`
        """
        kwargs: dict[str, Any] = {
            "write_disposition": self.write_disposition.value,
            "chunk_size": self.chunk_size,
        }
        if self.adapter is not None:
            kwargs["adapter"] = self.adapter.path
        return kwargs
