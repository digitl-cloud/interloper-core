"""Abstract base class for database-backed destination implementations."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import Any

from pydantic import field_validator

from interloper.destination.adapter import DataAdapter
from interloper.destination.base import Destination
from interloper.destination.context import DestinationContext
from interloper.errors import AdapterError
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.serialization.base import ComponentInstanceSpec, reconstruct_components


class WriteDisposition(str, Enum):
    """How a write operation should behave relative to existing data.

    Members:
        REPLACE: Delete existing rows (scoped to the active partition when
            partitioned) before inserting new data.
        APPEND: Insert new rows without touching existing data.
    """

    REPLACE = "replace"
    APPEND = "append"


class DatabaseDestination(Destination):
    """Abstract base class for database-backed destination implementations.

    Provides the partition-aware write/read dispatch logic that is common to any
    database backend (SQL, NoSQL, data-warehouse, etc.).  Subclasses only need to
    implement a small set of abstract hooks for the actual database operations.

    The target table name and schema are derived from the asset at call time
    (``asset.key`` → table, ``asset.dataset`` → schema) and passed as
    parameters to every hook.  The destination instance itself holds **no** table
    identity and can be safely shared across multiple assets.

    One or more :class:`~interloper.destination.adapter.DataAdapter` instances can be
    provided to convert between the asset's data type (e.g. a DataFrame) and
    the universal ``list[dict]`` row format used internally by every database
    hook.  When multiple adapters are configured, writes try each in order
    until one succeeds; reads use the first adapter.
    """

    write_disposition: WriteDisposition = WriteDisposition.REPLACE
    chunk_size: int = 1000
    adapter: list[DataAdapter] | None = None

    @field_validator("adapter", mode="before")
    @classmethod
    def _normalize_adapter(cls, v: Any) -> list[DataAdapter] | None:
        """Normalize adapter input before Pydantic validation.

        Accepts a single ``DataAdapter``, a single import-path string,
        a ``list[DataAdapter | str | dict]``, or ``None``.  Strings and
        dicts are reconstructed via :func:`reconstruct_components`.

        Returns:
            Normalized adapter list, or ``None``.
        """
        if v is None:
            return None
        if not isinstance(v, list):
            v = [v]
        return reconstruct_components(v)

    def to_spec(self) -> ComponentInstanceSpec:
        """Generate an instance spec, serializing adapters as nested specs.

        Follows the same pattern as Source/Asset for nested Components:
        adapters are excluded from ``model_dump()`` and serialized via
        their own ``to_spec()``.

        Returns:
            A ComponentInstanceSpec capturing this instance's state.
        """
        config_data = self.model_dump(
            mode="json",
            exclude={"key", "adapter"},
            exclude_none=True,
        )
        if self.adapter is not None:
            config_data["adapter"] = [
                a.to_spec().model_dump(exclude_none=True, exclude_defaults=True) for a in self.adapter
            ]
        return ComponentInstanceSpec(
            path=self.path,
            config=config_data or None,
            init={"key": self.key} if self.key else {},
        )

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
            table: Target table name (from ``asset.key``)
            schema: Database schema (from ``asset.dataset``)
            rows: Row data as list of dicts
        """

    @abstractmethod
    def _delete_all(self, table: str, schema: str | None) -> None:
        """Delete all rows from the target table.

        Called inside a :meth:`_transaction` context during writes with
        :attr:`WriteDisposition.REPLACE` and no partition context.

        Args:
            table: Target table name (from ``asset.key``)
            schema: Database schema (from ``asset.dataset``)
        """

    @abstractmethod
    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a single partition value.

        Called inside a :meth:`_transaction` context during writes with
        :attr:`WriteDisposition.REPLACE`.

        Args:
            table: Target table name (from ``asset.key``)
            schema: Database schema (from ``asset.dataset``)
            column: Partition column name
            value: Partition value to match
        """

    @abstractmethod
    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the target table.

        Args:
            table: Target table name (from ``asset.key``)
            schema: Database schema (from ``asset.dataset``)

        Returns:
            All rows as list of dicts
        """

    @abstractmethod
    def _select_partition(
        self,
        table: str,
        schema: str | None,
        column: str,
        value: Any,
    ) -> list[dict[str, Any]]:
        """Select rows matching a single partition value.

        Args:
            table: Target table name (from ``asset.key``)
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
        self,
        table: str,
        schema: str | None,
        column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by the values of the given column.

        Args:
            table: Target table name (from ``asset.key``)
            schema: Database schema (from ``asset.dataset``)
            column: Column to group by.

        Returns:
            Mapping from partition value (as string) to row count.
        """

    def partition_row_counts(self, context: DestinationContext) -> dict[str, int]:
        """Return row counts grouped by the asset's partition column.

        Delegates to :meth:`_count_by_partition` using the table, schema, and
        partition column extracted from the context.

        Args:
            context: Destination context with asset and partition information.

        Returns:
            Mapping from partition value (as string) to row count.
        """
        assert context.asset.partitioning is not None
        return self._count_by_partition(
            context.asset.key,
            context.asset.dataset,
            context.asset.partitioning.column,
        )

    # ------------------------------------------------------------------
    # Data conversion
    # ------------------------------------------------------------------

    def _to_rows(self, data: Any) -> list[dict[str, Any]]:
        """Convert input data to a list of row dicts.

        When adapters are configured, tries each in order until one succeeds.
        Falls back to accepting ``list[dict]`` directly if no adapter handles
        the data (or if no adapters are configured).

        Args:
            data: Input data to convert

        Returns:
            Data as list of dicts

        Raises:
            AdapterError: If the data type is not supported
        """
        if self.adapter is not None:
            assert isinstance(self.adapter, list)
            for adapter in self.adapter:
                try:
                    return adapter.to_rows(data)
                except AdapterError:
                    continue
        if isinstance(data, list):
            return data
        configured = ", ".join(type(a).__name__ for a in self.adapter) if isinstance(self.adapter, list) else "none"
        raise AdapterError(
            f"No adapter on {type(self).__name__} could handle {type(data).__name__} "
            f"(configured: [{configured}]). "
            f"Either pass list[dict] or configure a suitable DataAdapter."
        )

    def _from_rows(self, rows: list[dict[str, Any]]) -> Any:
        """Convert database rows back to the configured data format.

        When adapters are configured, the first adapter in the list is used
        to determine the read format.  Otherwise returns the raw
        ``list[dict]``.

        Args:
            rows: Raw rows from the database

        Returns:
            Data in the first adapter's format, or raw ``list[dict]``
        """
        if self.adapter:
            assert isinstance(self.adapter, list)
            return self.adapter[0].from_rows(rows)
        return rows

    # ------------------------------------------------------------------
    # Destination interface
    # ------------------------------------------------------------------

    def write(self, context: DestinationContext, data: Any) -> None:
        """Write data to the database table.

        With :attr:`WriteDisposition.REPLACE`, deletes matching rows (scoped to
        the active partition when partitioned) before inserting.  With
        :attr:`WriteDisposition.APPEND`, rows are inserted without any prior
        deletion.

        When the context contains a ``PartitionWindow``, rows for **every** partition
        in the window are deleted (if replacing) and the data is inserted once.

        Args:
            context: Destination context with asset and partition information
            data: Data to write (type must match the configured adapter, or
                ``list[dict]`` when no adapter is set)
        """
        table = context.asset.key
        schema = context.asset.dataset
        rows = self._to_rows(data)

        if not rows:
            return

        if context.partition_or_window is not None and context.asset.partitioning is not None:
            col = context.asset.partitioning.column
            if col not in rows[0]:
                import warnings

                warnings.warn(
                    f"Partition column '{col}' not found in data for asset "
                    f"'{context.asset.key}'. Columns present: {sorted(rows[0].keys())}. "
                    f"Downstream reads by partition will fail.",
                    UserWarning,
                    stacklevel=2,
                )

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

    def read(self, context: DestinationContext) -> Any:
        """Read data from the database table.

        When reading with a ``PartitionWindow``, returns a list of results, one
        per partition (matching the convention of ``FileDestination`` / ``MemoryDestination``).

        The return type depends on the configured :attr:`adapter` list:

        * No adapters → ``list[dict]`` (or ``list[list[dict]]`` for windows)
        * With adapters → first adapter's format (or list thereof for windows)

        Args:
            context: Destination context with asset and partition information

        Returns:
            Data in the adapter's format, or raw rows
        """
        table = context.asset.key
        schema = context.asset.dataset

        # No partitioning
        if context.partition_or_window is None:
            return self._from_rows(self._select_all(table, schema))

        # Partition window -- list of results per partition
        if isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            col = context.asset.partitioning.column
            return [
                self._from_rows(self._select_partition(table, schema, col, p.id)) for p in context.partition_or_window
            ]

        # Single partition
        assert isinstance(context.partition_or_window, Partition)
        assert context.asset.partitioning
        col = context.asset.partitioning.column
        return self._from_rows(self._select_partition(table, schema, col, context.partition_or_window.id))
