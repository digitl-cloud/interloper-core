"""SQLAlchemy IO implementation."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from interloper.errors import TableNotFoundError
from interloper.io.database import DatabaseIO, WriteDisposition
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy import inspect as sa_inspect

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter
    from sqlalchemy.engine import URL, Connection, Engine


def _infer_sa_type(value: Any) -> Any:
    """Infer a SQLAlchemy column type from a Python value.

    Args:
        value: A sample Python value used to determine the column type.

    Returns:
        A SQLAlchemy type instance.
    """
    import datetime
    from decimal import Decimal

    from sqlalchemy import BigInteger, Boolean, Date, DateTime, Float, LargeBinary, Numeric, Text

    if isinstance(value, bool):
        return Boolean()
    if isinstance(value, int):
        return BigInteger()
    if isinstance(value, float):
        return Float()
    if isinstance(value, Decimal):
        return Numeric()
    if isinstance(value, datetime.datetime):
        return DateTime()
    if isinstance(value, datetime.date):
        return Date()
    if isinstance(value, bytes):
        return LargeBinary()
    return Text()


class SqlIO(DatabaseIO):
    """Base IO implementation for SQL databases via SQLAlchemy.

    Provides connection management, transactional writes, table reflection, and
    automatic table creation.  Not intended for direct instantiation — use a
    dialect subclass (:class:`PostgresIO`, :class:`MySQLIO`, :class:`SqliteIO`)
    which accepts explicit connection parameters and implements ``to_spec``.

    The IO is fully stateless with respect to table identity — the table name
    and schema are passed through from the asset context on every call, so a
    single instance can safely serve multiple assets.

    Args:
        url: SQLAlchemy connection URL or :class:`~sqlalchemy.engine.URL` object
            (constructed by dialect subclasses).
        write_disposition: Controls whether existing rows are deleted before
            writing.  Defaults to :attr:`WriteDisposition.REPLACE`.
        chunk_size: Number of rows per insert batch
        adapter: Optional data adapter for type conversion
    """

    def __init__(
        self,
        url: str | URL,
        write_disposition: WriteDisposition = WriteDisposition.REPLACE,
        chunk_size: int = 1000,
        adapter: DataAdapter | str | None = None,
    ) -> None:
        super().__init__(write_disposition, chunk_size, adapter)
        self._engine: Engine = create_engine(url)
        self._table_cache: dict[tuple[str, str | None], Table] = {}
        self._conn: Connection | None = None

    # ------------------------------------------------------------------
    # Table helpers
    # ------------------------------------------------------------------

    def _resolve_table(self, table: str, schema: str | None) -> Table | None:
        """Reflect and cache the SQLAlchemy Table, or return None if it doesn't exist."""
        key = (table, schema)
        if key not in self._table_cache:
            if sa_inspect(self._engine).has_table(table, schema=schema):
                metadata = MetaData(schema=schema)
                self._table_cache[key] = Table(table, metadata, autoload_with=self._engine)
        return self._table_cache.get(key)

    def _require_table(self, table: str, schema: str | None) -> Table:
        """Resolve the table, raising if it does not exist."""
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            qualified = f"{schema}.{table}" if schema else table
            raise TableNotFoundError(f"Table '{qualified}' does not exist. Has the asset been materialized?")
        return sa_table

    def _create_table(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> Table:
        """Create a new table from the structure of the first row.

        Column types are inferred from the Python values in the sample row
        using :func:`_infer_sa_type`.

        Args:
            table: Target table name
            schema: Database schema
            rows: Row data (at least one row required for schema inference).

        Returns:
            The newly created :class:`~sqlalchemy.schema.Table`.
        """
        assert self._conn is not None
        sample = rows[0]
        columns = [Column(name, _infer_sa_type(value)) for name, value in sample.items()]
        sa_metadata = MetaData(schema=schema)
        sa_table = Table(table, sa_metadata, *columns)
        sa_metadata.create_all(self._conn)
        self._table_cache[(table, schema)] = sa_table
        return sa_table

    # ------------------------------------------------------------------
    # Transaction management
    # ------------------------------------------------------------------

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Open a SQLAlchemy transactional connection for write operations.

        Sets ``self._conn`` for the duration of the block.  The connection is
        committed on success and rolled back on exception (``engine.begin()``
        semantics).

        Yields:
            None
        """
        with self._engine.begin() as conn:
            self._conn = conn
            try:
                yield
            finally:
                self._conn = None

    # ------------------------------------------------------------------
    # DatabaseIO hooks
    # ------------------------------------------------------------------

    def _insert(self, table: str, schema: str | None, rows: list[dict[str, Any]]) -> None:
        """Insert rows in chunks using the active transaction connection.

        If the table does not exist yet, it is created from the row data
        before inserting.

        Args:
            table: Target table name
            schema: Database schema
            rows: Row data as list of dicts
        """
        assert self._conn is not None
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            sa_table = self._create_table(table, schema, rows)
        for i in range(0, len(rows), self.chunk_size):
            self._conn.execute(sa_table.insert(), rows[i : i + self.chunk_size])

    def _delete_all(self, table: str, schema: str | None) -> None:
        """Delete all rows from the table using the active transaction connection.

        No-op when the table does not exist yet.

        Args:
            table: Target table name
            schema: Database schema
        """
        assert self._conn is not None
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            return
        self._conn.execute(sa_table.delete())

    def _delete_partition(self, table: str, schema: str | None, column: str, value: Any) -> None:
        """Delete rows matching a partition value using the active transaction connection.

        No-op when the table does not exist yet.

        Args:
            table: Target table name
            schema: Database schema
            column: Partition column name
            value: Partition value to match
        """
        assert self._conn is not None
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            return
        self._conn.execute(sa_table.delete().where(sa_table.c[column] == value))

    def _select_all(self, table: str, schema: str | None) -> list[dict[str, Any]]:
        """Select all rows from the table.

        Opens a dedicated read connection (not part of the write transaction).

        Args:
            table: Target table name
            schema: Database schema

        Returns:
            All rows as list of dicts

        Raises:
            ValueError: If the table does not exist
        """
        sa_table = self._require_table(table, schema)
        with self._engine.connect() as conn:
            result = conn.execute(sa_table.select())
            return [dict(row._mapping) for row in result]

    def _select_partition(self, table: str, schema: str | None, column: str, value: Any) -> list[dict[str, Any]]:
        """Select rows matching a partition value.

        Opens a dedicated read connection (not part of the write transaction).

        Args:
            table: Target table name
            schema: Database schema
            column: Partition column name
            value: Partition value to match

        Returns:
            Matching rows as list of dicts

        Raises:
            ValueError: If the table does not exist
        """
        sa_table = self._require_table(table, schema)
        with self._engine.connect() as conn:
            result = conn.execute(sa_table.select().where(sa_table.c[column] == value))
            return [dict(row._mapping) for row in result]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def _count_by_partition(
        self, table: str, schema: str | None, column: str,
    ) -> dict[str, int]:
        """Return row counts grouped by partition column via SQL ``GROUP BY``.

        Args:
            table: Target table name
            schema: Database schema
            column: Column to group by

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            TableNotFoundError: If the table does not exist.
        """
        from sqlalchemy import func

        sa_table = self._require_table(table, schema)
        col = sa_table.c[column]
        stmt = sa_table.select().with_only_columns(col, func.count()).group_by(col)
        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            return {str(row[0]): row[1] for row in result}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def dispose(self) -> None:
        """Dispose the SQLAlchemy engine and clear the table cache."""
        self._engine.dispose()
        self._table_cache.clear()
