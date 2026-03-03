"""PostgreSQL IO implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper.io.database import WriteDisposition
from interloper.serialization.io import IOSpec
from sqlalchemy import text
from sqlalchemy.engine import URL

from interloper_sql.io.base import SqlIO

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter


class PostgresIO(SqlIO):
    """PostgreSQL-specific IO manager.

    Extends :class:`SqlIO` with PostgreSQL optimisations:

    * Uses ``TRUNCATE`` (transactional in Postgres) instead of ``DELETE`` for
      full-table replacements, which is significantly faster on large tables.

    Args:
        host: Database server hostname
        port: Database server port
        database: Database name
        user: Database user
        password: Database password
        driver: SQLAlchemy driver (e.g. ``psycopg2``, ``asyncpg``)
        write_disposition: Controls whether existing rows are deleted before writing
        chunk_size: Number of rows per insert batch
        adapter: Optional data adapter for type conversion
    """

    def __init__(
        self,
        host: str,
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str | None = None,
        driver: str | None = None,
        write_disposition: WriteDisposition = WriteDisposition.REPLACE,
        chunk_size: int = 1000,
        adapter: DataAdapter | str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

        drivername = f"postgresql+{driver}" if driver else "postgresql"
        url = URL.create(drivername, user, password, host, port, database)
        super().__init__(url, write_disposition, chunk_size, adapter)

    def _delete_all(self, table: str, schema: str | None) -> None:
        """Use TRUNCATE for full-table deletes (transactional in PostgreSQL).

        No-op when the table does not exist yet.

        Args:
            table: Target table name
            schema: Database schema
        """
        assert self._conn is not None
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            return
        self._conn.execute(text(f"TRUNCATE TABLE {sa_table.fullname}"))

    def to_spec(self) -> IOSpec:
        """Convert to serializable spec."""
        init = self._base_init_kwargs()
        init["host"] = self.host
        init["port"] = self.port
        init["database"] = self.database
        init["user"] = self.user
        if self.password is not None:
            init["password"] = self.password
        if self.driver is not None:
            init["driver"] = self.driver
        return IOSpec(path=self.path, init=init)
