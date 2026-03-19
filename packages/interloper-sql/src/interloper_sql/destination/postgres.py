"""PostgreSQL destination implementation."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.destination.context import DestinationContext
from sqlalchemy import text
from sqlalchemy.engine import URL

from interloper_sql.destination.base import SqlDestination


class PostgresDestination(SqlDestination):
    """PostgreSQL destination manager."""

    label: ClassVar[str] = "PostgreSQL"

    host: str
    port: int = 5432
    database: str = "postgres"
    username: str = "postgres"
    password: str | None = None
    driver: str | None = None

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        drivername = f"postgresql+{self.driver}" if self.driver else "postgresql"
        url = URL.create(
            drivername, self.username, self.password,
            self.host, self.port, self.database,
        )
        self._init_engine(url)

    def _delete_all(self, context: DestinationContext, table: str, schema: str | None) -> None:
        """Use TRUNCATE for full-table deletes (transactional in PostgreSQL).

        No-op when the table does not exist yet.

        Args:
            context: Destination context with asset and partition information
            table: Target table name
            schema: Database schema
        """
        assert self._conn is not None
        sa_table = self._resolve_table(table, schema)
        if sa_table is None:
            return
        self._conn.execute(text(f"TRUNCATE TABLE {sa_table.fullname}"))
