"""PostgreSQL destination implementation."""

from __future__ import annotations

from typing import Any, ClassVar

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

    def __str__(self) -> str:
        return f"PostgresDestination({self.host}:{self.port}/{self.database})"

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
