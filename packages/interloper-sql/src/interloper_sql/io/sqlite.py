"""SQLite IO implementation."""

from __future__ import annotations

from typing import Any

from interloper_sql.io.base import SqlIO


class SqliteIO(SqlIO):
    """SQLite-specific IO manager.

    Extends :class:`SqlIO` for SQLite connections. Useful for local development
    and testing without requiring an external database server.
    """

    database: str | None = None

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        if self.database is not None:
            url = f"sqlite:///{self.database}"
            self._init_engine(url)

    def __str__(self) -> str:
        return f"SqliteIO({self.database})"
