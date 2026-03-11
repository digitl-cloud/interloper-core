"""SQLite destination implementation."""

from __future__ import annotations

from typing import Any

from interloper_sql.destination.base import SqlDestination


class SqliteDestination(SqlDestination):
    """SQLite destination manager."""

    database: str

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        url = f"sqlite:///{self.database}"
        self._init_engine(url)

    def __str__(self) -> str:
        return f"SqliteDestination({self.database})"
