"""MySQL IO implementation."""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine import URL

from interloper_sql.io.base import SqlIO


class MySQLIO(SqlIO):
    """MySQL-specific IO manager.

    Extends :class:`SqlIO` for MySQL connections. Uses standard ``DELETE`` for
    row removal because MySQL's ``TRUNCATE`` causes an implicit commit and
    cannot participate in a transaction.
    """

    host: str | None = None
    database: str | None = None
    port: int = 3306
    username: str = "root"
    password: str | None = None
    driver: str | None = None

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        if self.host is not None:
            drivername = f"mysql+{self.driver}" if self.driver else "mysql"
            url = URL.create(
                drivername, self.username, self.password,
                self.host, self.port, self.database,
            )
            self._init_engine(url)

    def __str__(self) -> str:
        return f"MySQLIO({self.host}:{self.port}/{self.database})"
