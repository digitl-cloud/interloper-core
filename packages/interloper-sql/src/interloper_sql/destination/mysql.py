"""MySQL destination implementation."""

from __future__ import annotations

from typing import Any

from sqlalchemy.engine import URL

from interloper_sql.destination.base import SqlDestination


class MySQLDestination(SqlDestination):
    """MySQL destination manager."""

    host: str
    database: str
    port: int = 3306
    username: str = "root"
    password: str | None = None
    driver: str | None = None

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        drivername = f"mysql+{self.driver}" if self.driver else "mysql"
        url = URL.create(
            drivername,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
        )
        self._init_engine(url)
