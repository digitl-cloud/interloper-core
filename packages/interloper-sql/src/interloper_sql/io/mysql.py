"""MySQL IO implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper.io.database import WriteDisposition
from interloper.serialization.io import IOInstanceSpec
from sqlalchemy.engine import URL

from interloper_sql.io.base import SqlIO

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter


class MySQLIO(SqlIO):
    """MySQL-specific IO manager.

    Extends :class:`SqlIO` for MySQL connections. Uses standard ``DELETE`` for
    row removal because MySQL's ``TRUNCATE`` causes an implicit commit and
    cannot participate in a transaction.

    Args:
        host: Database server hostname
        database: Database name
        port: Database server port
        user: Database user
        password: Database password
        driver: SQLAlchemy driver (e.g. ``pymysql``, ``mysqlconnector``)
        write_disposition: Controls whether existing rows are deleted before writing
        chunk_size: Number of rows per insert batch
        adapter: Optional data adapter for type conversion
    """

    def __init__(
        self,
        host: str,
        database: str,
        port: int = 3306,
        user: str = "root",
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

        drivername = f"mysql+{driver}" if driver else "mysql"
        url = URL.create(drivername, user, password, host, port, database)
        super().__init__(url, write_disposition, chunk_size, adapter)

    def to_spec(self) -> IOInstanceSpec:
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
        return IOInstanceSpec(path=self.path, init=init)
