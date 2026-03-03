"""SQLite IO implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper.io.database import WriteDisposition
from interloper.serialization.io import IOSpec

from interloper_sql.io.base import SqlIO

if TYPE_CHECKING:
    from interloper.io.adapter import DataAdapter


class SqliteIO(SqlIO):
    """SQLite-specific IO manager.

    Extends :class:`SqlIO` for SQLite connections. Useful for local development
    and testing without requiring an external database server.

    Args:
        database: Path to the SQLite database file, or ``":memory:"`` for an
            in-memory database.
        write_disposition: Controls whether existing rows are deleted before writing
        chunk_size: Number of rows per insert batch
        adapter: Optional data adapter for type conversion
    """

    def __init__(
        self,
        database: str = ":memory:",
        write_disposition: WriteDisposition = WriteDisposition.REPLACE,
        chunk_size: int = 1000,
        adapter: DataAdapter | str | None = None,
    ) -> None:
        self.database = database
        url = f"sqlite:///{database}"
        super().__init__(url, write_disposition, chunk_size, adapter)

    def to_spec(self) -> IOSpec:
        """Convert to serializable spec."""
        init = self._base_init_kwargs()
        init["database"] = self.database
        return IOSpec(path=self.path, init=init)
