"""SQL destination managers for reading and writing to databases via SQLAlchemy."""

from interloper_sql.destination.base import SqlDestination
from interloper_sql.destination.mysql import MySQLDestination
from interloper_sql.destination.postgres import PostgresDestination
from interloper_sql.destination.sqlite import SqliteDestination

__all__ = [
    "MySQLDestination",
    "PostgresDestination",
    "SqlDestination",
    "SqliteDestination",
]
