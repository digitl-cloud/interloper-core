"""IO system for reading and writing data."""

from interloper.io.adapter import DataAdapter, DataFrameAdapter, RowAdapter
from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.io.database import DatabaseIO, WriteDisposition
from interloper.io.file import FileIO
from interloper.io.memory import MemoryIO

__all__ = [
    "IO",
    "DataAdapter",
    "DataFrameAdapter",
    "DatabaseIO",
    "FileIO",
    "IOContext",
    "MemoryIO",
    "RowAdapter",
    "WriteDisposition",
]
