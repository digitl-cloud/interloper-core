"""Destination system for reading and writing data."""

from interloper.destination.adapter import DataAdapter, RowAdapter
from interloper.destination.base import Destination
from interloper.destination.context import DestinationContext
from interloper.destination.csv import CsvDestination
from interloper.destination.database import DatabaseDestination, WriteDisposition
from interloper.destination.file import FileDestination
from interloper.destination.memory import MemoryDestination

__all__ = [
    "CsvDestination",
    "DataAdapter",
    "DatabaseDestination",
    "Destination",
    "DestinationContext",
    "FileDestination",
    "MemoryDestination",
    "RowAdapter",
    "WriteDisposition",
]
