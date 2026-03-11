"""DataFrame adapter for converting between pandas DataFrames and database rows."""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.destination.adapter import DataAdapter
from interloper.errors import AdapterError


class DataFrameAdapter(DataAdapter):
    """Adapter for pandas ``DataFrame``.

    Converts between ``DataFrame`` and ``list[dict]`` row format used by
    :class:`~interloper.destination.database.DatabaseDestination`.
    """

    def to_rows(self, data: Any) -> list[dict[str, Any]]:
        """Convert a ``DataFrame`` to a list of row dicts.

        Args:
            data: A pandas ``DataFrame``

        Returns:
            Rows as list of dicts

        Raises:
            TypeError: If *data* is not a ``DataFrame``
        """
        if not isinstance(data, pd.DataFrame):
            raise AdapterError(f"DataFrameAdapter expects a pandas DataFrame, got {type(data).__name__}.")
        return data.to_dict("records")

    def from_rows(self, rows: list[dict[str, Any]]) -> Any:
        """Convert rows to a pandas ``DataFrame``.

        Args:
            rows: Raw rows from the database

        Returns:
            A pandas ``DataFrame``
        """
        return pd.DataFrame(rows)
