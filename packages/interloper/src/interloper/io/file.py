"""Filesystem-backed IO using pickle serialization."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.serialization.io import IOSpec


class FileIO(IO):
    """IO that reads and writes pickle files on the local filesystem.

    Data is stored under ``{base_path}/{dataset}/{asset_name}/data.pkl``
    (or ``{base_path}/{asset_name}/data.pkl`` when no dataset is set).
    Partitioned assets add a ``{column}={id}`` subdirectory.
    """

    def __init__(self, base_path: str):
        """Initialize FileIO.

        Args:
            base_path: Base directory path for file storage
        """
        self.base_path = base_path

    def write(self, context: IOContext, data: Any) -> None:
        """Pickle data to a file, creating partition subdirectories as needed.

        Args:
            context: IO context with asset and partition information.
            data: Data to write.
        """
        base_path = Path(self.base_path) / (context.asset.dataset if context.asset.dataset else "") / context.asset.name
        base_path.mkdir(parents=True, exist_ok=True)

        # No partitioning - write directly
        if context.partition_or_window is None:
            file_path = base_path / "data.pkl"
            with file_path.open("wb") as f:
                pickle.dump(data, f)

        # Partition window - write for each partition
        elif isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning

            for partition in context.partition_or_window:
                partition_path = base_path / f"{context.asset.partitioning.column}={partition.id}"
                partition_path.mkdir(parents=True, exist_ok=True)
                file_path = partition_path / "data.pkl"
                with file_path.open("wb") as f:
                    pickle.dump(data, f)

        # Single partition
        else:
            assert isinstance(context.partition_or_window, Partition)
            assert context.asset.partitioning

            partition_path = base_path / f"{context.asset.partitioning.column}={context.partition_or_window.id}"
            partition_path.mkdir(parents=True, exist_ok=True)
            file_path = partition_path / "data.pkl"
            with file_path.open("wb") as f:
                pickle.dump(data, f)

    def read(self, context: IOContext) -> Any:
        """Unpickle data from a file.

        Args:
            context: IO context with asset and partition information.

        Returns:
            The deserialized data, or a list of results for partition windows.

        Raises:
            FileNotFoundError: If the expected data file does not exist.
        """
        base_path = Path(self.base_path) / (context.asset.dataset if context.asset.dataset else "") / context.asset.name

        # No partitioning - read directly
        if context.partition_or_window is None:
            file_path = base_path / "data.pkl"
            if not file_path.exists():
                raise FileNotFoundError(f"Data file not found for asset {context.asset.name}: {file_path}")
            with file_path.open("rb") as f:
                return pickle.load(f)

        # Partition window - read each partition and return as list
        elif isinstance(context.partition_or_window, PartitionWindow):
            assert context.asset.partitioning
            results = []
            for partition in context.partition_or_window:
                file_path = base_path / f"{context.asset.partitioning.column}={partition.id}" / "data.pkl"
                if not file_path.exists():
                    raise FileNotFoundError(f"Data file not found for asset {context.asset.name}: {file_path}")
                with file_path.open("rb") as f:
                    results.append(pickle.load(f))
            return results

        # Single partition
        else:
            assert isinstance(context.partition_or_window, Partition)
            assert context.asset.partitioning
            file_path = base_path / f"{context.asset.partitioning.column}={context.partition_or_window.id}" / "data.pkl"
            if not file_path.exists():
                raise FileNotFoundError(f"Data file not found for asset {context.asset.name}: {file_path}")
            with file_path.open("rb") as f:
                return pickle.load(f)

    def to_spec(self) -> IOSpec:
        """Convert to a serializable spec.

        Returns:
            The IOSpec representation of this FileIO.
        """
        return IOSpec(
            path=self.path,
            init={"base_path": self.base_path},
        )
