"""Tests for MemoryIO."""

import datetime as dt

import pytest

import interloper as il
from interloper.errors import DataNotFoundError


class TestMemoryIO:
    """Tests for MemoryIO."""

    def setup_method(self):
        """Clear memory storage before each test."""
        il.MemoryIO.clear()

    def test_initialization(self):
        """Test MemoryIO initialization."""
        memory_io = il.MemoryIO()
        assert isinstance(memory_io, il.IO)

    def test_write_and_read_non_partitioned(self):
        """Test writing and reading non-partitioned data."""
        memory_io = il.MemoryIO()

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "test_data"

        # Write data
        memory_io.write(context, data)

        # Read data
        result = memory_io.read(context)
        assert result == data

    def test_write_and_read_partitioned(self):
        """Test writing and reading partitioned data."""
        memory_io = il.MemoryIO()

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.IOContext(asset=my_asset(), partition_or_window=partition)
        data = "partitioned_data"

        # Write data
        memory_io.write(context, data)

        # Read data
        result = memory_io.read(context)
        assert result == data

    def test_read_nonexistent_data(self):
        """Test reading non-existent data raises KeyError."""
        memory_io = il.MemoryIO()

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())

        with pytest.raises(DataNotFoundError, match="No data found in memory for: my_asset"):
            memory_io.read(context)

    def test_dataset_organization(self):
        """Test that assets with datasets are stored with dataset prefix."""
        memory_io = il.MemoryIO()

        @il.asset(dataset="my_dataset")
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "dataset_data"

        # Write data
        memory_io.write(context, data)

        # Read data
        result = memory_io.read(context)
        assert result == data

    def test_multiple_assets_same_io(self):
        """Test multiple assets storing to the same MemoryIO instance."""
        memory_io = il.MemoryIO()

        @il.asset
        def asset_a():
            return "a"

        @il.asset
        def asset_b():
            return "b"

        context_a = il.IOContext(asset=asset_a())
        context_b = il.IOContext(asset=asset_b())

        # Write different data for each asset
        memory_io.write(context_a, "data_a")
        memory_io.write(context_b, "data_b")

        # Read data for each asset
        result_a = memory_io.read(context_a)
        result_b = memory_io.read(context_b)

        assert result_a == "data_a"
        assert result_b == "data_b"

    def test_clear_method(self):
        """Test that clear() method removes all stored data."""
        memory_io = il.MemoryIO()

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "test_data"

        # Write data
        memory_io.write(context, data)

        # Verify data exists
        result = memory_io.read(context)
        assert result == data

        # Clear storage
        il.MemoryIO.clear()

        # Verify data is gone
        with pytest.raises(DataNotFoundError):
            memory_io.read(context)

    def test_to_spec(self):
        """Test serialization with to_spec()."""
        memory_io = il.MemoryIO()

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "test_data"

        # Write some data
        memory_io.write(context, data)

        # Get spec
        spec = memory_io.to_spec()

        assert spec.path == "interloper.io.memory.MemoryIO"
        assert spec.init == {}

    def test_partitioned_with_dataset(self):
        """Test partitioned asset with dataset."""
        memory_io = il.MemoryIO()

        @il.asset(dataset="my_dataset", partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.IOContext(asset=my_asset(), partition_or_window=partition)
        data = "partitioned_dataset_data"

        # Write data
        memory_io.write(context, data)

        # Read data
        result = memory_io.read(context)
        assert result == data

    def test_global_storage_shared_across_instances(self):
        """Test that storage is shared across MemoryIO instances."""
        memory_io1 = il.MemoryIO()
        memory_io2 = il.MemoryIO()

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "shared_data"

        # Write with first instance
        memory_io1.write(context, data)

        # Read with second instance
        result = memory_io2.read(context)
        assert result == data

    def test_singleton_pattern(self):
        """Test that singleton() returns the same instance."""
        instance1 = il.MemoryIO.singleton()
        instance2 = il.MemoryIO.singleton()

        assert instance1 is instance2
        assert isinstance(instance1, il.MemoryIO)

    def test_asset_defaults_to_memory_io(self):
        """Test that assets use MemoryIO by default when no IO is specified."""
        # Clear any existing data
        il.MemoryIO.clear()

        @il.asset  # No IO specified
        def my_asset():
            return "default_io_data"

        # Create asset instance and materialize it
        asset_instance = my_asset()
        context = il.IOContext(asset=asset_instance)

        # The asset should have MemoryIO as its IO
        assert isinstance(asset_instance.io, il.MemoryIO)

        # Write some data using the asset's IO
        asset_instance.io.write(context, "test_data")

        # Read it back
        result = asset_instance.io.read(context)
        assert result == "test_data"

