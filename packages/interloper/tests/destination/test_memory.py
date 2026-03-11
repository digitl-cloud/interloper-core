"""Tests for MemoryDestination."""

import datetime as dt

import pytest

import interloper as il
from interloper.errors import DataNotFoundError


class TestMemoryDestination:
    """Tests for MemoryDestination."""

    def setup_method(self):
        """Clear memory storage before each test."""
        il.MemoryDestination.clear()

    def test_initialization(self):
        """Test MemoryDestination initialization."""
        dest_instance = il.MemoryDestination()
        assert isinstance(dest_instance, il.Destination)

    def test_write_and_read_non_partitioned(self):
        """Test writing and reading non-partitioned data."""
        dest_instance = il.MemoryDestination()

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "test_data"

        # Write data
        dest_instance.write(context, data)

        # Read data
        result = dest_instance.read(context)
        assert result == data

    def test_write_and_read_partitioned(self):
        """Test writing and reading partitioned data."""
        dest_instance = il.MemoryDestination()

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(asset=my_asset(), partition_or_window=partition)
        data = "partitioned_data"

        # Write data
        dest_instance.write(context, data)

        # Read data
        result = dest_instance.read(context)
        assert result == data

    def test_read_nonexistent_data(self):
        """Test reading non-existent data raises KeyError."""
        dest_instance = il.MemoryDestination()

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())

        with pytest.raises(DataNotFoundError, match="No data found in memory for: my_asset"):
            dest_instance.read(context)

    def test_dataset_organization(self):
        """Test that assets with datasets are stored with dataset prefix."""
        dest_instance = il.MemoryDestination()

        @il.asset(dataset="my_dataset")
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "dataset_data"

        # Write data
        dest_instance.write(context, data)

        # Read data
        result = dest_instance.read(context)
        assert result == data

    def test_multiple_assets_same_destination(self):
        """Test multiple assets storing to the same MemoryDestination instance."""
        dest_instance = il.MemoryDestination()

        @il.asset
        def asset_a():
            return "a"

        @il.asset
        def asset_b():
            return "b"

        context_a = il.DestinationContext(asset=asset_a())
        context_b = il.DestinationContext(asset=asset_b())

        # Write different data for each asset
        dest_instance.write(context_a, "data_a")
        dest_instance.write(context_b, "data_b")

        # Read data for each asset
        result_a = dest_instance.read(context_a)
        result_b = dest_instance.read(context_b)

        assert result_a == "data_a"
        assert result_b == "data_b"

    def test_clear_method(self):
        """Test that clear() method removes all stored data."""
        dest_instance = il.MemoryDestination()

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "test_data"

        # Write data
        dest_instance.write(context, data)

        # Verify data exists
        result = dest_instance.read(context)
        assert result == data

        # Clear storage
        il.MemoryDestination.clear()

        # Verify data is gone
        with pytest.raises(DataNotFoundError):
            dest_instance.read(context)

    def test_to_spec(self):
        """Test serialization with to_spec()."""
        dest_instance = il.MemoryDestination()

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "test_data"

        # Write some data
        dest_instance.write(context, data)

        # Get spec
        spec = dest_instance.to_spec()

        assert spec.path == "interloper.destination.memory.MemoryDestination"
        assert spec.init == {"key": "memory"}

    def test_partitioned_with_dataset(self):
        """Test partitioned asset with dataset."""
        dest_instance = il.MemoryDestination()

        @il.asset(dataset="my_dataset", partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(asset=my_asset(), partition_or_window=partition)
        data = "partitioned_dataset_data"

        # Write data
        dest_instance.write(context, data)

        # Read data
        result = dest_instance.read(context)
        assert result == data

    def test_global_storage_shared_across_instances(self):
        """Test that storage is shared across MemoryDestination instances."""
        dest1 = il.MemoryDestination()
        dest2 = il.MemoryDestination()

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "shared_data"

        # Write with first instance
        dest1.write(context, data)

        # Read with second instance
        result = dest2.read(context)
        assert result == data

    def test_singleton_pattern(self):
        """Test that singleton() returns the same instance."""
        instance1 = il.MemoryDestination.singleton()
        instance2 = il.MemoryDestination.singleton()

        assert instance1 is instance2
        assert isinstance(instance1, il.MemoryDestination)

    def test_asset_defaults_to_memory_destination(self):
        """Test that assets use MemoryDestination by default when no destination is specified."""
        # Clear any existing data
        il.MemoryDestination.clear()

        @il.asset  # No destination specified
        def my_asset():
            return "default_destination_data"

        # Create asset instance and materialize it
        asset_instance = my_asset()
        context = il.DestinationContext(asset=asset_instance)

        # The asset should have MemoryDestination as its destination
        assert isinstance(asset_instance.destination, il.MemoryDestination)

        # Write some data using the asset's destination
        asset_instance.destination.write(context, "test_data")

        # Read it back
        result = asset_instance.destination.read(context)
        assert result == "test_data"
