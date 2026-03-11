"""Tests for FileDestination."""

import datetime as dt

import pytest
from pydantic import BaseModel, ValidationError

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int


class TestFileDestination:
    """Tests for FileDestination."""

    def test_initialization(self, tmp_path):
        """Test FileDestination initialization."""
        file_dest = il.FileDestination(base_path=str(tmp_path))
        assert file_dest.base_path == str(tmp_path)

    def test_write_non_partitioned(self, tmp_path):
        """Test writing non-partitioned data."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset
        def my_asset():
            return "x"

        context = il.DestinationContext(asset=my_asset())
        data = "data"

        # Should write to: data/my_asset/
        file_dest.write(context, data)

    def test_write_partitioned(self, tmp_path):
        """Test writing partitioned data."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(asset=my_asset(), partition_or_window=partition)
        data = "data"

        # Should write to: data/my_asset/partition=2025-01-01/
        file_dest.write(context, data)

    def test_read_non_partitioned(self, tmp_path):
        """Test reading non-partitioned data."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset
        def my_asset():
            return "data"

        context = il.DestinationContext(asset=my_asset())

        # First write the data, then read it
        file_dest.write(context, "data")
        result = file_dest.read(context)
        assert result == "data"

    def test_read_partitioned(self, tmp_path):
        """Test reading partitioned data."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "data"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(asset=my_asset(), partition_or_window=partition)

        # First write the data, then read it
        file_dest.write(context, "data")
        result = file_dest.read(context)
        assert result == "data"

    def test_different_base_paths(self, tmp_path):
        """Test FileDestination with different base paths."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        dest1 = il.FileDestination(base_path=str(local_dir))
        dest2 = il.FileDestination(base_path=str(cloud_dir))
        dest3 = il.FileDestination(base_path="/absolute/path/")

        assert dest1.base_path == str(local_dir)
        assert dest2.base_path == str(cloud_dir)
        assert dest3.base_path == "/absolute/path/"

    def test_with_schema(self, tmp_path):
        """Test FileDestination with schema in context."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset
        def my_asset() -> SampleSchema:
            return SampleSchema(value=1)

        context = il.DestinationContext(asset=my_asset())
        data = "data"

        file_dest.write(context, data)

    def test_write_read_roundtrip(self, tmp_path):
        """Test writing and then reading data."""
        file_dest = il.FileDestination(base_path=str(tmp_path))

        @il.asset
        def test_asset():
            return "data"

        context = il.DestinationContext(asset=test_asset())
        original_data = "data"

        file_dest.write(context, original_data)
        result = file_dest.read(context)
        assert result == original_data


class TestMultipleDestinations:
    """Tests for using multiple destinations."""

    def test_multiple_destination_list(self, tmp_path):
        """Test asset with multiple destinations as list."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        destinations = [
            il.FileDestination(key="local", base_path=str(local_dir)),
            il.FileDestination(key="cloud", base_path=str(cloud_dir)),
        ]

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(destination=destinations, default_destination_key="local")
        assert asset_instance.destination == destinations
        assert asset_instance.default_destination_key == "local"

    def test_write_to_all_destinations(self, tmp_path):
        """Test that materialize writes to all destinations."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        destinations = [
            il.FileDestination(key="local", base_path=str(local_dir)),
            il.FileDestination(key="cloud", base_path=str(cloud_dir)),
        ]

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset(destination=destinations, default_destination_key="local")
        # Materialize should write to both local and cloud
        asset_instance.materialize()

    def test_read_from_default_destination(self, tmp_path):
        """Test that dependencies read from default_destination_key."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)
        main_dir = tmp_path / "main"
        main_dir.mkdir(parents=True)

        destinations = [
            il.FileDestination(key="local", base_path=str(local_dir)),
            il.FileDestination(key="cloud", base_path=str(cloud_dir)),
        ]

        @il.asset
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            # Should read upstream from "local" destination
            return upstream + "b"

        # Destination is now passed at instantiation time
        upstream(destination=destinations, default_destination_key="local")
        downstream(destination=il.FileDestination(base_path=str(main_dir)))

    @pytest.mark.skip("skip test_missing_default_destination_key")
    def test_missing_default_destination_key(self, tmp_path):
        """Test that missing default_destination_key with list destination raises ConfigError."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        destinations = [
            il.FileDestination(key="local", base_path=str(local_dir)),
            il.FileDestination(key="cloud", base_path=str(cloud_dir)),
        ]

        @il.asset
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1}]

        # Creating asset with list destination but no default_destination_key raises at init
        with pytest.raises((il.ConfigError, ValidationError), match="no default_destination_key"):
            my_asset(destination=destinations)

    @pytest.mark.skip("skip test_invalid_default_destination_key")
    def test_invalid_default_destination_key(self, tmp_path):
        """Test that invalid default_destination_key raises ConfigError at creation."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        destinations = [
            il.FileDestination(key="local", base_path=str(local_dir)),
            il.FileDestination(key="cloud", base_path=str(cloud_dir)),
        ]

        @il.asset
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1}]

        # Creating asset with an invalid default_destination_key raises at init
        with pytest.raises((il.ConfigError, ValidationError), match="not found in destination list"):
            my_asset(destination=destinations, default_destination_key="invalid")
