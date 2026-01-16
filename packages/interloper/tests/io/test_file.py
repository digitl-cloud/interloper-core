"""Tests for FileIO."""

import datetime as dt

import pytest
from pydantic import BaseModel

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int


class TestFileIO:
    """Tests for FileIO."""

    def test_initialization(self, tmp_path):
        """Test FileIO initialization."""
        file_io = il.FileIO(tmp_path)
        assert file_io.base_path == tmp_path

    def test_write_non_partitioned(self, tmp_path):
        """Test writing non-partitioned data."""
        file_io = il.FileIO(tmp_path)

        @il.asset
        def my_asset():
            return "x"

        context = il.IOContext(asset=my_asset())
        data = "data"

        # Should write to: data/my_asset/
        file_io.write(context, data)

    def test_write_partitioned(self, tmp_path):
        """Test writing partitioned data."""
        file_io = il.FileIO(tmp_path)

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "x"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.IOContext(asset=my_asset(), partition_or_window=partition)
        data = "data"

        # Should write to: data/my_asset/partition=2025-01-01/
        file_io.write(context, data)

    def test_read_non_partitioned(self, tmp_path):
        """Test reading non-partitioned data."""
        file_io = il.FileIO(tmp_path)

        @il.asset
        def my_asset():
            return "data"

        context = il.IOContext(asset=my_asset())

        # First write the data, then read it
        file_io.write(context, "data")
        result = file_io.read(context)
        assert result == "data"

    def test_read_partitioned(self, tmp_path):
        """Test reading partitioned data."""
        file_io = il.FileIO(tmp_path)

        @il.asset(partitioning=il.TimePartitionConfig(column="ds"))
        def my_asset():
            return "data"

        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.IOContext(asset=my_asset(), partition_or_window=partition)

        # First write the data, then read it
        file_io.write(context, "data")
        result = file_io.read(context)
        assert result == "data"

    def test_different_base_paths(self, tmp_path):
        """Test FileIO with different base paths."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        io1 = il.FileIO(str(local_dir))
        io2 = il.FileIO(str(cloud_dir))
        io3 = il.FileIO("/absolute/path/")

        assert io1.base_path == str(local_dir)
        assert io2.base_path == str(cloud_dir)
        assert io3.base_path == "/absolute/path/"

    def test_with_schema(self, tmp_path):
        """Test FileIO with schema in context."""
        file_io = il.FileIO(tmp_path)

        @il.asset
        def my_asset() -> SampleSchema:
            return SampleSchema(value=1)

        context = il.IOContext(asset=my_asset())
        data = "data"

        file_io.write(context, data)

    def test_write_read_roundtrip(self, tmp_path):
        """Test writing and then reading data."""
        file_io = il.FileIO(tmp_path)

        @il.asset
        def test_asset():
            return "data"

        context = il.IOContext(asset=test_asset())
        original_data = "data"

        file_io.write(context, original_data)
        result = file_io.read(context)
        assert result == original_data


class TestMultipleIOs:
    """Tests for using multiple IO destinations."""

    def test_multiple_io_dict(self, tmp_path):
        """Test asset with multiple IOs as dict."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }

        @il.asset(io=ios, default_io_key="local")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.io == ios
        assert my_asset.default_io_key == "local"

    def test_write_to_all_ios(self, tmp_path):
        """Test that materialize writes to all IOs."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }

        @il.asset(io=ios, default_io_key="local")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        asset_instance = my_asset()
        # Materialize should write to both local and cloud
        asset_instance.materialize()

    def test_read_from_default_io(self, tmp_path):
        """Test that dependencies read from default_io_key."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)
        main_dir = tmp_path / "main"
        main_dir.mkdir(parents=True)

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }

        @il.asset(io=ios, default_io_key="local")
        def upstream(context: il.ExecutionContext) -> str:
            return "a"

        @il.asset(io=il.FileIO(str(main_dir)))
        def downstream(context: il.ExecutionContext, upstream: str) -> str:
            # Should read upstream from "local" IO
            return upstream + "b"

    def test_missing_default_io_key(self, tmp_path):
        """Test that missing default_io_key with dict raises error."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }

        # Should raise error when default_io_key is missing with multiple IOs
        with pytest.raises(Exception):

            @il.asset(io=ios)  # Missing default_io_key
            def my_asset(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

    def test_invalid_default_io_key(self, tmp_path):
        """Test that invalid default_io_key raises error."""
        local_dir = tmp_path / "local"
        local_dir.mkdir(parents=True)
        cloud_dir = tmp_path / "cloud"
        cloud_dir.mkdir(parents=True)

        ios = {
            "local": il.FileIO(str(local_dir)),
            "cloud": il.FileIO(str(cloud_dir)),
        }

        # Should raise error when default_io_key doesn't match any key
        with pytest.raises(Exception):

            @il.asset(io=ios, default_io_key="invalid")
            def my_asset(context: il.ExecutionContext) -> list[dict]:
                return [{"value": 1}]

