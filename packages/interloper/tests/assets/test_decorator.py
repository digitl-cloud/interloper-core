"""Tests for @asset decorator."""

import datetime as dt

from pydantic import BaseModel
from pydantic_settings import BaseSettings

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int
    name: str


class SampleConfig(BaseSettings):
    """Sample config."""

    api_key: str = "test"


class TestAssetDecorator:
    """Tests for @asset decorator."""

    def test_decorator_without_parentheses(self):
        """Test @asset decorator without parentheses."""

        @il.asset
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.name == "my_asset"
        assert my_asset.func is not None

    def test_decorator_with_empty_parentheses(self):
        """Test @asset decorator with empty parentheses."""

        @il.asset()
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.name == "my_asset"

    def test_decorator_with_name(self):
        """Test @asset decorator with custom name."""

        @il.asset(name="custom_name")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.name == "custom_name"

    def test_decorator_with_deps(self):
        """Test @asset decorator with deps parameter."""

        @il.asset(deps={"param1": "dataset1.asset1", "param2": "dataset2.asset2"})
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.deps == {"param1": "dataset1.asset1", "param2": "dataset2.asset2"}

    def test_decorator_with_deps_none(self):
        """Test @asset decorator with deps=None."""

        @il.asset(deps=None)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.deps == {}

    def test_decorator_with_schema(self):
        """Test @asset decorator with schema."""

        @il.asset(schema=SampleSchema)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1, "name": "test"}]

        assert my_asset.schema == SampleSchema

    def test_decorator_with_config(self):
        """Test @asset decorator with config."""

        @il.asset(config=SampleConfig)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.config == SampleConfig

    def test_decorator_with_io(self, tmp_path):
        """Test @asset decorator with IO."""
        io = il.FileIO(tmp_path)

        @il.asset(io=io)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.io == io

    def test_decorator_with_multiple_ios(self, tmp_path):
        """Test @asset decorator with multiple IOs."""
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

    def test_decorator_with_partitioning(self):
        """Test @asset decorator with partitioning."""
        partitioning = il.TimePartitionConfig(column="date")

        @il.asset(partitioning=partitioning)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": dt.date.today()}]

        assert my_asset.partitioning == partitioning

    def test_decorator_with_dataset(self):
        """Test @asset decorator with dataset."""

        @il.asset(dataset="my_dataset")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.dataset == "my_dataset"

    def test_decorator_with_all_parameters(self, tmp_path):
        """Test @asset decorator with all parameters."""
        io = il.FileIO(tmp_path)
        partitioning = il.TimePartitionConfig(column="date")

        @il.asset(
            name="custom",
            schema=SampleSchema,
            config=SampleConfig,
            io=io,
            partitioning=partitioning,
            dataset="data",
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1, "name": "test", "date": dt.date.today()}]

        assert my_asset.name == "custom"
        assert my_asset.schema == SampleSchema
        assert my_asset.config == SampleConfig
        assert my_asset.io == io
        assert my_asset.partitioning == partitioning
        assert my_asset.dataset == "data"

