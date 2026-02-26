"""Tests for @asset decorator."""

import datetime as dt

from pydantic import BaseModel

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int
    name: str


class SampleConfig(il.Config):
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

    def test_decorator_with_requires(self):
        """Test @asset decorator with requires parameter."""

        @il.asset(requires={"campaign": "facebook_ads:campaign", "display": "amazon_ads:display"})
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.requires == {"campaign": "facebook_ads:campaign", "display": "amazon_ads:display"}

    def test_decorator_with_requires_none(self):
        """Test @asset decorator with requires=None."""

        @il.asset(requires=None)
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.requires == {}

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

    def test_decorator_with_partitioning(self):
        """Test @asset decorator with partitioning."""
        partitioning = il.TimePartitionConfig(column="date")

        @il.asset(partitioning=partitioning)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"date": dt.date(2025, 1, 1)}]

        assert my_asset.partitioning == partitioning

    def test_decorator_with_dataset(self):
        """Test @asset decorator with dataset."""

        @il.asset(dataset="my_dataset")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.dataset == "my_dataset"

    def test_decorator_with_all_parameters(self):
        """Test @asset decorator with all parameters."""
        partitioning = il.TimePartitionConfig(column="date")

        @il.asset(
            name="custom",
            schema=SampleSchema,
            config=SampleConfig,
            partitioning=partitioning,
            dataset="data",
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1, "name": "test", "date": dt.date(2025, 1, 1)}]

        assert my_asset.name == "custom"
        assert my_asset.schema == SampleSchema
        assert my_asset.config == SampleConfig
        assert my_asset.partitioning == partitioning
        assert my_asset.dataset == "data"
