"""Tests for @asset decorator."""

import datetime as dt
import warnings

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
        assert my_asset.key == "my_asset"
        assert my_asset.func is not None

    def test_decorator_with_empty_parentheses(self):
        """Test @asset decorator with empty parentheses."""

        @il.asset()
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert isinstance(my_asset, il.AssetDefinition)
        assert my_asset.key == "my_asset"

    def test_decorator_with_key(self):
        """Test @asset decorator with custom key."""

        @il.asset(key="custom_name")
        def my_asset(context: il.ExecutionContext) -> str:
            return "value"

        assert my_asset.key == "custom_name"

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
            key="custom",
            schema=SampleSchema,
            config=SampleConfig,
            partitioning=partitioning,
            dataset="data",
        )
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return [{"value": 1, "name": "test", "date": dt.date(2025, 1, 1)}]

        assert my_asset.key == "custom"
        assert my_asset.schema == SampleSchema
        assert my_asset.config == SampleConfig
        assert my_asset.partitioning == partitioning
        assert my_asset.dataset == "data"


class TestPartitionColumnSchemaWarning:
    """Warn at instantiation when partition column is missing from the asset schema."""

    def test_warns_when_partition_column_not_in_schema(self):
        """A warning is emitted when the asset is instantiated with a missing partition column."""

        class MySchema(BaseModel):
            value: int

        @il.asset(schema=MySchema, partitioning=il.PartitionConfig(column="date"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            my_asset()  # instantiate Asset from AssetDefinition

        assert len(w) == 1
        assert "partition column 'date'" in str(w[0].message)
        assert "['value']" in str(w[0].message)

    def test_no_warning_when_partition_column_in_schema(self):
        """No warning when the partition column is present in the schema."""

        class MySchema(BaseModel):
            date: str
            value: int

        @il.asset(schema=MySchema, partitioning=il.PartitionConfig(column="date"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            my_asset()

        assert len(w) == 0

    def test_no_warning_when_no_schema(self):
        """No warning when schema is None (can't validate)."""

        @il.asset(partitioning=il.PartitionConfig(column="date"))
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            my_asset()

        assert len(w) == 0

    def test_no_warning_when_no_partitioning(self):
        """No warning when partitioning is None."""

        class MySchema(BaseModel):
            value: int

        @il.asset(schema=MySchema)
        def my_asset(context: il.ExecutionContext) -> list[dict]:
            return []

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            my_asset()

        assert len(w) == 0

    def test_no_warning_at_definition_time(self):
        """No warning is emitted when the @asset decorator is applied (only at instantiation)."""

        class MySchema(BaseModel):
            value: int

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            @il.asset(schema=MySchema, partitioning=il.PartitionConfig(column="date"))
            def my_asset(context: il.ExecutionContext) -> list[dict]:
                return []

        assert len(w) == 0
