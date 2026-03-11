"""Tests for DestinationContext."""

import datetime as dt

from pydantic import BaseModel

import interloper as il


class SampleSchema(BaseModel):
    """Sample schema."""

    value: int


class TestDestinationContext:
    """Tests for DestinationContext."""

    def test_initialization(self):
        """Test DestinationContext initialization."""

        @il.asset
        def my_asset():
            return "x"

        asset = my_asset()
        context = il.DestinationContext(asset=asset)
        assert context.asset == asset
        assert context.partition_or_window is None

    def test_with_partition(self):
        """Test DestinationContext with partition."""

        @il.asset
        def my_asset():
            return "x"

        asset = my_asset()
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(asset=asset, partition_or_window=partition)
        assert context.partition_or_window == partition

    def test_with_schema(self):
        """Test DestinationContext with schema."""

        @il.asset
        def my_asset() -> SampleSchema:
            return SampleSchema(value=1)

        asset = my_asset()
        context = il.DestinationContext(asset=asset)
        assert context.asset == asset

    def test_all_parameters(self):
        """Test DestinationContext with all parameters."""

        @il.asset
        def my_asset():
            return "x"

        asset = my_asset()
        partition = il.TimePartition(dt.date(2025, 1, 1))
        context = il.DestinationContext(
            asset=asset,
            partition_or_window=partition,
        )
        assert context.asset == asset
        assert context.partition_or_window == partition
