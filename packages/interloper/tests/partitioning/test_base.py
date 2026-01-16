"""Tests for PartitionConfig base class."""

import interloper as il


class TestPartitionConfig:
    """Tests for PartitionConfig base class."""

    def test_initialization(self):
        """Test PartitionConfig initialization."""
        config = il.PartitionConfig(column="my_column")
        assert config.column == "my_column"

    def test_column_attribute(self):
        """Test column attribute."""
        config = il.PartitionConfig(column="test_col")
        assert config.column == "test_col"

