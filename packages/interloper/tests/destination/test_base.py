"""Tests for Destination base class."""

import pytest

import interloper as il


class TestDestination:
    """Tests for Destination base class."""

    def test_destination_is_abstract(self):
        """Test that Destination is an abstract base class."""
        # Destination should not be directly instantiable
        with pytest.raises(TypeError):
            il.Destination()

    def test_singleton_pattern(self):
        """Test singleton pattern for Destination subclasses."""
        instance1 = il.MemoryDestination.singleton()
        instance2 = il.MemoryDestination.singleton()
        assert instance1 is instance2
