"""Tests for IO base class."""

import pytest

import interloper as il


class TestIO:
    """Tests for IO base class."""

    def test_io_is_abstract(self):
        """Test that IO is an abstract base class."""
        # IO should not be directly instantiable
        with pytest.raises(TypeError):
            il.IO()

    def test_singleton_pattern(self):
        """Test singleton pattern for IO subclasses."""
        instance1 = il.MemoryIO.singleton()
        instance2 = il.MemoryIO.singleton()
        assert instance1 is instance2

