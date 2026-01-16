"""Tests for Runner base class."""

import pytest

import interloper as il


class TestRunner:
    """Tests for Runner base class."""

    def test_runner_is_abstract(self):
        """Test that Runner is an abstract base class."""
        # Runner should not be directly instantiable
        with pytest.raises(TypeError):
            il.Runner()

