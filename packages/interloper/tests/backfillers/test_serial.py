"""Tests for SerialBackfiller."""

import interloper as il


class TestSerialBackfiller:
    """Tests for SerialBackfiller."""

    def test_initialization(self):
        """Test SerialBackfiller initialization."""
        backfiller = il.SerialBackfiller()
        assert isinstance(backfiller, il.SerialBackfiller)
        assert isinstance(backfiller, il.Backfiller)

