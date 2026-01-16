"""Tests for backfiller state."""

import datetime as dt

import interloper as il
from interloper.backfillers.state import BackfillState


class TestBackfillState:
    """Tests for BackfillState."""

    def test_initialization(self):
        """Test BackfillState initialization."""
        partitions = [
            il.TimePartition(dt.date(2025, 1, 1)),
            il.TimePartition(dt.date(2025, 1, 2)),
        ]
        state = BackfillState(partitions=partitions)
        assert len(state.run_executions) == 2
        assert state.start_time is None
        assert state.end_time is None

