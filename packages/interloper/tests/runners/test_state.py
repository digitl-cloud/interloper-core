"""Tests for runner state."""


from interloper.runners.state import RunState


class TestRunState:
    """Tests for RunState."""

    def test_initialization(self, dag):
        """Test RunState initialization."""
        state = RunState(dag=dag)
        assert state.dag == dag
        assert len(state.asset_executions) == len(dag.assets)

