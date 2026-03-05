"""Tests for DBOSRunner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from interloper.errors import RunnerError
from interloper.serialization.runner import RunnerSpec

from interloper_dbos.runner import DBOSRunner


@pytest.fixture
def mock_dbos():
    """Patch DBOS internals to avoid global registry conflicts."""
    with (
        patch("interloper_dbos.runner.DBOSConfiguredInstance.__init__", return_value=None),
        patch("interloper_dbos.runner.Queue"),
    ):
        yield


@pytest.fixture
def runner(mock_dbos):
    """A DBOSRunner with default settings."""
    return DBOSRunner()


@pytest.fixture
def runner_custom(mock_dbos):
    """A DBOSRunner with custom concurrency."""
    return DBOSRunner(concurrency=25)


@pytest.fixture
def runner_with_event(mock_dbos):
    """A DBOSRunner with an on_event callback."""
    handler = MagicMock()
    return DBOSRunner(concurrency=3, on_event=handler), handler


class TestDBOSRunnerInit:
    """Tests for DBOSRunner initialization."""

    def test_default_concurrency(self, runner):
        """Default concurrency is 10."""
        assert runner._concurrency == 10

    def test_custom_concurrency(self, runner_custom):
        """Custom concurrency is stored correctly."""
        assert runner_custom._concurrency == 25

    def test_handle_initially_none(self, runner):
        """Handle is None before any workflow execution."""
        assert runner.handle is None

    def test_queue_created(self, runner):
        """An internal queue is created during init."""
        assert runner._queue is not None

    def test_inherits_runner_defaults(self, runner):
        """Runner base class defaults are applied (fail_fast=False, reraise=True)."""
        assert runner._fail_fast is False
        assert runner._reraise is True

    def test_on_event_none_by_default(self, runner):
        """No event handler is registered by default."""
        assert runner._on_event is None

    def test_on_event_with_callback(self, runner_with_event):
        """Event handler is registered when provided."""
        runner, handler = runner_with_event
        assert runner._on_event is not None


class TestDBOSRunnerCapacity:
    """Tests for the _capacity property."""

    def test_capacity_matches_concurrency(self, runner):
        """Capacity returns the concurrency value."""
        assert runner._capacity == 10

    def test_capacity_matches_custom_concurrency(self, runner_custom):
        """Capacity returns the custom concurrency value."""
        assert runner_custom._capacity == 25

    def test_capacity_matches_concurrency_one(self, mock_dbos):
        """Capacity returns 1 when concurrency is 1."""
        runner = DBOSRunner(concurrency=1)
        assert runner._capacity == 1


class TestDBOSRunnerToSpec:
    """Tests for to_spec() serialization."""

    def test_returns_runner_spec(self, runner):
        """to_spec() returns a RunnerSpec instance."""
        spec = runner.to_spec()
        assert isinstance(spec, RunnerSpec)

    def test_spec_path(self, runner):
        """Spec path is 'dbos'."""
        spec = runner.to_spec()
        assert spec.path == "dbos"

    def test_spec_init_default_concurrency(self, runner):
        """Spec init dict contains default concurrency."""
        spec = runner.to_spec()
        assert spec.init == {"concurrency": 10}

    def test_spec_init_custom_concurrency(self, runner_custom):
        """Spec init dict contains custom concurrency."""
        spec = runner_custom.to_spec()
        assert spec.init == {"concurrency": 25}

    def test_spec_json_roundtrip(self, runner):
        """Spec can be serialized to JSON and deserialized back."""
        spec = runner.to_spec()
        json_str = spec.model_dump_json()
        restored = RunnerSpec.model_validate_json(json_str)
        assert restored.path == spec.path
        assert restored.init == spec.init

    def test_spec_dict_roundtrip(self, runner_custom):
        """Spec can be converted to dict and back."""
        spec = runner_custom.to_spec()
        data = spec.model_dump()
        restored = RunnerSpec.model_validate(data)
        assert restored.path == "dbos"
        assert restored.init == {"concurrency": 25}

    def test_spec_only_contains_concurrency(self, runner_with_event):
        """Spec init dict does not contain on_event (it is not serializable)."""
        runner, _ = runner_with_event
        spec = runner.to_spec()
        assert "on_event" not in spec.init
        assert list(spec.init.keys()) == ["concurrency"]


class TestDBOSRunnerHandle:
    """Tests for the handle property."""

    def test_handle_is_none_initially(self, runner):
        """Handle is None before run() is called."""
        assert runner.handle is None


class TestDBOSRunnerListSteps:
    """Tests for list_steps error handling."""

    def test_list_steps_raises_without_handle(self, runner):
        """list_steps raises RunnerError when handle is not set."""
        with pytest.raises(RunnerError, match="Workflow handle is not set"):
            runner.list_steps()

    def test_list_failed_steps_raises_without_handle(self, runner):
        """list_failed_steps raises RunnerError when handle is not set (via list_steps)."""
        with pytest.raises(RunnerError, match="Workflow handle is not set"):
            runner.list_failed_steps()

    def test_get_first_failed_step_raises_without_handle(self, runner):
        """get_first_failed_step raises RunnerError when handle is not set."""
        with pytest.raises(RunnerError, match="Workflow handle is not set"):
            runner.get_first_failed_step()

    def test_list_steps_with_handle(self, runner):
        """list_steps delegates to DBOS.list_workflow_steps when handle is set."""
        mock_handle = MagicMock()
        mock_handle.workflow_id = "wf-123"
        runner._handle = mock_handle

        step_execute = {"function_name": "execute_asset", "child_workflow_id": "child-1"}
        step_other = {"function_name": "other_step", "child_workflow_id": "child-2"}

        with patch("interloper_dbos.runner.DBOS.list_workflow_steps", return_value=[step_execute, step_other]):
            steps = runner.list_steps()

        assert steps == [step_execute]

    def test_list_steps_filters_by_function_name(self, runner):
        """list_steps only returns steps with function_name == 'execute_asset'."""
        mock_handle = MagicMock()
        mock_handle.workflow_id = "wf-456"
        runner._handle = mock_handle

        steps_data = [
            {"function_name": "execute_asset", "child_workflow_id": "c1"},
            {"function_name": "materialize", "child_workflow_id": "c2"},
            {"function_name": "execute_asset", "child_workflow_id": "c3"},
        ]

        with patch("interloper_dbos.runner.DBOS.list_workflow_steps", return_value=steps_data):
            result = runner.list_steps()

        assert len(result) == 2
        assert all(s["function_name"] == "execute_asset" for s in result)


class TestDBOSRunnerCancelAll:
    """Tests for _cancel_all."""

    def test_cancel_all_not_implemented(self, runner):
        """_cancel_all raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Not supported for DBOS runner"):
            runner._cancel_all([])
