"""Tests for the rich terminal display helpers and state management."""

from __future__ import annotations

import datetime as dt

import pytest

import interloper as il
from interloper.assets.keys import AssetInstanceKey
from interloper.cli.display import (
    MODE_BACKFILL,
    MODE_RUN,
    PHASE_EXECUTING,
    PHASE_READING,
    PHASE_WRITING,
    AssetState,
    PartitionRun,
    RichView,
    Status,
    _event_type_style,
    _fit_column,
    _fmt_io,
    _fmt_time,
    _fmt_ts,
    _partition_style,
    _render_ops,
    _short_id,
    _status_text,
)
from interloper.events.base import Event, EventType
from interloper.partitioning.time import TimePartitionWindow

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ts():
    """A fixed UTC timestamp for deterministic tests.

    Returns:
        UTC datetime fixture.
    """
    return dt.datetime(2025, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def simple_dag():
    """A minimal DAG with a single materializable asset.

    Returns:
        DAG fixture with one asset.
    """

    @il.asset
    def my_asset(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 1}]

    return il.DAG(my_asset(io=il.MemoryIO()))


@pytest.fixture
def view(simple_dag):
    """A RichView backed by the simple DAG (never started).

    Returns:
        RichView fixture.
    """
    return RichView(simple_dag)


def _make_event(
    event_type: EventType,
    ts: dt.datetime,
    *,
    run_id: str = "run-1",
    asset_key: str | None = None,
    partition_or_window: str | None = None,
    extra: dict | None = None,
) -> Event:
    """Build an Event with common metadata fields.

    Returns:
        Event with the given type and metadata.
    """
    metadata: dict = {"run_id": run_id}
    if partition_or_window is not None:
        metadata["partition_or_window"] = partition_or_window
    if asset_key is not None:
        metadata["asset_key"] = asset_key
    if extra:
        metadata.update(extra)
    return Event(type=event_type, timestamp=ts, metadata=metadata)


# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------


class TestStatus:
    """Verify Status enum values."""

    def test_waiting(self):
        assert Status.WAITING.value == "waiting"

    def test_running(self):
        assert Status.RUNNING.value == "running"

    def test_done(self):
        assert Status.DONE.value == "done"

    def test_failed(self):
        assert Status.FAILED.value == "failed"


# ---------------------------------------------------------------------------
# AssetState.elapsed
# ---------------------------------------------------------------------------


class TestAssetState:
    """Verify elapsed property on AssetState."""

    def test_elapsed_no_start(self):
        """None when start_time is unset."""
        state = AssetState(key=AssetInstanceKey("a"), name="a")
        assert state.elapsed is None

    def test_elapsed_with_start_and_end(self, ts):
        """Returns difference in seconds when both are set."""
        state = AssetState(
            key=AssetInstanceKey("a"),
            name="a",
            start_time=ts,
            end_time=ts + dt.timedelta(seconds=5),
        )
        assert state.elapsed == pytest.approx(5.0)

    def test_elapsed_with_start_only(self, ts):
        """Returns a positive number when only start is set (uses now)."""
        state = AssetState(
            key=AssetInstanceKey("a"),
            name="a",
            start_time=ts,
        )
        assert state.elapsed is not None
        assert state.elapsed > 0


# ---------------------------------------------------------------------------
# PartitionRun.elapsed
# ---------------------------------------------------------------------------


class TestPartitionRun:
    """Verify elapsed property on PartitionRun."""

    def test_elapsed_no_start(self):
        """None when start_time is unset."""
        run = PartitionRun()
        assert run.elapsed is None

    def test_elapsed_with_start_and_end(self, ts):
        """Returns difference in seconds when both are set."""
        run = PartitionRun(
            start_time=ts,
            end_time=ts + dt.timedelta(seconds=90),
        )
        assert run.elapsed == pytest.approx(90.0)

    def test_elapsed_with_start_only(self, ts):
        """Returns a positive number when only start is set (uses now)."""
        run = PartitionRun(start_time=ts)
        assert run.elapsed is not None
        assert run.elapsed > 0


# ---------------------------------------------------------------------------
# _fmt_ts
# ---------------------------------------------------------------------------


class TestFmtTs:
    """Verify timestamp formatting."""

    def test_none_returns_spaces(self):
        """None produces an 8-character blank string."""
        result = _fmt_ts(None)
        assert result == "        "
        assert len(result) == 8

    def test_valid_timestamp(self, ts):
        """A valid timestamp produces HH:MM:SS.mmm in local time."""
        result = _fmt_ts(ts)
        # The format is HH:MM:SS.mmm  (12 chars)
        assert len(result) == 12
        assert result.endswith(".000")


# ---------------------------------------------------------------------------
# _fmt_time
# ---------------------------------------------------------------------------


class TestFmtTime:
    """Verify human-readable duration formatting."""

    def test_none_returns_dash(self):
        assert _fmt_time(None) == "-"

    def test_less_than_60(self):
        assert _fmt_time(3.14) == "3.1s"

    def test_zero_seconds(self):
        assert _fmt_time(0.0) == "0.0s"

    def test_exactly_60(self):
        assert _fmt_time(60.0) == "1m00.0s"

    def test_over_60(self):
        assert _fmt_time(125.5) == "2m05.5s"


# ---------------------------------------------------------------------------
# _fmt_io
# ---------------------------------------------------------------------------


class TestFmtIo:
    """Verify IO count formatting."""

    def test_no_counts_returns_dash(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a")
        assert _fmt_io(state) == "-"

    def test_reads_only(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", io_reads=3)
        assert _fmt_io(state) == "R:3"

    def test_writes_only(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", io_writes=2)
        assert _fmt_io(state) == "W:2"

    def test_errors_only(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", io_errors=1)
        assert _fmt_io(state) == "E:1"

    def test_reads_writes_errors(self):
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", io_reads=5, io_writes=3, io_errors=1
        )
        assert _fmt_io(state) == "R:5 W:3 E:1"


# ---------------------------------------------------------------------------
# _short_id
# ---------------------------------------------------------------------------


class TestShortId:
    """Verify ID truncation."""

    def test_none_returns_empty(self):
        assert _short_id(None) == ""

    def test_short_string_unchanged(self):
        assert _short_id("abc") == "abc"

    def test_twelve_char_string_unchanged(self):
        assert _short_id("123456789012") == "123456789012"

    def test_long_string_truncated_to_8(self):
        assert _short_id("1234567890123") == "12345678"


# ---------------------------------------------------------------------------
# _fit_column
# ---------------------------------------------------------------------------


class TestFitColumn:
    """Verify column padding and truncation."""

    def test_shorter_than_width_padded(self):
        result = _fit_column("hi", 6)
        assert result == "hi    "
        assert len(result) == 6

    def test_exact_width(self):
        result = _fit_column("hello", 5)
        assert result == "hello"

    def test_longer_truncated_with_ellipsis(self):
        result = _fit_column("hello world", 6)
        assert result == "hello\u2026"
        assert len(result) == 6

    def test_width_1(self):
        result = _fit_column("hello", 1)
        assert result == "h"


# ---------------------------------------------------------------------------
# _event_type_style
# ---------------------------------------------------------------------------


class TestEventTypeStyle:
    """Verify event type color styles."""

    def test_failed_is_bold_red(self):
        assert _event_type_style("RUN_FAILED") == "bold red"
        assert _event_type_style("ASSET_EXEC_FAILED") == "bold red"

    def test_completed_is_green(self):
        assert _event_type_style("RUN_COMPLETED") == "green"
        assert _event_type_style("IO_WRITE_COMPLETED") == "green"

    def test_started_is_cyan(self):
        assert _event_type_style("RUN_STARTED") == "cyan"
        assert _event_type_style("BACKFILL_STARTED") == "cyan"

    def test_other_is_bold(self):
        assert _event_type_style("LOG") == "bold"


# ---------------------------------------------------------------------------
# _partition_style
# ---------------------------------------------------------------------------


class TestPartitionStyle:
    """Verify partition display styles."""

    def test_none_is_dim(self):
        assert _partition_style(None) == "dim"

    def test_waiting_is_dim(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.WAITING)
        assert _partition_style(state) == "dim"

    def test_done_is_bold_green(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.DONE)
        assert _partition_style(state) == "bold green"

    def test_failed_is_bold_red(self):
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.FAILED)
        assert _partition_style(state) == "bold red"

    def test_running_with_reading_phase(self):
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_READING
        )
        assert _partition_style(state) == "cyan"

    def test_running_with_executing_phase(self):
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_EXECUTING
        )
        assert _partition_style(state) == "yellow"

    def test_running_with_writing_phase(self):
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_WRITING
        )
        assert _partition_style(state) == "magenta"

    def test_running_no_phase_falls_back_to_yellow(self):
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=None
        )
        assert _partition_style(state) == "yellow"


# ---------------------------------------------------------------------------
# RichView state management
# ---------------------------------------------------------------------------


class TestRichViewStateUpdate:
    """Verify RichView._update_state without rendering."""

    def test_run_started_creates_run(self, view, ts):
        """RUN_STARTED sets mode to 'run' and creates a partition run."""
        event = _make_event(EventType.RUN_STARTED, ts)
        view._update_state(event)

        assert view._mode == "run"
        assert len(view._partition_runs) == 1
        assert view._partition_runs[0].status == Status.RUNNING
        assert view._partition_runs[0].run_id == "run-1"
        assert view._partition_runs[0].start_time == ts

    def test_run_started_seeds_assets(self, view, ts):
        """RUN_STARTED pre-populates asset states from the DAG."""
        event = _make_event(EventType.RUN_STARTED, ts)
        view._update_state(event)

        run = view._partition_runs[0]
        assert len(run.assets) == 1  # single-asset DAG
        asset_state = next(iter(run.assets.values()))
        assert asset_state.name == "my_asset"
        assert asset_state.status == Status.WAITING

    def test_asset_started_updates_state(self, view, ts):
        """ASSET_STARTED transitions asset to RUNNING."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.status == Status.RUNNING
        assert asset.start_time == ts

    def test_asset_completed_updates_state(self, view, ts):
        """ASSET_COMPLETED transitions asset to DONE."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )

        end_ts = ts + dt.timedelta(seconds=2)
        view._update_state(
            _make_event(EventType.ASSET_COMPLETED, end_ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.status == Status.DONE
        assert asset.end_time == end_ts
        assert asset.op_read == "done"
        assert asset.op_exec == "done"
        assert asset.op_write == "done"

    def test_asset_failed_updates_state(self, view, ts):
        """ASSET_FAILED transitions asset to FAILED and records error."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )

        fail_ts = ts + dt.timedelta(seconds=1)
        view._update_state(
            _make_event(
                EventType.ASSET_FAILED,
                fail_ts,
                asset_key=asset_key,
                extra={"error": "boom"},
            )
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.status == Status.FAILED
        assert asset.error == "boom"
        assert asset.end_time == fail_ts

    def test_io_read_completed_increments_counter(self, view, ts):
        """IO_READ_COMPLETED increments io_reads on the asset."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_READ_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_READ_COMPLETED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.io_reads == 1

    def test_io_write_completed_increments_counter(self, view, ts):
        """IO_WRITE_COMPLETED increments io_writes on the asset."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_WRITE_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_WRITE_COMPLETED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.io_writes == 1

    def test_io_read_failed_increments_errors(self, view, ts):
        """IO_READ_FAILED increments io_errors on the asset."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_READ_FAILED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.io_errors == 1

    def test_io_write_failed_increments_errors(self, view, ts):
        """IO_WRITE_FAILED increments io_errors on the asset."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_WRITE_FAILED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.io_errors == 1

    def test_backfill_started_sets_mode(self, view, ts):
        """BACKFILL_STARTED sets mode to 'backfill'."""
        event = Event(
            type=EventType.BACKFILL_STARTED,
            timestamp=ts,
            metadata={"backfill_id": "bf-1"},
        )
        view._update_state(event)

        assert view._mode == "backfill"
        assert view._backfill_id == "bf-1"
        assert view._backfill_start == ts

    def test_run_completed_updates_run(self, view, ts):
        """RUN_COMPLETED transitions the run to DONE."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        end_ts = ts + dt.timedelta(seconds=10)
        view._update_state(_make_event(EventType.RUN_COMPLETED, end_ts))

        run = view._partition_runs[0]
        assert run.status == Status.DONE
        assert run.end_time == end_ts

    def test_run_failed_updates_run(self, view, ts):
        """RUN_FAILED transitions the run to FAILED and records error."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        fail_ts = ts + dt.timedelta(seconds=5)
        view._update_state(
            _make_event(EventType.RUN_FAILED, fail_ts, extra={"error": "run error"})
        )

        run = view._partition_runs[0]
        assert run.status == Status.FAILED
        assert run.error == "run error"
        assert run.end_time == fail_ts

    def test_asset_exec_started_sets_phase(self, view, ts):
        """ASSET_EXEC_STARTED sets phase to PHASE_EXECUTING and marks read done."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.ASSET_EXEC_STARTED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.phase == PHASE_EXECUTING
        assert asset.op_read == "done"

    def test_io_read_started_sets_phase(self, view, ts):
        """IO_READ_STARTED sets phase to PHASE_READING."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_READ_STARTED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.phase == PHASE_READING

    def test_io_write_started_sets_phase(self, view, ts):
        """IO_WRITE_STARTED sets phase to PHASE_WRITING."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(
            _make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key)
        )
        view._update_state(
            _make_event(EventType.IO_WRITE_STARTED, ts, asset_key=asset_key)
        )

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.phase == PHASE_WRITING

    def test_get_or_create_run_reuses_existing(self, view, ts):
        """_get_or_create_run returns the same run for the same run_id."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        view._update_state(_make_event(EventType.RUN_STARTED, ts, run_id="run-2"))

        assert len(view._partition_runs) == 2
        assert view._find_run("run-1") is not None
        assert view._find_run("run-2") is not None
        assert view._find_run("run-1") is not view._find_run("run-2")

    def test_find_run_returns_none_for_unknown(self, view):
        """_find_run returns None for an unregistered run_id."""
        assert view._find_run("nonexistent") is None

    def test_find_asset_creates_if_absent(self, view, ts):
        """_find_asset creates a new AssetState when asset_key is unknown."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))

        metadata = {"run_id": "run-1", "asset_key": "unknown_asset", "asset_name": "Unknown"}
        asset = view._find_asset(metadata)

        assert asset is not None
        assert asset.name == "Unknown"
        assert asset.key == AssetInstanceKey("unknown_asset")

    def test_backfill_completed_updates_state(self, view, ts):
        """BACKFILL_COMPLETED sets end time."""
        view._update_state(Event(type=EventType.BACKFILL_STARTED, timestamp=ts, metadata={"backfill_id": "bf-1"}))
        end_ts = ts + dt.timedelta(seconds=30)
        view._update_state(Event(type=EventType.BACKFILL_COMPLETED, timestamp=end_ts, metadata={}))

        assert view._backfill_end == end_ts

    def test_backfill_failed_updates_state(self, view, ts):
        """BACKFILL_FAILED records error and end time."""
        view._update_state(Event(type=EventType.BACKFILL_STARTED, timestamp=ts, metadata={"backfill_id": "bf-1"}))
        fail_ts = ts + dt.timedelta(seconds=5)
        view._update_state(Event(type=EventType.BACKFILL_FAILED, timestamp=fail_ts, metadata={"error": "bf error"}))

        assert view._backfill_end == fail_ts
        assert view._backfill_error == "bf error"

    def test_asset_exec_completed_marks_done(self, view, ts):
        """ASSET_EXEC_COMPLETED marks op_exec done and clears phase."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(_make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key))
        view._update_state(_make_event(EventType.ASSET_EXEC_STARTED, ts, asset_key=asset_key))
        view._update_state(_make_event(EventType.ASSET_EXEC_COMPLETED, ts, asset_key=asset_key))

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.op_exec == "done"
        assert asset.phase is None

    def test_asset_exec_failed_clears_phase(self, view, ts):
        """ASSET_EXEC_FAILED clears phase."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        asset_key = str(next(iter(view._partition_runs[0].assets.keys())))
        view._update_state(_make_event(EventType.ASSET_STARTED, ts, asset_key=asset_key))
        view._update_state(_make_event(EventType.ASSET_EXEC_STARTED, ts, asset_key=asset_key))
        view._update_state(_make_event(EventType.ASSET_EXEC_FAILED, ts, asset_key=asset_key))

        asset = next(iter(view._partition_runs[0].assets.values()))
        assert asset.phase is None

    def test_active_run_returns_current(self, view, ts):
        """_active_run returns the most recently started run."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        run = view._active_run()
        assert run is not None
        assert run.run_id == "run-1"

    def test_active_run_returns_none_when_empty(self, view):
        """_active_run returns None before any run starts."""
        assert view._active_run() is None

    def test_find_asset_readonly_returns_none(self, view, ts):
        """_find_asset_readonly returns None for unknown asset."""
        view._update_state(_make_event(EventType.RUN_STARTED, ts))
        result = view._find_asset_readonly({"run_id": "run-1", "asset_key": "missing"})
        assert result is None

    def test_render_dispatches_to_run(self, ts):
        """_render returns run view when mode is 'run'."""

        @il.source(dataset="src")
        class RenderSrc:
            @il.asset
            def a(self, context: il.ExecutionContext) -> str:
                return "v"

        src = RenderSrc(io=il.MemoryIO())
        dag = il.DAG(*src.assets.values())
        v = RichView(dag)
        v._update_state(_make_event(EventType.RUN_STARTED, ts))
        assert v._mode == MODE_RUN
        result = v._render()
        assert result is not None

    def test_render_returns_starting_when_no_mode(self, view):
        """_render returns 'Starting...' when no mode is set."""
        result = view._render()
        assert "Starting" in str(result)


# ---------------------------------------------------------------------------
# _status_text rendering helper
# ---------------------------------------------------------------------------


class TestStatusText:
    """Verify _status_text rendering."""

    def test_waiting(self):
        """WAITING status shows wait label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.WAITING)
        text = _status_text(state)
        assert "wait" in text.plain

    def test_running_no_phase(self):
        """RUNNING status without phase shows run label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.RUNNING)
        text = _status_text(state)
        assert "run" in text.plain

    def test_running_with_reading_phase(self):
        """RUNNING with reading phase shows read label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_READING)
        text = _status_text(state)
        assert "read" in text.plain

    def test_running_with_executing_phase(self):
        """RUNNING with executing phase shows exec label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_EXECUTING)
        text = _status_text(state)
        assert "exec" in text.plain

    def test_running_with_writing_phase(self):
        """RUNNING with writing phase shows write label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_WRITING)
        text = _status_text(state)
        assert "write" in text.plain

    def test_done(self):
        """DONE status shows done label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.DONE)
        text = _status_text(state)
        assert "done" in text.plain

    def test_failed(self):
        """FAILED status shows FAIL label."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.FAILED)
        text = _status_text(state)
        assert "FAIL" in text.plain


# ---------------------------------------------------------------------------
# _render_ops rendering helper
# ---------------------------------------------------------------------------


class TestRenderOps:
    """Verify _render_ops pipeline visualization."""

    def test_all_pending(self):
        """All operations pending shows 3 chars."""
        state = AssetState(key=AssetInstanceKey("a"), name="a", status=Status.RUNNING)
        ops = _render_ops(state)
        assert len(ops.plain) == 3

    def test_all_done(self):
        """All operations done shows 3 green chars."""
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.DONE,
            op_read="done", op_exec="done", op_write="done",
        )
        ops = _render_ops(state)
        assert len(ops.plain) == 3

    def test_reading_phase(self):
        """Reading phase colors first char cyan."""
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_READING,
        )
        ops = _render_ops(state)
        assert len(ops.plain) == 3

    def test_executing_phase(self):
        """Executing phase colors middle char yellow."""
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_EXECUTING,
            op_read="done",
        )
        ops = _render_ops(state)
        assert len(ops.plain) == 3

    def test_writing_phase(self):
        """Writing phase colors last char magenta."""
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.RUNNING, phase=PHASE_WRITING,
            op_read="done", op_exec="done",
        )
        ops = _render_ops(state)
        assert len(ops.plain) == 3

    def test_failed_with_read_done(self):
        """Failed after read shows red exec char."""
        state = AssetState(
            key=AssetInstanceKey("a"), name="a", status=Status.FAILED,
            op_read="done",
        )
        ops = _render_ops(state)
        assert len(ops.plain) == 3


# ---------------------------------------------------------------------------
# RichView with backfill context
# ---------------------------------------------------------------------------


class TestRichViewBackfill:
    """Verify RichView with partition window for backfill mode."""

    @staticmethod
    def _sourced_dag():
        """Build a DAG with a sourced partitioned asset.

        Returns:
            DAG with a partitioned asset.
        """

        @il.source(dataset="bf")
        class BfSrc:
            @il.asset(partitioning=il.TimePartitionConfig(column="date"))
            def my_asset(self, context: il.ExecutionContext) -> list[dict]:
                return [{"date": context.partition_date, "v": 1}]

        return il.DAG(*BfSrc(io=il.MemoryIO()).assets.values())

    def test_partition_window_pre_populates_runs(self, ts):
        """PartitionWindow pre-populates partition runs."""
        dag = self._sourced_dag()
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))
        view = RichView(dag, partition_or_window=window)

        assert len(view._partition_runs) == 2
        assert view._partition_runs[0].label is not None
        assert view._partition_runs[1].label is not None

    def test_backfill_mode_render(self, ts):
        """Backfill mode renders without error."""
        dag = self._sourced_dag()
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))
        view = RichView(dag, partition_or_window=window)

        view._update_state(Event(
            type=EventType.BACKFILL_STARTED, timestamp=ts,
            metadata={"backfill_id": "bf-1"},
        ))
        assert view._mode == MODE_BACKFILL
        result = view._render()
        assert result is not None
