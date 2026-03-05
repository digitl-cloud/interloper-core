"""Rich terminal visualization for the Interloper CLI."""

from __future__ import annotations

import datetime as dt
import os
import sys
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TextIO

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.spinner import Spinner
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from interloper.assets.keys import AssetInstanceKey
from interloper.dag.base import DAG
from interloper.events.base import Event, EventType, LogLevel
from interloper.partitioning.base import Partition, PartitionWindow

# ---------------------------------------------------------------------------
# State models
# ---------------------------------------------------------------------------


class Status(str, Enum):
    """Display status for assets and partitions."""

    WAITING = "waiting"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


MODE_RUN = "run"
MODE_BACKFILL = "backfill"

PHASE_READING = "reading"
PHASE_EXECUTING = "executing"
PHASE_WRITING = "writing"


@dataclass
class AssetState:
    """Tracked display state for a single asset execution."""

    key: AssetInstanceKey
    name: str
    source_name: str | None = None
    status: Status = Status.WAITING
    phase: str | None = None  # "reading", "executing", "writing"
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    error: str | None = None
    io_reads: int = 0
    io_writes: int = 0
    io_errors: int = 0

    # Operation tracking — "pending" or "done".
    # When the asset fails, the incomplete op is inferred (pending + failed status = red).
    op_read: str = "pending"
    op_exec: str = "pending"
    op_write: str = "pending"

    @property
    def elapsed(self) -> float | None:
        """Elapsed seconds since start, or total if finished."""
        if self.start_time is None:
            return None
        end = self.end_time or dt.datetime.now(dt.timezone.utc)
        return (end - self.start_time).total_seconds()


@dataclass
class PartitionRun:
    """Tracked display state for a single partition run."""

    label: str | None = None
    run_id: str | None = None
    status: Status = Status.WAITING
    start_time: dt.datetime | None = None
    end_time: dt.datetime | None = None
    assets: dict[AssetInstanceKey, AssetState] = field(default_factory=dict)
    error: str | None = None

    @property
    def elapsed(self) -> float | None:
        """Elapsed seconds for this partition run."""
        if self.start_time is None:
            return None
        end = self.end_time or dt.datetime.now(dt.timezone.utc)
        return (end - self.start_time).total_seconds()


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

# Status labels for run-mode asset lines
_STATUS_LABEL: dict[Status, tuple[str, str]] = {
    Status.WAITING: ("\u00b7 wait", "dim"),
    Status.RUNNING: ("\u25c9 run", "cyan"),
    Status.DONE: ("\u2713 done", "green"),
    Status.FAILED: ("\u2717 FAIL", "bold red"),
}

# Phase-specific labels when status is RUNNING
_PHASE_LABEL: dict[str, tuple[str, str]] = {
    PHASE_READING: ("\u25c9 read", "cyan"),
    PHASE_EXECUTING: ("\u25c9 exec", "yellow"),
    PHASE_WRITING: ("\u25c9 write", "magenta"),
}

# Partition progress strip: same char, different colors per state/phase
_PARTITION_CHAR = "\u25aa"  # ▪

_PHASE_STYLE: dict[str, str] = {
    PHASE_READING: "cyan",
    PHASE_EXECUTING: "yellow",
    PHASE_WRITING: "magenta",
}

_EVENT_TYPE_WIDTH = 22


def _status_text(asset: AssetState) -> Text:
    """Render the status label, with phase detail when running."""
    if asset.status == Status.RUNNING and asset.phase in _PHASE_LABEL:
        label, style = _PHASE_LABEL[asset.phase]
    else:
        label, style = _STATUS_LABEL[asset.status]
    return Text(label, style=style)


def _render_ops(asset: AssetState) -> Text:
    """Render the 3-char operation pipeline: ↓▸↑ (read, exec, write).

    Each char is colored by state:
      dim     = not reached yet
      cyan    = reading in progress
      yellow  = executing in progress
      magenta = writing in progress
      green   = completed
      red     = this is where the failure occurred
    """
    ops = Text()
    is_failed = asset.status == Status.FAILED

    # Read phase
    if asset.phase == PHASE_READING:
        ops.append("\u2193", style="cyan")
    elif asset.op_read == "done":
        ops.append("\u2193", style="green")
    elif is_failed:
        ops.append("\u2193", style="bold red")
    else:
        ops.append("\u2193", style="dim")

    # Exec phase
    if asset.phase == PHASE_EXECUTING:
        ops.append("\u25b8", style="yellow")
    elif asset.op_exec == "done":
        ops.append("\u25b8", style="green")
    elif is_failed and asset.op_read == "done":
        ops.append("\u25b8", style="bold red")
    else:
        ops.append("\u25b8", style="dim")

    # Write phase
    if asset.phase == PHASE_WRITING:
        ops.append("\u2191", style="magenta")
    elif asset.op_write == "done":
        ops.append("\u2191", style="green")
    elif is_failed and asset.op_exec == "done":
        ops.append("\u2191", style="bold red")
    else:
        ops.append("\u2191", style="dim")

    return ops


def _fmt_ts(timestamp: dt.datetime | None) -> str:
    """Format a timestamp as HH:MM:SS.mmm for log lines."""
    if timestamp is None:
        return "        "
    local = timestamp.astimezone()
    return local.strftime("%H:%M:%S") + f".{local.microsecond // 1000:03d}"


def _fmt_time(seconds: float | None) -> str:
    """Format elapsed seconds as a human-readable duration."""
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m{secs:04.1f}s"


def _fmt_io(asset: AssetState) -> str:
    """Format IO read/write/error counts for display."""
    parts: list[str] = []
    if asset.io_reads:
        parts.append(f"R:{asset.io_reads}")
    if asset.io_writes:
        parts.append(f"W:{asset.io_writes}")
    if asset.io_errors:
        parts.append(f"E:{asset.io_errors}")
    return " ".join(parts) or "-"


def _short_id(value: str | None) -> str:
    """Truncate long IDs to 8 characters for display."""
    if value is None:
        return ""
    return value[:8] if len(value) > 12 else value


def _fit_column(value: str, width: int) -> str:
    """Pad or truncate values to keep log columns aligned."""
    if len(value) <= width:
        return value.ljust(width)
    if width <= 1:
        return value[:width]
    return f"{value[: width - 1]}…"


def _event_type_style(event_type: str) -> str:
    """Color event type by lifecycle outcome."""
    if event_type.endswith("_FAILED"):
        return "bold red"
    if event_type.endswith("_COMPLETED"):
        return "green"
    if event_type.endswith("_STARTED"):
        return "cyan"
    return "bold"


def _progress_bar(done: int, total: int) -> Table:
    """Build a progress bar with count label for display inside a panel."""
    table = Table(show_header=False, box=None, pad_edge=False, expand=True)
    table.add_column("Bar", ratio=1)
    table.add_column("Count", no_wrap=True, justify="right", style="bold")
    table.add_row(
        ProgressBar(
            total=max(total, 1),
            completed=done,
            width=None,
            complete_style="magenta",
            finished_style="magenta",
        ),
        f"{done}/{total}",
    )
    return table


def _partition_style(asset: AssetState | None) -> str:
    """Return the style string for a partition char based on asset state."""
    if asset is None or asset.status == Status.WAITING:
        return "dim"
    if asset.status == Status.DONE:
        return "bold green"
    if asset.status == Status.FAILED:
        return "bold red"
    return _PHASE_STYLE.get(asset.phase or "", "yellow")


# ---------------------------------------------------------------------------
# RichView
# ---------------------------------------------------------------------------


class RichView:
    """Event-driven rich terminal display for Interloper runs and backfills.

    Thread-safe: ``handle_event`` is called from the EventBus background
    thread.  ``rich.live.Live`` handles concurrent refresh internally.
    """

    def __init__(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        console: Console | None = None,
    ) -> None:
        """Initialize the rich view.

        Args:
            dag: The DAG being executed (used to seed asset list).
            partition_or_window: Partition context (used to pre-populate backfill progress).
            console: Optional rich Console override (useful for testing).
        """
        # Pin the Console to the real terminal fd so it keeps working
        # even after we redirect sys.stdout to suppress stray print() calls.
        self._console = console or Console(file=sys.__stdout__)
        self._lock = threading.Lock()
        self._live: Live | None = None
        self._saved_stdout = sys.stdout
        self._saved_stderr = sys.stderr
        self._devnull: TextIO | None = None

        # Pre-computed DAG info (asset order, sources)
        self._asset_order: list[tuple[str | None, AssetInstanceKey, str]] = []
        for asset in dag.assets:
            if asset.materializable:
                self._asset_order.append((asset.source.name if asset.source else None, asset.instance_key, asset.name))
        self._asset_order.sort(key=lambda t: (t[0] or "", t[1]))

        self._max_name_len = max((len(n) for _, _, n in self._asset_order), default=0)
        self._event_asset_key_width = max((len(str(key)) for _, key, _ in self._asset_order), default=1)

        # Mode detection
        self._mode: str | None = None

        # Backfill state
        self._backfill_id: str | None = None
        self._backfill_start: dt.datetime | None = None
        self._backfill_end: dt.datetime | None = None
        self._backfill_error: str | None = None

        # Partition runs
        self._partition_runs: list[PartitionRun] = []
        self._run_index: dict[str, int] = {}
        self._current_run_idx: int | None = None

        if isinstance(partition_or_window, PartitionWindow):
            for p in partition_or_window:
                self._partition_runs.append(PartitionRun(label=str(p)))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the live display."""
        if self._live is not None:
            return

        # Suppress stdout/stderr so that stray print() calls from user code
        # don't corrupt the terminal.  Route them to devnull instead.
        self._saved_stdout = sys.stdout
        self._saved_stderr = sys.stderr
        self._devnull = open(os.devnull, "w")  # noqa: SIM115
        sys.stdout = self._devnull
        sys.stderr = self._devnull

        self._live = Live(
            console=self._console,
            refresh_per_second=8,
            transient=True,
            redirect_stdout=False,
            redirect_stderr=False,
            get_renderable=self._render,
        )
        self._live.start()

    def stop(self) -> None:
        """Stop the live display and print the final summary."""
        if self._live is not None:
            self._live.stop()
            self._live = None

        # Restore stdout/stderr
        sys.stdout = self._saved_stdout
        sys.stderr = self._saved_stderr
        if self._devnull is not None:
            self._devnull.close()
            self._devnull = None

        self._print_summary()

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    def handle_event(self, event: Event) -> None:
        """Process an event and refresh the display."""
        with self._lock:
            self._update_state(event)

        # Log the event above the live panel — Live's render hook handles
        # repositioning so log lines scroll above the fixed panel.
        self._log_event(event)

    def _log_event(self, event: Event) -> None:
        """Log every event above the live panel.

        Uses ``console.print()`` with an explicit timestamp so every line
        gets its own time (unlike ``console.log()`` which deduplicates).
        Live's render hook repositions the output above the fixed panel.
        """
        m = event.metadata
        etype = event.type
        event_label = etype.value.upper()
        asset_key = str(m.get("asset_key") or "-")
        error = m.get("error", "")
        tb = m.get("traceback", "")
        ts = _fmt_ts(event.timestamp)

        # --- Backfill lifecycle ---
        if etype == EventType.BACKFILL_STARTED:
            self._emit_log(
                ts,
                event_label,
                asset_key,
                f"[dim]id={_short_id(m.get('backfill_id'))}[/dim]",
            )
        elif etype == EventType.BACKFILL_COMPLETED:
            self._emit_log(ts, event_label, asset_key, "")
        elif etype == EventType.BACKFILL_FAILED:
            self._emit_log(ts, event_label, asset_key, "")
            self._emit_error_detail(ts, error, tb, event_label, asset_key)

        # --- Run lifecycle ---
        elif etype == EventType.RUN_STARTED:
            partition = f"  [dim]{m['partition_or_window']}[/dim]" if m.get("partition_or_window") else ""
            self._emit_log(
                ts,
                event_label,
                asset_key,
                f"[dim]run={_short_id(m.get('run_id'))}[/dim]{partition}",
            )
        elif etype == EventType.RUN_COMPLETED:
            self._emit_log(
                ts,
                event_label,
                asset_key,
                f"[dim]run={_short_id(m.get('run_id'))}[/dim]",
            )
        elif etype == EventType.RUN_FAILED:
            self._emit_log(
                ts,
                event_label,
                asset_key,
                f"[dim]run={_short_id(m.get('run_id'))}[/dim]",
            )
            self._emit_error_detail(ts, error, tb, event_label, asset_key)

        # --- Asset lifecycle ---
        elif etype == EventType.ASSET_STARTED:
            self._emit_log(ts, event_label, asset_key, "")
        elif etype == EventType.ASSET_COMPLETED:
            asset = self._find_asset_readonly(m)
            parts: list[str] = []
            if asset and asset.elapsed:
                parts.append(_fmt_time(asset.elapsed))
            if asset and asset.io_reads:
                parts.append(f"R:{asset.io_reads}")
            if asset and asset.io_writes:
                parts.append(f"W:{asset.io_writes}")
            detail = f"[dim]{' '.join(parts)}[/dim]" if parts else ""
            self._emit_log(ts, event_label, asset_key, detail)
        elif etype == EventType.ASSET_FAILED:
            self._emit_log(ts, event_label, asset_key, "")
            self._emit_error_detail(ts, error, tb, event_label, asset_key)

        # --- Asset exec lifecycle ---
        elif etype in (EventType.ASSET_EXEC_STARTED, EventType.ASSET_EXEC_COMPLETED):
            self._emit_log(ts, event_label, asset_key, "")
        elif etype == EventType.ASSET_EXEC_FAILED:
            self._emit_log(ts, event_label, asset_key, "")
            self._emit_error_detail(ts, error, tb, event_label, asset_key)

        # --- IO lifecycle ---
        elif etype in (EventType.IO_READ_STARTED, EventType.IO_READ_COMPLETED):
            self._emit_log(ts, event_label, asset_key, "")
        elif etype == EventType.IO_READ_FAILED:
            self._emit_log(ts, event_label, asset_key, "")
            self._emit_error_detail(ts, error, tb, event_label, asset_key)
        elif etype in (EventType.IO_WRITE_STARTED, EventType.IO_WRITE_COMPLETED):
            self._emit_log(ts, event_label, asset_key, "")
        elif etype == EventType.IO_WRITE_FAILED:
            self._emit_log(ts, event_label, asset_key, "")
            self._emit_error_detail(ts, error, tb, event_label, asset_key)

        # --- User log ---
        elif etype == EventType.LOG:
            level = m.get("level", LogLevel.INFO.value)
            message = m.get("message", "")
            level_style = {
                LogLevel.ERROR.value: "bold red",
                LogLevel.WARNING.value: "yellow",
                LogLevel.DEBUG.value: "dim",
            }.get(level, "")
            styled_msg = f"[{level_style}]{message}[/{level_style}]" if level_style else message
            self._emit_log(ts, event_label, asset_key, styled_msg)

    def _emit_log(self, ts: str, event_type: str, asset_key: str, message: str) -> None:
        """Print a timestamped log line above the live panel."""
        type_col = _fit_column(event_type, _EVENT_TYPE_WIDTH)
        asset_col = _fit_column(asset_key, self._event_asset_key_width)
        type_style = _event_type_style(event_type)
        suffix = f"  {message}" if message else ""
        self._console.print(
            f"[dim]{ts}[/dim]  [{type_style}]{type_col}[/{type_style}]  [cyan]{asset_col}[/cyan]{suffix}"
        )

    def _emit_traceback(self, ts: str, tb_str: str, event_type: str, asset_key: str) -> None:
        """Render a traceback with Rich syntax highlighting above the live panel."""
        self._emit_log(ts, event_type, asset_key, "[dim]traceback:[/dim]")
        self._console.print(Syntax(tb_str.rstrip(), "pytb", theme="monokai", padding=1))

    def _emit_error_detail(
        self,
        ts: str,
        error: str,
        traceback_text: str,
        event_type: str,
        asset_key: str,
    ) -> None:
        """Render failure details preferring traceback over plain error text."""
        if traceback_text:
            self._emit_traceback(ts, traceback_text, event_type, asset_key)
        elif error:
            self._emit_log(ts, event_type, asset_key, f"[red]{error}[/red]")

    def _update_state(self, event: Event) -> None:
        """Map an event to internal display state. Caller holds ``_lock``."""
        m = event.metadata
        etype = event.type

        # --- Backfill lifecycle -------------------------------------------
        if etype == EventType.BACKFILL_STARTED:
            self._mode = MODE_BACKFILL
            self._backfill_id = m.get("backfill_id")
            self._backfill_start = event.timestamp

        elif etype == EventType.BACKFILL_COMPLETED:
            self._backfill_end = event.timestamp

        elif etype == EventType.BACKFILL_FAILED:
            self._backfill_end = event.timestamp
            self._backfill_error = m.get("error")

        # --- Run lifecycle ------------------------------------------------
        elif etype == EventType.RUN_STARTED:
            if self._mode is None:
                self._mode = MODE_RUN
            run_id = m.get("run_id", "?")
            partition_label = m.get("partition_or_window")
            run = self._get_or_create_run(run_id, partition_label, event.timestamp)
            self._seed_assets(run)

        elif etype in (EventType.RUN_COMPLETED, EventType.RUN_FAILED):
            run_id = m.get("run_id", "?")
            run = self._find_run(run_id)
            if run is not None:
                run.status = Status.DONE if etype == EventType.RUN_COMPLETED else Status.FAILED
                run.end_time = event.timestamp
                if etype == EventType.RUN_FAILED:
                    run.error = m.get("error")

        # --- Asset lifecycle ----------------------------------------------
        elif etype == EventType.ASSET_STARTED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.status = Status.RUNNING
                asset.phase = None
                asset.start_time = event.timestamp
                asset.op_read = "pending"
                asset.op_exec = "pending"
                asset.op_write = "pending"

        elif etype == EventType.ASSET_COMPLETED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.status = Status.DONE
                asset.phase = None
                asset.end_time = event.timestamp
                asset.op_read = "done"
                asset.op_exec = "done"
                asset.op_write = "done"

        elif etype == EventType.ASSET_FAILED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.status = Status.FAILED
                asset.phase = None
                asset.end_time = event.timestamp
                asset.error = m.get("error")

        # --- Asset exec lifecycle -----------------------------------------
        elif etype == EventType.ASSET_EXEC_STARTED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.phase = PHASE_EXECUTING
                asset.op_read = "done"  # all reads complete (or none needed)

        elif etype == EventType.ASSET_EXEC_COMPLETED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.phase = None
                asset.op_exec = "done"

        elif etype == EventType.ASSET_EXEC_FAILED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.phase = None

        # --- IO lifecycle -------------------------------------------------
        elif etype == EventType.IO_READ_STARTED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.phase = PHASE_READING

        elif etype == EventType.IO_READ_COMPLETED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.io_reads += 1
                asset.phase = None

        elif etype == EventType.IO_WRITE_STARTED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.phase = PHASE_WRITING

        elif etype == EventType.IO_WRITE_COMPLETED:
            asset = self._find_asset(m)
            if asset is not None:
                asset.io_writes += 1
                asset.phase = None

        elif etype in (EventType.IO_READ_FAILED, EventType.IO_WRITE_FAILED):
            asset = self._find_asset(m)
            if asset is not None:
                asset.io_errors += 1
                asset.phase = None

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def _get_or_create_run(
        self,
        run_id: str,
        partition_label: str | None,
        timestamp: dt.datetime | None,
    ) -> PartitionRun:
        """Find an existing pre-populated run by label, or create a new one."""
        for idx, run in enumerate(self._partition_runs):
            if run.run_id is None and run.label == partition_label:
                run.run_id = run_id
                run.status = Status.RUNNING
                run.start_time = timestamp
                self._run_index[run_id] = idx
                self._current_run_idx = idx
                return run

        run = PartitionRun(
            label=partition_label,
            run_id=run_id,
            status=Status.RUNNING,
            start_time=timestamp,
        )
        idx = len(self._partition_runs)
        self._partition_runs.append(run)
        self._run_index[run_id] = idx
        self._current_run_idx = idx
        return run

    def _seed_assets(self, run: PartitionRun) -> None:
        """Pre-populate asset states from the DAG order."""
        for source_name, key, name in self._asset_order:
            if key not in run.assets:
                run.assets[key] = AssetState(key=key, name=name, source_name=source_name)

    def _find_run(self, run_id: str) -> PartitionRun | None:
        """Look up a partition run by its run ID."""
        idx = self._run_index.get(run_id)
        return self._partition_runs[idx] if idx is not None else None

    def _find_asset(self, metadata: dict[str, Any]) -> AssetState | None:
        """Look up an asset by event metadata, creating it if absent."""
        run_id = metadata.get("run_id", "?")
        asset_key = AssetInstanceKey(metadata.get("asset_key", "?"))
        run = self._find_run(run_id)
        if run is None:
            return None
        asset = run.assets.get(asset_key)
        if asset is None:
            asset = AssetState(
                key=asset_key,
                name=metadata.get("asset_name", str(asset_key)),
                source_name=metadata.get("source_name"),
            )
            run.assets[asset_key] = asset
        return asset

    def _find_asset_readonly(self, metadata: dict[str, Any]) -> AssetState | None:
        """Look up an asset without creating it. Used for logging after state update."""
        run = self._find_run(metadata.get("run_id", "?"))
        if run is None:
            return None
        return run.assets.get(AssetInstanceKey(metadata.get("asset_key", "?")))

    def _active_run(self) -> PartitionRun | None:
        """Return the currently executing partition run, if any."""
        if self._current_run_idx is not None and self._current_run_idx < len(self._partition_runs):
            return self._partition_runs[self._current_run_idx]
        return None

    # ------------------------------------------------------------------
    # Rendering — run mode
    # ------------------------------------------------------------------

    def _render_run_view(self) -> RenderableType:
        """Render the run view as a panel with a progress bar inside."""
        run = self._active_run()
        if run is None:
            return Text("Starting...", style="dim")

        completed = sum(1 for a in run.assets.values() if a.status == Status.DONE)
        failed = sum(1 for a in run.assets.values() if a.status == Status.FAILED)
        total = len(run.assets)

        trees = self._build_run_trees(run)
        return Panel(
            Group(_progress_bar(completed + failed, total), *trees),
            title=self._run_title(run),
            border_style="cyan",
            padding=(0, 1),
        )

    def _run_title(self, run: PartitionRun) -> Text:
        """Build a descriptive panel title for run mode."""
        title = Text()
        title.append("Run", style="bold")
        title.append(f" {_short_id(run.run_id)}", style="dim")
        if run.label:
            title.append(f"  {run.label}", style="dim")
        if run.start_time:
            elapsed = ((run.end_time or dt.datetime.now(dt.timezone.utc)) - run.start_time).total_seconds()
            title.append(f"  {_fmt_time(elapsed)}", style="dim")
        return title

    def _build_run_trees(self, run: PartitionRun) -> list[Tree]:
        """Build source trees with per-asset status lines for a single run."""
        trees: list[Tree] = []
        current_source: str | None = None
        current_tree: Tree | None = None

        for source_name, asset_key, _ in self._asset_order:
            if source_name != current_source:
                if current_tree is not None:
                    trees.append(current_tree)
                label = source_name or "<no source>"
                current_tree = Tree(Text(label, style="bold cyan"), guide_style="dim")
                current_source = source_name

            asset = run.assets.get(asset_key)
            if asset is not None:
                line = self._render_run_asset_line(asset)
                assert current_tree is not None
                current_tree.add(line)

        if current_tree is not None:
            trees.append(current_tree)
        return trees

    def _render_run_asset_line(self, asset: AssetState) -> Table:
        """Render a single asset line: name, spinner, ops, status, time, IO."""
        table = Table(show_header=False, box=None, show_lines=False, pad_edge=False, expand=False)
        table.add_column("Name", min_width=self._max_name_len)
        table.add_column("Spinner", min_width=2)
        table.add_column("Ops", min_width=3)
        table.add_column("Status", min_width=8)
        table.add_column("Time", min_width=6, justify="right", style="dim")
        table.add_column("IO", style="dim")

        spinner: RenderableType = Spinner("dots") if asset.status == Status.RUNNING else ""
        ops = _render_ops(asset) if asset.status != Status.WAITING else Text("\u00b7\u00b7\u00b7", style="dim")
        status = _status_text(asset)
        time_str = _fmt_time(asset.elapsed) if asset.status != Status.WAITING else ""
        io_str = _fmt_io(asset) if (asset.io_reads or asset.io_writes or asset.io_errors) else ""

        table.add_row(asset.name, spinner, ops, status, time_str, io_str)
        return table

    # ------------------------------------------------------------------
    # Rendering — backfill mode
    # ------------------------------------------------------------------

    def _render_backfill_view(self) -> RenderableType:
        """Render the backfill view as a panel with a progress bar inside."""
        done = sum(1 for r in self._partition_runs if r.status in (Status.DONE, Status.FAILED))
        total = len(self._partition_runs) or 1

        trees = self._build_backfill_trees()
        return Panel(
            Group(_progress_bar(done, total), *trees),
            title=self._backfill_title(),
            border_style="cyan",
            padding=(0, 1),
        )

    def _backfill_title(self) -> Text:
        """Build a descriptive panel title for backfill mode."""
        title = Text()
        title.append("Backfill", style="bold")
        title.append(f" {_short_id(self._backfill_id)}", style="dim")

        if self._partition_runs:
            first = self._partition_runs[0].label or "?"
            last = self._partition_runs[-1].label or "?"
            if first != last:
                title.append(f"  {first} \u2192 {last}", style="dim")
            else:
                title.append(f"  {first}", style="dim")

        if self._backfill_start:
            elapsed = ((self._backfill_end or dt.datetime.now(dt.timezone.utc)) - self._backfill_start).total_seconds()
            title.append(f"  {_fmt_time(elapsed)}", style="dim")

        return title

    def _build_backfill_trees(self) -> list[Tree]:
        """Build source trees with per-asset progress strips."""
        trees: list[Tree] = []
        current_source: str | None = None
        current_tree: Tree | None = None
        partition_count = len(self._partition_runs)
        active_run = self._active_run()

        for source_name, asset_key, asset_name in self._asset_order:
            if source_name != current_source:
                if current_tree is not None:
                    trees.append(current_tree)
                label = source_name or "<no source>"
                current_tree = Tree(Text(label, style="bold cyan"), guide_style="dim")
                current_source = source_name

            # Get ops for the currently active partition
            current_asset = active_run.assets.get(asset_key) if active_run else None
            line = self._render_backfill_asset_line(asset_key, asset_name, partition_count, current_asset)
            assert current_tree is not None
            current_tree.add(line)

        if current_tree is not None:
            trees.append(current_tree)
        return trees

    def _render_backfill_asset_line(
        self,
        asset_key: AssetInstanceKey,
        asset_name: str,
        partition_count: int,
        current_asset: AssetState | None,
    ) -> Table:
        """Render a single asset line with ops + partition progress strip."""
        table = Table(show_header=False, box=None, show_lines=False, pad_edge=False, expand=False)
        table.add_column("Name", min_width=self._max_name_len)
        table.add_column("Spinner", min_width=2)
        table.add_column("Ops", min_width=3)
        table.add_column("Progress", min_width=partition_count)
        table.add_column("Percent", min_width=5, justify="right", style="magenta")

        progress = Text()
        completed = 0
        has_running = False

        for run in self._partition_runs:
            asset = run.assets.get(asset_key)
            style = _partition_style(asset)
            progress.append(_PARTITION_CHAR, style=style)

            if asset is not None:
                if asset.status in (Status.DONE, Status.FAILED):
                    completed += 1
                elif asset.status == Status.RUNNING:
                    has_running = True

        total = partition_count or 1
        pct = int((completed / total) * 100)
        spinner: RenderableType = Spinner("dots") if has_running else ""

        # Show ops for the currently active partition's asset
        if current_asset is not None and current_asset.status not in (Status.WAITING, Status.DONE):
            ops = _render_ops(current_asset)
        else:
            ops = Text("   ")

        table.add_row(asset_name, spinner, ops, progress, f"{pct}%")
        return table

    # ------------------------------------------------------------------
    # Top-level render dispatch
    # ------------------------------------------------------------------

    def _render(self) -> RenderableType:
        """Build the live renderable (the fixed panel at the bottom)."""
        with self._lock:
            if self._mode == MODE_BACKFILL:
                return self._render_backfill_view()
            elif self._mode == MODE_RUN:
                return self._render_run_view()
            else:
                return Text("Starting...", style="dim")

    # ------------------------------------------------------------------
    # Final summary (static, after live stops)
    # ------------------------------------------------------------------

    def _print_summary(self) -> None:
        """Print the permanent summary to the console."""
        c = self._console

        if self._mode == MODE_RUN:
            self._print_run_summary(c)
        elif self._mode == MODE_BACKFILL:
            self._print_backfill_summary(c)

    def _print_run_summary(self, c: Console) -> None:
        """Print the final run summary as a single panel."""
        run = self._active_run()
        if run is None:
            return

        completed = sum(1 for a in run.assets.values() if a.status == Status.DONE)
        failed = sum(1 for a in run.assets.values() if a.status == Status.FAILED)
        total = len(run.assets)
        ok = failed == 0
        border = "green" if ok else "red"

        trees = self._build_run_trees(run)
        content: list[RenderableType] = [_progress_bar(completed + failed, total), *trees]

        # Footer: status summary
        if ok:
            subtitle = Text(f"\u2713 {completed} completed", style="bold green")
        else:
            subtitle = Text(f"\u2717 {completed} completed, {failed} failed", style="bold red")
        if run.elapsed is not None:
            subtitle.append(f"  {_fmt_time(run.elapsed)}", style="dim")

        c.print(
            Panel(
                Group(*content),
                title=self._run_title(run),
                subtitle=subtitle,
                border_style=border,
                padding=(0, 1),
            )
        )

    def _print_backfill_summary(self, c: Console) -> None:
        """Print the final backfill summary as a single panel."""
        done = sum(1 for r in self._partition_runs if r.status == Status.DONE)
        failed = sum(1 for r in self._partition_runs if r.status == Status.FAILED)
        total = len(self._partition_runs) or 1
        ok = failed == 0
        border = "green" if ok else "red"

        trees = self._build_backfill_trees()
        content: list[RenderableType] = [_progress_bar(done + failed, total), *trees]

        # Footer: status summary
        if ok:
            subtitle = Text(f"\u2713 {done} completed", style="bold green")
        else:
            subtitle = Text(f"\u2717 {done} completed, {failed} failed", style="bold red")
        if self._backfill_start and self._backfill_end:
            elapsed = (self._backfill_end - self._backfill_start).total_seconds()
            subtitle.append(f"  {_fmt_time(elapsed)}", style="dim")

        c.print(
            Panel(
                Group(*content),
                title=self._backfill_title(),
                subtitle=subtitle,
                border_style=border,
                padding=(0, 1),
            )
        )
