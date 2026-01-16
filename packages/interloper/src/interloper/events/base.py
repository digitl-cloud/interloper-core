"""Event bus system for the Interloper framework."""

from __future__ import annotations

import atexit
import datetime as dt
import json
import os
import queue
import sys
import threading
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from queue import Queue
from typing import Any

# Marker prefix for log-based event streaming
INTERLOPER_EVENT_MARKER = "[INTERLOPER_EVENT]"


class EventType(Enum):
    """Event types for the framework lifecycle."""

    ASSET_STARTED = "asset_started"
    ASSET_COMPLETED = "asset_completed"
    ASSET_FAILED = "asset_failed"
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    BACKFILL_STARTED = "backfill_started"
    BACKFILL_COMPLETED = "backfill_completed"
    BACKFILL_FAILED = "backfill_failed"


@dataclass
class Event:
    """Event data structure."""

    type: EventType
    timestamp: dt.datetime | None = None
    run_id: str | None = None
    backfill_id: str | None = None
    asset_key: str | None = None
    partition_or_window: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = dt.datetime.now(dt.timezone.utc)

    def __str__(self) -> str:
        """Return a string representation of the event."""
        fields = [
            f"{str(self.timestamp):<26}",
            f"{str(self.run_id) if self.run_id is not None else '-':<36}",
            f"{str(self.backfill_id) if self.backfill_id is not None else '-':<36}",
            f"{self.type.value.upper():<18}",
            f"{str(self.asset_key) if self.asset_key is not None else '-':<50}",
            f"{str(self.partition_or_window) if self.partition_or_window is not None else '-':<21}",
            f"{str(self.error) if self.error is not None else '-'}",
        ]
        return "  |  ".join(fields)

    def to_dict(self) -> dict[str, Any]:
        """Convert the event to a dictionary."""
        return {
            "type": self.type.value,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "run_id": self.run_id,
            "backfill_id": self.backfill_id,
            "asset_key": self.asset_key,
            "partition_or_window": self.partition_or_window,
            "error": self.error,
        }

    def to_json(self) -> str:
        """Convert the event to a JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Create an Event from a dictionary."""
        timestamp = data.get("timestamp")
        if timestamp and isinstance(timestamp, str):
            timestamp = dt.datetime.fromisoformat(timestamp)
        return cls(
            type=EventType(data["type"]),
            timestamp=timestamp,
            run_id=data.get("run_id"),
            backfill_id=data.get("backfill_id"),
            asset_key=data.get("asset_key"),
            partition_or_window=data.get("partition_or_window"),
            error=data.get("error"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> Event:
        """Create an Event from a JSON string."""
        return cls.from_dict(json.loads(json_str))


@dataclass
class Sentinel:
    """Internal sentinel for flushing the queue."""

    completion_event: threading.Event


class EventBus:
    """Singleton event bus for the framework."""

    _instance: EventBus | None = None
    _lock = threading.Lock()

    def __new__(cls) -> EventBus:
        """Thread-safe singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the event bus."""
        if hasattr(self, "_initialized") and self._initialized:
            return

        self._handlers: dict[Callable, list[EventType] | None] = {}
        self._handler_lock = threading.Lock()
        self._event_queue: Queue[Event | Sentinel] = Queue()
        self._worker_thread: threading.Thread | None = None
        self._shutdown = threading.Event()
        self._initialized = True

        # Start the background worker thread
        self._start_worker()

        # Register shutdown on exit to ensure queue is drained
        atexit.register(self.shutdown)

    def __del__(self) -> None:
        """Clean up the event bus."""
        self.shutdown()

    def _start_worker(self) -> None:
        """Start the background worker thread for processing events."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._worker_thread = threading.Thread(target=self._process_events, daemon=True)
            self._worker_thread.start()

    def _process_events(self) -> None:
        """Background worker that processes events from the queue.

        Continues processing events until shutdown is set and the queue is empty.
        """
        while True:
            # If shutdown is set, only process remaining events, then exit
            if self._shutdown.is_set() and self._event_queue.empty():
                break

            try:
                item = self._event_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            # Check if it's a sentinel
            if isinstance(item, Sentinel):
                item.completion_event.set()
                self._event_queue.task_done()
                continue

            event = item

            # Process the event - ensure task_done() is always called
            try:
                # Get handlers that should process this event
                with self._handler_lock:
                    handlers_to_notify = []
                    for handler, event_types in self._handlers.items():
                        if event_types is None or event.type in event_types:
                            handlers_to_notify.append(handler)

                # Notify handlers (fire-and-forget, errors are isolated)
                for handler in handlers_to_notify:
                    try:
                        handler(event)
                    except Exception:
                        # Isolate handler errors - don't let one handler break others
                        pass
            except Exception as e:
                print(f"Error processing event: {e}")
            finally:
                # Always mark task as done, even if processing failed
                self._event_queue.task_done()

    def subscribe(self, handler: Callable[[Event], None], event_types: list[EventType] | None = None) -> None:
        """Subscribe a handler to events.

        Args:
            handler: Function to call when events are emitted
            event_types: List of event types to subscribe to. If None, subscribes to all events.
        """
        with self._handler_lock:
            self._handlers[handler] = event_types

    def unsubscribe(self, handler: Callable[[Event], None]) -> None:
        """Unsubscribe a handler from events.

        Args:
            handler: Handler to remove
        """
        with self._handler_lock:
            self._handlers.pop(handler, None)

    def emit(self, event: Event) -> None:
        """Emit an event (fire-and-forget).

        Args:
            event: Event to emit
        """
        # Enqueue locally for in-process handlers
        self._event_queue.put(event)

    def flush(self, timeout: float | None = None) -> bool:
        """Flush the event queue.

        This puts a sentinel in the queue and waits for it to be processed.
        This ensures that all events emitted before this call have been processed.

        Args:
            timeout: Maximum time to wait for flush to complete.

        Returns:
            True if flush completed successfully, False if timed out.
        """
        completion_event = threading.Event()
        sentinel = Sentinel(completion_event)
        self._event_queue.put(sentinel)
        return completion_event.wait(timeout)

    def shutdown(self) -> None:
        """Shutdown the event bus and stop the worker thread.

        Waits for the event queue to be drained before stopping the worker thread
        to ensure all events are processed before program exit.
        """
        if self._shutdown.is_set():
            return  # Already shutting down

        self._shutdown.set()

        if self._worker_thread and self._worker_thread.is_alive():
            # Wait for all queued events to be processed
            # queue.join() blocks until task_done() has been called for every item
            try:
                self._event_queue.join()
            except Exception:
                # If join fails, continue anyway to avoid hanging
                pass

            # Wait for thread to finish processing
            self._worker_thread.join(timeout=2.0)

    @classmethod
    def get_instance(cls) -> EventBus:
        """Get the singleton instance of the event bus."""
        return cls()


# Module-level sugar syntax functions
def emit(event: Event) -> None:
    """Emit an event using the global event bus.

    Args:
        event: Event to emit
    """
    EventBus.get_instance().emit(event)


def flush(timeout: float | None = None) -> bool:
    """Flush the event queue.

    Args:
        timeout: Maximum time to wait for flush to complete.

    Returns:
        True if flush completed successfully, False if timed out.
    """
    return EventBus.get_instance().flush(timeout)


def subscribe(handler: Callable[[Event], None], event_types: list[EventType] | None = None) -> None:
    """Subscribe to events using the global event bus.

    Args:
        handler: Function to call when events are emitted
        event_types: List of event types to subscribe to. If None, subscribes to all events.
    """
    EventBus.get_instance().subscribe(handler, event_types)


def unsubscribe(handler: Callable[[Event], None]) -> None:
    """Unsubscribe from events using the global event bus.

    Args:
        handler: Handler to remove
    """
    EventBus.get_instance().unsubscribe(handler)


# Eagerly initialize the event bus so HTTP emission is available even if callers
# only use the module-level helpers and never explicitly touch the singleton.
EventBus.get_instance()


# Propagate all events to a remote event bus if configured (used by containerized runs)
def on_event(event: Event) -> None:  # noqa: D103
    # Log-based event streaming (for Docker/K8s log collection)
    if os.getenv("INTERLOPER_EVENTS_TO_STDERR"):
        try:
            sys.stderr.write(f"{INTERLOPER_EVENT_MARKER}{event.to_json()}\n")
            sys.stderr.flush()
        except Exception:
            pass

    # HTTP-based event forwarding (legacy, for backwards compatibility)
    target = os.getenv("INTERLOPER_EVENTS_TARGET_URL")
    if target:
        try:
            data = json.dumps(event.to_dict()).encode("utf-8")
            req = urllib.request.Request(
                url=target,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception as e:
            print(f"Error propagating event to {target}: {e}")
            pass


subscribe(on_event)


def parse_event_from_log_line(line: str) -> Event | None:
    """Parse an event from a log line if it contains the event marker.

    Args:
        line: A log line that may contain an event marker

    Returns:
        The parsed Event if the line contains a valid event, None otherwise
    """
    if INTERLOPER_EVENT_MARKER not in line:
        return None

    try:
        # Extract JSON after the marker
        marker_idx = line.index(INTERLOPER_EVENT_MARKER)
        json_str = line[marker_idx + len(INTERLOPER_EVENT_MARKER) :].strip()
        return Event.from_json(json_str)
    except (ValueError, json.JSONDecodeError, KeyError):
        return None
