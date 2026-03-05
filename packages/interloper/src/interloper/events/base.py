"""Event types, data structures, and the singleton event bus."""

from __future__ import annotations

import atexit
import datetime as dt
import json
import logging
import os
import queue
import sys
import threading
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from queue import Queue
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import Self

from interloper.errors import EventError

if TYPE_CHECKING:
    from interloper.assets.base import Asset

logger = logging.getLogger(__name__)

# Marker prefix for log-based event streaming
INTERLOPER_EVENT_MARKER = "[INTERLOPER_EVENT]"


class EventType(Enum):
    """Enumeration of all framework lifecycle event types."""

    ASSET_STARTED = "asset_started"
    ASSET_COMPLETED = "asset_completed"
    ASSET_FAILED = "asset_failed"
    ASSET_EXEC_STARTED = "asset_exec_started"
    ASSET_EXEC_COMPLETED = "asset_exec_completed"
    ASSET_EXEC_FAILED = "asset_exec_failed"
    IO_READ_STARTED = "io_read_started"
    IO_READ_COMPLETED = "io_read_completed"
    IO_READ_FAILED = "io_read_failed"
    IO_WRITE_STARTED = "io_write_started"
    IO_WRITE_COMPLETED = "io_write_completed"
    IO_WRITE_FAILED = "io_write_failed"
    RUN_STARTED = "run_started"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    BACKFILL_STARTED = "backfill_started"
    BACKFILL_COMPLETED = "backfill_completed"
    BACKFILL_FAILED = "backfill_failed"
    LOG = "log"


class LogLevel(Enum):
    """Log levels for user-emitted LOG events."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class Event:
    """An event emitted during the framework lifecycle.

    Carries a type, UTC timestamp, and an arbitrary metadata dict.
    Supports JSON and dict serialization for forwarding and persistence.
    """

    type: EventType
    timestamp: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return a human-readable summary line for logging."""
        m = self.metadata
        fields = [
            f"{self.timestamp.strftime('%H:%M:%S.%f')[:-3]}",
            f"{self.type.value.upper():<20}",
            f"{str(m.get('asset_key')) if m.get('asset_key') is not None else '-'}",
            # f"{str(m.get('partition_or_window')) if m.get('partition_or_window') is not None else '-':<21}",
            # f"{str(m.get('error')) if m.get('error') is not None else '-'}",
            f"{str(m.get('message')) if m.get('message') is not None else '-'}",
        ]
        return "  ".join(fields)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a flat dict with type, timestamp, and metadata fields.

        Returns:
            Dict with type, timestamp, and metadata fields.
        """
        return {
            "type": self.type.value,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            **self.metadata,
        }

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Returns:
            JSON-encoded string of the event.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Deserialize an Event from a dict.

        Returns:
            The deserialized Event instance.

        Raises:
            EventError: If required fields are missing or have invalid values.
        """
        # Check mandatory 'type'
        type_val = data.get("type")
        if type_val is None:
            raise EventError("Missing required field 'type' in Event")
        try:
            event_type = EventType(type_val)
        except ValueError:
            raise EventError(f"Invalid event type '{type_val}' in Event")

        # Check mandatory 'timestamp'
        timestamp_val = data.get("timestamp")
        if timestamp_val is None:
            raise EventError("Missing required field 'timestamp' in Event")
        if isinstance(timestamp_val, str):
            try:
                timestamp = dt.datetime.fromisoformat(timestamp_val)
            except ValueError:
                raise EventError(f"Invalid timestamp format: {timestamp_val!r}")
        elif isinstance(timestamp_val, dt.datetime):
            timestamp = timestamp_val
        else:
            raise EventError(f"Invalid timestamp value for Event: {timestamp_val!r}")

        metadata = {k: v for k, v in data.items() if k not in ("type", "timestamp")}

        return cls(
            type=event_type,
            timestamp=timestamp,
            metadata=metadata,
        )

    @classmethod
    def from_json(cls, json_str: str) -> Event:
        """Deserialize an Event from a JSON string.

        Returns:
            The deserialized Event instance.
        """
        return cls.from_dict(json.loads(json_str))


@dataclass
class Sentinel:
    """Marker placed on the event queue to signal that all preceding events have been processed.

    Used by :meth:`EventBus.flush` to block until the queue is drained up to
    this point.  The ``completion_event`` is set by the worker thread when it
    dequeues the sentinel.
    """

    completion_event: threading.Event


class EventBus:
    """Thread-safe singleton event bus.

    Events are enqueued and processed asynchronously by a background worker
    thread.  Handlers are called in the order they were subscribed.
    """

    _instance: EventBus | None = None
    _lock = threading.Lock()

    def __new__(cls) -> Self:
        """Create or return the singleton instance (double-checked locking)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        assert cls._instance is not None
        return cast(Self, cls._instance)

    def __init__(self) -> None:
        """Initialize handlers, queue, and background worker (runs once)."""
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
        """Drain the queue and stop the worker on garbage collection."""
        self.shutdown()

    def _start_worker(self) -> None:
        """Start the background daemon thread that drains the event queue."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._worker_thread = threading.Thread(target=self._process_events, daemon=True)
            self._worker_thread.start()

    def _process_events(self) -> None:
        """Worker loop: dequeue events and dispatch to handlers until shutdown."""
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
                    except Exception:  # noqa: BLE001, S110
                        # Isolate handler errors - don't let one handler break others
                        pass
            except Exception:  # noqa: BLE001, S110
                pass
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
            except Exception:  # noqa: BLE001, S110
                # If join fails, continue anyway to avoid hanging
                pass

            # Wait for thread to finish processing
            self._worker_thread.join(timeout=2.0)

    @classmethod
    def get_instance(cls) -> EventBus:
        """Get the singleton instance of the event bus.

        Returns:
            The singleton EventBus instance.
        """
        return cls()


# Module-level sugar syntax functions
def emit(event_type: EventType, *, metadata: dict[str, Any] | None = None) -> None:
    """Emit an event using the global event bus.

    Args:
        event_type: The type of event to emit.
        metadata: Optional key-value pairs attached to the event.
    """
    EventBus.get_instance().emit(Event(type=event_type, metadata=metadata or {}))


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


def get_asset_event_metadata(asset: Asset) -> dict[str, Any]:
    """Build common metadata fields (key, name, source) for an asset event.

    Returns:
        Dict with asset_key, asset_name, and optionally source_name.
    """
    metadata = {
        "asset_key": asset.instance_key,
        "asset_name": asset.name,
    }
    if asset.source is not None:
        metadata["source_name"] = asset.source.name
    return metadata


_event_forwarding_enabled = False


def enable_event_forwarding() -> bool:
    """Enable event forwarding hooks in an idempotent way.

    Returns:
        True if forwarding was enabled by this call, False if it was already enabled.
    """
    global _event_forwarding_enabled
    if _event_forwarding_enabled:
        return False
    subscribe(forward_event)
    _event_forwarding_enabled = True
    return True


def disable_event_forwarding() -> bool:
    """Disable event forwarding hooks in an idempotent way.

    Returns:
        True if forwarding was disabled by this call, False if it was already disabled.
    """
    global _event_forwarding_enabled
    if not _event_forwarding_enabled:
        return False
    unsubscribe(forward_event)
    _event_forwarding_enabled = False
    return True


def forward_event(event: Event) -> None:
    """Forward an event via stderr logging and/or HTTP POST.

    Controlled by environment variables:

    - ``INTERLOPER_EVENTS_TO_STDERR``: write JSON to stderr (for Docker/K8s log collection).
    - ``INTERLOPER_EVENTS_TARGET_URL``: POST JSON to a remote endpoint (legacy).
    """
    # Log-based event streaming (for Docker/K8s log collection)
    if os.getenv("INTERLOPER_EVENTS_TO_STDERR"):
        try:
            sys.stderr.write(f"{INTERLOPER_EVENT_MARKER}{event.to_json()}\n")
            sys.stderr.flush()
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error writing event to stderr: {e}")

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
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error forwarding event to {target}: {e}")


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
