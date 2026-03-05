"""Tests for the event bus system."""

import datetime as dt
import threading
import time
from unittest.mock import Mock

import pytest

from interloper.errors import EventError
from interloper.events.base import (
    INTERLOPER_EVENT_MARKER,
    Event,
    EventBus,
    EventType,
    disable_event_forwarding,
    emit,
    enable_event_forwarding,
    forward_event,
    parse_event_from_log_line,
    subscribe,
    unsubscribe,
)
from interloper.events.base import (
    flush as flush_fn,
)


class TestEvent:
    """Test Event dataclass."""

    def test_event_creation(self):
        """Test basic event creation."""
        event = Event(
            type=EventType.ASSET_STARTED,
            metadata={"asset_key": "test_asset", "partition_or_window": "2025-01-01"},
        )

        assert event.type == EventType.ASSET_STARTED
        assert event.metadata["asset_key"] == "test_asset"
        assert event.metadata["partition_or_window"] == "2025-01-01"
        assert isinstance(event.timestamp, dt.datetime)

    def test_event_creation_with_window(self):
        """Test event creation with partition window."""
        event = Event(
            type=EventType.ASSET_STARTED,
            metadata={"asset_key": "test_asset", "partition_or_window": "2025-01-01 to 2025-01-07"},
        )

        assert event.type == EventType.ASSET_STARTED
        assert event.metadata["asset_key"] == "test_asset"
        assert event.metadata["partition_or_window"] == "2025-01-01 to 2025-01-07"
        assert isinstance(event.timestamp, dt.datetime)

    def test_log_event_creation(self):
        """Test creating a LOG event."""
        event = Event(
            type=EventType.LOG,
            metadata={"asset_key": "my_asset", "message": "hello world", "level": "info"},
        )

        assert event.type == EventType.LOG
        assert event.metadata["message"] == "hello world"
        assert event.metadata["level"] == "info"

    def test_log_event_round_trip(self):
        """Test LOG event serialization/deserialization round-trip."""
        event = Event(
            type=EventType.LOG,
            metadata={"asset_key": "my_asset", "message": "test message", "level": "warning"},
        )

        json_str = event.to_json()
        restored = Event.from_json(json_str)

        assert restored.type == EventType.LOG
        assert restored.metadata["message"] == "test message"
        assert restored.metadata["level"] == "warning"
        assert restored.metadata["asset_key"] == "my_asset"


class TestEventBus:
    """Test EventBus singleton."""

    def test_singleton(self):
        """Test that EventBus is a singleton."""
        bus1 = EventBus()
        bus2 = EventBus()
        bus3 = EventBus.get_instance()

        assert bus1 is bus2
        assert bus1 is bus3

    def test_subscribe_unsubscribe(self):
        """Test subscribing and unsubscribing handlers."""
        bus = EventBus.get_instance()
        handler = Mock()

        # Subscribe
        bus.subscribe(handler)
        assert handler in bus._handlers
        assert bus._handlers[handler] is None  # No type filter

        # Unsubscribe
        bus.unsubscribe(handler)
        assert handler not in bus._handlers

    def test_subscribe_with_type_filter(self):
        """Test subscribing with event type filter."""
        bus = EventBus.get_instance()
        handler = Mock()
        event_types = [EventType.ASSET_STARTED, EventType.ASSET_COMPLETED]

        bus.subscribe(handler, event_types)
        assert bus._handlers[handler] == event_types

    def test_emit_basic(self):
        """Test basic event emission."""
        bus = EventBus.get_instance()
        handler = Mock()

        bus.subscribe(handler)

        event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "test_asset"})

        bus.emit(event)

        # Give the background thread time to process
        time.sleep(0.1)

        handler.assert_called_once_with(event)

    def test_emit_with_type_filter(self):
        """Test that type filtering works correctly."""
        bus = EventBus.get_instance()
        handler1 = Mock()
        handler2 = Mock()

        # Handler 1 subscribes to ASSET_STARTED only
        bus.subscribe(handler1, [EventType.ASSET_STARTED])
        # Handler 2 subscribes to all events
        bus.subscribe(handler2)

        # Emit ASSET_STARTED event
        event1 = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "asset1"})
        bus.emit(event1)

        # Emit ASSET_COMPLETED event
        event2 = Event(type=EventType.ASSET_COMPLETED, metadata={"asset_key": "asset1"})
        bus.emit(event2)

        # Give the background thread time to process
        time.sleep(0.1)

        # Handler 1 should only receive ASSET_STARTED
        assert handler1.call_count == 1
        handler1.assert_called_with(event1)

        # Handler 2 should receive both events
        assert handler2.call_count == 2
        handler2.assert_any_call(event1)
        handler2.assert_any_call(event2)

    def test_handler_error_isolation(self):
        """Test that handler errors don't break other handlers."""
        bus = EventBus.get_instance()

        def bad_handler(event):
            raise ValueError("Handler error")

        good_handler = Mock()

        bus.subscribe(bad_handler)
        bus.subscribe(good_handler)

        event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "test_asset"})
        bus.emit(event)

        # Give the background thread time to process
        time.sleep(0.1)

        # Good handler should still be called despite bad handler error
        good_handler.assert_called_once_with(event)

    def test_fifo_ordering(self):
        """Test that events are processed in FIFO order."""
        bus = EventBus.get_instance()
        received_events = []

        def handler(event):
            received_events.append(int(event.metadata["asset_key"].split("_")[1]))

        bus.subscribe(handler)

        # Emit events in order
        for i in range(5):
            event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": f"asset_{i}"})
            bus.emit(event)

        # Give the background thread time to process
        time.sleep(0.2)

        # Events should be processed in order
        assert received_events == [0, 1, 2, 3, 4]


class TestSugarSyntax:
    """Test the sugar syntax functions."""

    def test_emit_sugar(self):
        """Test the emit sugar function."""
        handler = Mock()
        subscribe(handler)

        emit(EventType.ASSET_STARTED, metadata={"asset_key": "test_asset"})

        # Give the background thread time to process
        time.sleep(0.1)

        handler.assert_called_once()
        event = handler.call_args[0][0]
        assert event.type == EventType.ASSET_STARTED
        assert event.metadata["asset_key"] == "test_asset"

    def test_subscribe_sugar(self):
        """Test the subscribe sugar function."""
        handler = Mock()
        event_types = [EventType.ASSET_STARTED]

        subscribe(handler, event_types)

        # Verify subscription worked
        bus = EventBus.get_instance()
        assert handler in bus._handlers
        assert bus._handlers[handler] == event_types

    def test_unsubscribe_sugar(self):
        """Test the unsubscribe sugar function."""
        handler = Mock()
        subscribe(handler)

        # Verify subscription
        bus = EventBus.get_instance()
        assert handler in bus._handlers

        # Unsubscribe
        unsubscribe(handler)

        # Verify unsubscription
        assert handler not in bus._handlers


class TestIntegration:
    """Integration tests for the event bus."""

    def test_multiple_handlers_same_event(self):
        """Test that multiple handlers can subscribe to the same event."""
        bus = EventBus.get_instance()
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()

        bus.subscribe(handler1)
        bus.subscribe(handler2, [EventType.ASSET_STARTED])
        bus.subscribe(handler3, [EventType.ASSET_COMPLETED])

        event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "test_asset"})
        bus.emit(event)

        # Give the background thread time to process
        time.sleep(0.1)

        # Handler 1 and 2 should be called (handler 1 subscribes to all, handler 2 to ASSET_STARTED)
        assert handler1.call_count == 1
        assert handler2.call_count == 1
        assert handler3.call_count == 0

    def test_concurrent_emission(self):
        """Test that concurrent event emission works correctly."""
        bus = EventBus.get_instance()
        received_events = []

        def handler(event):
            received_events.append(int(event.metadata["asset_key"].split("_")[1]))

        bus.subscribe(handler)

        # Emit events concurrently
        threads = []
        for i in range(10):

            def emit_event(event_id):
                event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": f"asset_{event_id}"})
                bus.emit(event)

            thread = threading.Thread(target=emit_event, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Give the background thread time to process
        time.sleep(0.2)

        # All events should be received
        assert len(received_events) == 10
        assert set(received_events) == set(range(10))


class TestDefaultForwarding:
    """Tests for opt-in default event forwarding."""

    def test_default_forwarding_is_opt_in_and_idempotent(self):
        bus = EventBus.get_instance()

        # Ensure clean state regardless of previous tests.
        disable_event_forwarding()
        assert forward_event not in bus._handlers

        assert enable_event_forwarding() is True
        assert forward_event in bus._handlers

        # Second enable should be a no-op.
        assert enable_event_forwarding() is False
        assert forward_event in bus._handlers

        assert disable_event_forwarding() is True
        assert forward_event not in bus._handlers

        # Second disable should be a no-op.
        assert disable_event_forwarding() is False


class TestEventStr:
    """Test Event.__str__ human-readable output."""

    def test_str_with_all_metadata(self):
        """Test __str__ includes timestamp, type, asset_key, and message."""
        ts = dt.datetime(2025, 6, 15, 10, 30, 45, 123456, tzinfo=dt.timezone.utc)
        event = Event(
            type=EventType.ASSET_STARTED,
            timestamp=ts,
            metadata={"asset_key": "my_asset", "message": "starting up"},
        )
        result = str(event)
        assert "10:30:45.123" in result
        assert "ASSET_STARTED" in result
        assert "my_asset" in result
        assert "starting up" in result

    def test_str_missing_asset_key_shows_dash(self):
        """Test __str__ shows '-' when asset_key is absent."""
        event = Event(type=EventType.RUN_STARTED, metadata={})
        result = str(event)
        # The type field is padded to 20 chars; after the type the remaining
        # two-space-separated tokens should both be '-'.
        assert "RUN_STARTED" in result
        # Strip leading timestamp + type portion and check trailing fields.
        # The format is: "HH:MM:SS.mmm  TYPE<pad>  asset_key  message"
        assert result.rstrip().endswith("-  -")

    def test_str_missing_message_shows_dash(self):
        """Test __str__ shows '-' when message is absent."""
        event = Event(
            type=EventType.ASSET_COMPLETED,
            metadata={"asset_key": "x"},
        )
        result = str(event)
        assert result.endswith("-")


class TestEventFromDictErrors:
    """Test Event.from_dict validation error paths."""

    def test_missing_type_raises_event_error(self):
        """Missing 'type' key raises EventError."""
        with pytest.raises(EventError, match="Missing required field 'type'"):
            Event.from_dict({"timestamp": "2025-01-01T00:00:00+00:00"})

    def test_invalid_event_type_raises_event_error(self):
        """Unrecognised type value raises EventError."""
        with pytest.raises(EventError, match="Invalid event type"):
            Event.from_dict({"type": "bogus_type", "timestamp": "2025-01-01T00:00:00+00:00"})

    def test_missing_timestamp_raises_event_error(self):
        """Missing 'timestamp' key raises EventError."""
        with pytest.raises(EventError, match="Missing required field 'timestamp'"):
            Event.from_dict({"type": "asset_started"})

    def test_invalid_timestamp_format_raises_event_error(self):
        """Unparseable timestamp string raises EventError."""
        with pytest.raises(EventError, match="Invalid timestamp format"):
            Event.from_dict({"type": "asset_started", "timestamp": "not-a-date"})

    def test_non_string_non_datetime_timestamp_raises_event_error(self):
        """Non-string, non-datetime timestamp raises EventError."""
        with pytest.raises(EventError, match="Invalid timestamp value"):
            Event.from_dict({"type": "asset_started", "timestamp": 12345})


class TestEventBusFlush:
    """Test EventBus.flush sentinel-based draining."""

    def test_flush_returns_true(self):
        """flush(timeout=5) returns True after all events are processed."""
        bus = EventBus.get_instance()
        handler = Mock()
        bus.subscribe(handler)

        event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "f"})
        bus.emit(event)

        result = bus.flush(timeout=5)
        assert result is True
        handler.assert_called_once_with(event)


class TestEventBusShutdownIdempotent:
    """Test EventBus.shutdown idempotency."""

    def test_shutdown_twice_is_idempotent(self):
        """Calling shutdown() a second time returns immediately without error."""
        bus = EventBus.get_instance()

        # Emit something so the bus is actively working.
        bus.emit(Event(type=EventType.LOG, metadata={"asset_key": "z", "message": "hi", "level": "info"}))
        bus.flush(timeout=2)

        bus.shutdown()
        # Second call should return immediately (no-op).
        bus.shutdown()

        # Re-initialise the bus for other tests.
        bus._shutdown.clear()
        bus._start_worker()


class TestFlushSugar:
    """Test module-level flush() convenience function."""

    def test_flush_sugar_returns_true(self):
        """Module-level flush() returns True after events are processed."""
        handler = Mock()
        subscribe(handler)

        emit(EventType.ASSET_STARTED, metadata={"asset_key": "sugar"})
        result = flush_fn(timeout=5)

        assert result is True
        handler.assert_called_once()


class TestForwardEvent:
    """Test forward_event with env-controlled transports."""

    def test_stderr_output(self, monkeypatch, capsys):
        """With INTERLOPER_EVENTS_TO_STDERR set, event JSON is written to stderr."""
        monkeypatch.setenv("INTERLOPER_EVENTS_TO_STDERR", "1")
        monkeypatch.delenv("INTERLOPER_EVENTS_TARGET_URL", raising=False)

        event = Event(
            type=EventType.ASSET_STARTED,
            metadata={"asset_key": "test_asset"},
        )
        forward_event(event)

        captured = capsys.readouterr()
        assert INTERLOPER_EVENT_MARKER in captured.err
        assert '"asset_started"' in captured.err

    def test_unreachable_url_does_not_raise(self, monkeypatch):
        """With INTERLOPER_EVENTS_TARGET_URL pointing nowhere, forward_event should not raise."""
        monkeypatch.delenv("INTERLOPER_EVENTS_TO_STDERR", raising=False)
        monkeypatch.setenv("INTERLOPER_EVENTS_TARGET_URL", "http://127.0.0.1:1")

        event = Event(
            type=EventType.ASSET_COMPLETED,
            metadata={"asset_key": "test_asset"},
        )
        # Should not raise despite the connection error.
        forward_event(event)


class TestParseEventFromLogLine:
    """Test parse_event_from_log_line helper."""

    def test_valid_line(self):
        """A line with the marker followed by valid JSON returns an Event."""
        event = Event(type=EventType.ASSET_STARTED, metadata={"asset_key": "a"})
        line = f"some prefix {INTERLOPER_EVENT_MARKER}{event.to_json()}"
        parsed = parse_event_from_log_line(line)
        assert parsed is not None
        assert parsed.type == EventType.ASSET_STARTED
        assert parsed.metadata["asset_key"] == "a"

    def test_line_without_marker_returns_none(self):
        """A regular log line without the marker returns None."""
        assert parse_event_from_log_line("INFO: nothing special here") is None

    def test_malformed_json_returns_none(self):
        """A line with the marker but invalid JSON returns None."""
        line = f"{INTERLOPER_EVENT_MARKER}{{not valid json}}"
        assert parse_event_from_log_line(line) is None
