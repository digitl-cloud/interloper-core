"""Tests for the event bus system."""

import datetime as dt
import threading
import time
from unittest.mock import Mock

from interloper.events.base import (
    Event,
    EventBus,
    EventType,
    disable_event_forwarding,
    emit,
    enable_event_forwarding,
    forward_event,
    subscribe,
    unsubscribe,
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
