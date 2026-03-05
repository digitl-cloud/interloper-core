"""Tests for the event HTTP server."""

from __future__ import annotations

import json
import urllib.request

import pytest

from interloper.events.base import EventType
from interloper.events.server import EventHttpServer

# ---------------------------------------------------------------------------
# _should_forward
# ---------------------------------------------------------------------------


class TestShouldForward:
    """Verify include/exclude filter logic."""

    def test_no_filters_all_pass(self):
        """All event types pass when neither include nor exclude is set."""
        server = EventHttpServer()
        assert server._should_forward(EventType.RUN_STARTED) is True
        assert server._should_forward(EventType.ASSET_FAILED) is True
        assert server._should_forward(EventType.LOG) is True

    def test_include_only_matching_pass(self):
        """Only included event types pass."""
        server = EventHttpServer(include=[EventType.RUN_STARTED, EventType.RUN_COMPLETED])
        assert server._should_forward(EventType.RUN_STARTED) is True
        assert server._should_forward(EventType.RUN_COMPLETED) is True

    def test_include_only_non_matching_blocked(self):
        """Non-included event types are blocked."""
        server = EventHttpServer(include=[EventType.RUN_STARTED])
        assert server._should_forward(EventType.ASSET_STARTED) is False
        assert server._should_forward(EventType.LOG) is False

    def test_exclude_only_matching_blocked(self):
        """Excluded event types are blocked."""
        server = EventHttpServer(exclude=[EventType.LOG])
        assert server._should_forward(EventType.LOG) is False

    def test_exclude_only_non_matching_pass(self):
        """Non-excluded event types pass."""
        server = EventHttpServer(exclude=[EventType.LOG])
        assert server._should_forward(EventType.RUN_STARTED) is True
        assert server._should_forward(EventType.ASSET_COMPLETED) is True

    def test_exclude_takes_precedence_over_include(self):
        """When both are set, exclude wins for overlapping types."""
        server = EventHttpServer(
            include=[EventType.RUN_STARTED, EventType.LOG],
            exclude=[EventType.LOG],
        )
        # LOG is in both include and exclude -- exclude wins
        assert server._should_forward(EventType.LOG) is False
        # RUN_STARTED is included and not excluded
        assert server._should_forward(EventType.RUN_STARTED) is True
        # ASSET_STARTED is not in include
        assert server._should_forward(EventType.ASSET_STARTED) is False


# ---------------------------------------------------------------------------
# EventHttpServer lifecycle and HTTP handling
# ---------------------------------------------------------------------------


class TestEventHttpServer:
    """Verify start/stop lifecycle and HTTP request handling."""

    def test_url_none_before_start(self):
        """Url is None before the server is started."""
        server = EventHttpServer()
        assert server.url is None

    def test_url_set_after_start(self):
        """Url is set after start() and contains host.docker.internal."""
        server = EventHttpServer()
        server.start()
        try:
            assert server.url is not None
            assert "host.docker.internal" in server.url
            assert server.url.endswith("/events")
        finally:
            server.stop()

    def test_url_none_after_stop(self):
        """Server resources are released after stop (server is None)."""
        server = EventHttpServer()
        server.start()
        assert server.url is not None
        server.stop()
        # Internal server is cleaned up
        assert server._server is None

    def test_post_valid_event(self):
        """POST a valid JSON event to /events returns 200."""
        server = EventHttpServer()
        server.start()
        try:
            url = server.url.replace("host.docker.internal", "127.0.0.1")
            data = json.dumps({
                "type": "run_started",
                "timestamp": "2025-01-01T00:00:00Z",
                "run_id": "test-run",
            }).encode()
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
            resp = urllib.request.urlopen(req)
            assert resp.status == 200
        finally:
            server.stop()

    def test_post_invalid_path_returns_404(self):
        """POST to a path other than /events returns 404."""
        server = EventHttpServer()
        server.start()
        try:
            url = server.url.replace("host.docker.internal", "127.0.0.1")
            bad_url = url.replace("/events", "/wrong")
            data = json.dumps({"type": "run_started", "timestamp": "2025-01-01T00:00:00Z"}).encode()
            req = urllib.request.Request(
                bad_url, data=data, headers={"Content-Type": "application/json"}
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 404
        finally:
            server.stop()

    def test_post_invalid_json_returns_400(self):
        """POST with malformed JSON returns 400."""
        server = EventHttpServer()
        server.start()
        try:
            url = server.url.replace("host.docker.internal", "127.0.0.1")
            data = b"not valid json{{"
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 400
        finally:
            server.stop()

    def test_post_invalid_event_type_returns_400(self):
        """POST with an unrecognized event type returns 400."""
        server = EventHttpServer()
        server.start()
        try:
            url = server.url.replace("host.docker.internal", "127.0.0.1")
            data = json.dumps({
                "type": "not_a_real_event",
                "timestamp": "2025-01-01T00:00:00Z",
            }).encode()
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 400
        finally:
            server.stop()

    def test_filtered_event_returns_200_but_not_forwarded(self):
        """POST an excluded event still returns 200 (filter is silent)."""
        server = EventHttpServer(exclude=[EventType.LOG])
        server.start()
        try:
            url = server.url.replace("host.docker.internal", "127.0.0.1")
            data = json.dumps({
                "type": "log",
                "timestamp": "2025-01-01T00:00:00Z",
                "message": "hello",
            }).encode()
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
            resp = urllib.request.urlopen(req)
            assert resp.status == 200
        finally:
            server.stop()
