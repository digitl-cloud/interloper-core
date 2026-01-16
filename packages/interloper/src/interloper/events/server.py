"""Lightweight HTTP server that forwards JSON events to the local event bus."""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from interloper.events.base import Event, EventType, emit


class EventHttpServer:
    """Lightweight HTTP server that forwards JSON events to the local event bus."""

    def __init__(
        self,
        include: list[EventType] | None = None,
        exclude: list[EventType] | None = None,
    ) -> None:
        """Initialize the event HTTP server.

        Args:
            include: If provided, only events with types in this list will be forwarded.
                If None, all events pass the include filter.
            exclude: If provided, events with types in this list will be filtered out.
                If None, no events are excluded.
        """
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._url: str | None = None
        self._include: list[EventType] | None = include
        self._exclude: list[EventType] | None = exclude

    @property
    def url(self) -> str | None:
        """Get the URL of the event HTTP server."""
        return self._url

    def start(self) -> None:
        """Start the event HTTP server."""
        # Bind to an ephemeral port on all interfaces
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", 0))
        _, port = sock.getsockname()
        sock.close()

        handler_class = self._make_handler()
        server = HTTPServer(("0.0.0.0", port), handler_class)
        self._server = server
        self._url = f"http://host.docker.internal:{port}/events"

        def serve() -> None:
            try:
                server.serve_forever(poll_interval=0.2)
            except Exception:
                pass

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        self._thread = t

    def stop(self) -> None:
        """Stop the event HTTP server."""
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:
                pass
            self._server.server_close()
            self._server = None
        self._thread = None

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        """Create an HTTP handler class that captures the server's filters."""
        server_ref = self

        class EventHttpHandler(BaseHTTPRequestHandler):
            """HTTP handler that forwards JSON events to the local event bus."""

            def do_POST(self) -> None:  # noqa: N802
                """Handle a POST request."""
                if self.path != "/events":
                    self.send_response(404)
                    self.end_headers()
                    return

                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)

                try:
                    data = json.loads(body.decode("utf-8"))
                    try:
                        event_type = EventType(data.get("type"))
                    except (ValueError, TypeError):
                        # Invalid or missing event type
                        self.send_response(400)
                        self.end_headers()
                        return

                    # Check filters before forwarding
                    if server_ref._should_forward(event_type):
                        emit(
                            Event(
                                type=event_type,
                                timestamp=data.get("timestamp"),
                                run_id=data.get("run_id"),
                                backfill_id=data.get("backfill_id"),
                                asset_key=data.get("asset_key"),
                                partition_or_window=data.get("partition_or_window"),
                                error=data.get("error"),
                            )
                        )

                    self.send_response(200)
                    self.end_headers()
                except json.JSONDecodeError:
                    print(f"Error decoding event: {body.decode('utf-8')}")
                    self.send_response(400, "Error decoding event")
                    self.end_headers()
                except Exception as e:
                    print(f"Error forwarding event: {e}")
                    self.send_response(500, "Error forwarding event")
                    self.end_headers()

            def log_message(self, format: str, *args: object) -> None:  # silence
                """Silence HTTP server logs."""
                return

        return EventHttpHandler

    def _should_forward(self, event_type: EventType) -> bool:
        """Check if an event type should be forwarded based on include/exclude filters.

        Args:
            event_type: The event type to check

        Returns:
            True if the event should be forwarded, False otherwise
        """
        # Exclude filter takes precedence
        if self._exclude is not None and event_type in self._exclude:
            return False

        # Include filter: if provided, must be in the list
        if self._include is not None and event_type not in self._include:
            return False

        return True
