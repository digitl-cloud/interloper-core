"""Mixin providing event subscription lifecycle for Runner and Backfiller."""

from __future__ import annotations

from typing import Any

from typing_extensions import Self

from interloper.events.base import Event, flush, subscribe, unsubscribe


class EventSubscriber:
    """Mixin that wires an ``on_event`` callback to the event bus.

    Concrete classes must declare the following on themselves (Pydantic
    fields cannot be inherited from a non-BaseModel mixin):

    - ``_state_id_field: str`` — class variable naming the metadata key
      used to filter events (e.g. ``"run_id"`` or ``"backfill_id"``).
    - ``on_event`` — Pydantic ``Field`` for the user callback.
    - ``_on_event`` — ``PrivateAttr`` for the internal filtered handler.
    - ``_subscribed_via_context_manager`` — ``PrivateAttr(default=False)``.
    - ``_state`` — ``PrivateAttr`` holding the state object (RunState / BackfillState).

    The mixin then provides ``model_post_init``, context-manager, and
    ``__del__`` implementations that operate on those attributes.

    Concrete classes should call ``super().model_post_init(__context)``
    if they override ``model_post_init``.
    """

    _state_id_field: str  # e.g. "run_id" or "backfill_id"

    def model_post_init(self, __context: Any, /) -> None:
        """Subscribe a filtered event handler if ``on_event`` is set."""
        super().model_post_init(__context)  # type: ignore[misc]

        if self.on_event is not None:  # type: ignore[attr-defined]
            raw_callback = self.on_event  # type: ignore[attr-defined]

            def event_handler(event: Event) -> None:
                state = getattr(self, "_state", None)
                state_id = getattr(state, self._state_id_field, None)
                if state is not None and event.metadata.get(self._state_id_field) == state_id:
                    raw_callback(event)

            self._on_event = event_handler  # type: ignore[attr-defined]
            subscribe(event_handler)

    def __del__(self) -> None:
        """Flush pending events and unsubscribe if not using context manager."""
        try:
            if self._on_event is not None and not self._subscribed_via_context_manager:  # type: ignore[attr-defined]
                flush()
                unsubscribe(self._on_event)  # type: ignore[attr-defined]
        except AttributeError:
            pass  # Pydantic private attrs not yet initialized during GC

    def __enter__(self) -> Self:
        """Mark that event cleanup should happen on ``__exit__`` rather than ``__del__``.

        Returns:
            This instance for use in a ``with`` block.
        """
        self._subscribed_via_context_manager = True  # type: ignore[attr-defined]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Flush pending events and unsubscribe from the event bus."""
        if self._on_event is not None and self._subscribed_via_context_manager:  # type: ignore[attr-defined]
            flush()
            unsubscribe(self._on_event)  # type: ignore[attr-defined]
            self._on_event = None  # type: ignore[attr-defined]
            self._subscribed_via_context_manager = False  # type: ignore[attr-defined]
