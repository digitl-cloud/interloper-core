"""Serialization specs for backfillers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import Field

from interloper.serialization.base import Spec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.backfillers.base import Backfiller


class BackfillerSpec(Spec):
    """Spec for a Backfiller, storing its import path and constructor kwargs."""

    path: str
    init: dict[str, Any] = Field(default_factory=dict)

    def reconstruct(self) -> Backfiller:
        """Reconstruct the backfiller from the spec."""
        return import_from_path(self.path)(**self.init)

