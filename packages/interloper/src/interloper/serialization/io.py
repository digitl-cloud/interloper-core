"""Serialization specs for IO."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import Field

from interloper.serialization.base import InstanceSpec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.io.base import IO


class IOInstanceSpec(InstanceSpec):
    """InstanceSpec for an IO handler, storing its import path and constructor kwargs."""

    path: str
    init: dict[str, Any] = Field(default_factory=dict)

    def reconstruct(self) -> IO:
        """Reconstruct the IO object from spec.

        Returns:
            The reconstructed IO instance.
        """
        return import_from_path(self.path)(**self.init)
