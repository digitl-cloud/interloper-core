"""Serialization specs for IO."""

from __future__ import annotations

from interloper.serialization.base import PathInitSpec


class IOInstanceSpec(PathInitSpec):
    """InstanceSpec for an IO handler, storing its import path and constructor kwargs."""
