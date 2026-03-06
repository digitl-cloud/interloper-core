"""Serialization specs for runners."""

from __future__ import annotations

from interloper.serialization.base import PathInitSpec


class RunnerInstanceSpec(PathInitSpec):
    """InstanceSpec for a Runner, storing its import path and constructor kwargs."""
