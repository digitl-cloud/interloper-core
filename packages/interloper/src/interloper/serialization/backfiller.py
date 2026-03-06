"""Serialization specs for backfillers."""

from __future__ import annotations

from interloper.serialization.base import PathInitSpec


class BackfillerInstanceSpec(PathInitSpec):
    """InstanceSpec for a Backfiller, storing its import path and constructor kwargs."""
