"""Specs for serializing framework objects into portable Pydantic models.

Each Spec can reconstruct the original object from its serialized form,
enabling cross-process transport and persistent configuration.
"""

from interloper.serialization.asset import AssetSpec
from interloper.serialization.backfiller import BackfillerSpec
from interloper.serialization.config import ConfigSpec
from interloper.serialization.dag import DAGSpec
from interloper.serialization.io import IOSpec
from interloper.serialization.runner import RunnerSpec

__all__ = [
    "AssetSpec",
    "BackfillerSpec",
    "ConfigSpec",
    "DAGSpec",
    "IOSpec",
    "RunnerSpec",
]
