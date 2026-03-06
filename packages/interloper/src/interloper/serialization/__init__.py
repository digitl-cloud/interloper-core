"""Specs for serializing framework objects into portable Pydantic models.

Each Spec can reconstruct the original object from its serialized form,
enabling cross-process transport and persistent configuration.
"""

from interloper.serialization.asset import AssetInstanceSpec
from interloper.serialization.backfiller import BackfillerInstanceSpec
from interloper.serialization.config import ConfigInstanceSpec
from interloper.serialization.dag import DAGInstanceSpec
from interloper.serialization.io import IOInstanceSpec
from interloper.serialization.runner import RunnerInstanceSpec

__all__ = [
    "AssetInstanceSpec",
    "BackfillerInstanceSpec",
    "ConfigInstanceSpec",
    "DAGInstanceSpec",
    "IOInstanceSpec",
    "RunnerInstanceSpec",
]
