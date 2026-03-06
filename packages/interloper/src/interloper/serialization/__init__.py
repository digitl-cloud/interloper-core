"""Specs for serializing framework objects into portable Pydantic models.

The module provides two families of specs:

- **InstanceSpec** subclasses capture runtime state and can reconstruct
  the original object, enabling cross-process transport and persistent
  configuration.
- **DefinitionSpec** subclasses capture definition metadata for API
  responses, frontend display, and introspection.
"""

from interloper.serialization.asset import AssetDefinitionSpec, AssetInstanceSpec
from interloper.serialization.backfiller import BackfillerInstanceSpec
from interloper.serialization.base import DefinitionSpec, PathInitSpec, Spec
from interloper.serialization.config import ConfigInstanceSpec
from interloper.serialization.dag import DAGInstanceSpec
from interloper.serialization.io import IOInstanceSpec
from interloper.serialization.runner import RunnerInstanceSpec
from interloper.serialization.schema import SchemaFieldSpec, extract_schema_fields
from interloper.serialization.source import SourceDefinitionSpec, SourceInstanceSpec

__all__ = [
    "AssetDefinitionSpec",
    "AssetInstanceSpec",
    "BackfillerInstanceSpec",
    "ConfigInstanceSpec",
    "DAGInstanceSpec",
    "DefinitionSpec",
    "IOInstanceSpec",
    "PathInitSpec",
    "RunnerInstanceSpec",
    "SchemaFieldSpec",
    "SourceDefinitionSpec",
    "SourceInstanceSpec",
    "Spec",
    "extract_schema_fields",
]
