"""Specs for serializing framework objects into portable Pydantic models.

The module provides two families of specs:

- **InstanceSpec** subclasses capture runtime state and can reconstruct
  the original object, enabling cross-process transport and persistent
  configuration.
- **DefinitionSpec** subclasses capture definition metadata for API
  responses, frontend display, and introspection.
"""

from interloper.serialization.asset import AssetDefinitionSpec, AssetInstanceSpec
from interloper.serialization.base import (
    Component,
    ComponentDefinitionSpec,
    ComponentInstanceSpec,
    DefinitionSpec,
    Spec,
    reconstruct_components,
    reconstruct_config,
)
from interloper.serialization.config import ConfigInstanceSpec
from interloper.serialization.dag import DAGInstanceSpec
from interloper.serialization.schema import SchemaFieldSpec, extract_schema_fields
from interloper.serialization.source import SourceDefinitionSpec, SourceInstanceSpec

__all__ = [
    "AssetDefinitionSpec",
    "AssetInstanceSpec",
    "Component",
    "ComponentDefinitionSpec",
    "ComponentInstanceSpec",
    "ConfigInstanceSpec",
    "DAGInstanceSpec",
    "DefinitionSpec",
    "SchemaFieldSpec",
    "SourceDefinitionSpec",
    "SourceInstanceSpec",
    "Spec",
    "extract_schema_fields",
    "reconstruct_components",
    "reconstruct_config",
]
