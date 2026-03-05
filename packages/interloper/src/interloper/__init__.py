"""Interloper - A Python framework for building and executing data pipelines."""

from interloper.assets import Asset, AssetDefinition, asset
from interloper.assets.context import EventLogger, ExecutionContext
from interloper.assets.keys import AssetDefinitionKey, AssetInstanceKey
from interloper.backfillers import Backfiller
from interloper.backfillers.results import BackfillResult
from interloper.backfillers.serial import SerialBackfiller
from interloper.dag.base import DAG
from interloper.errors import (
    AdapterError,
    AssetError,
    AssetNotFoundError,
    AuthenticationError,
    BackfillError,
    CircularDependencyError,
    ConfigError,
    DAGError,
    DataNotFoundError,
    DependencyNotFoundError,
    EventError,
    InterloperError,
    InterloperIOError,
    NormalizerError,
    PartitionError,
    RunnerError,
    SchemaError,
    ScriptLoadError,
    SourceError,
    TableNotFoundError,
)
from interloper.events.base import (
    Event,
    EventBus,
    EventType,
    LogLevel,
    disable_event_forwarding,
    emit,
    enable_event_forwarding,
    subscribe,
    unsubscribe,
)
from interloper.io import IO, FileIO, IOContext, MemoryIO
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.partitioning import (
    Partition,
    PartitionConfig,
    PartitionWindow,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)
from interloper.rest import HTTPBearerAuth, OAuth2Auth, OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, RESTClient
from interloper.runners import MultiProcessRunner, MultiThreadRunner, Runner, SerialRunner
from interloper.runners.results import AssetExecutionInfo, ExecutionStatus, RunResult
from interloper.schema import AssetSchema
from interloper.serialization import AssetSpec, BackfillerSpec, ConfigSpec, DAGSpec, IOSpec, RunnerSpec
from interloper.source import Source, SourceDefinition, source
from interloper.source.config import Config

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "IO",
    "AdapterError",
    "Asset",
    "AssetDefinition",
    "AssetDefinitionKey",
    "AssetError",
    "AssetExecutionInfo",
    "AssetInstanceKey",
    "AssetNotFoundError",
    "AssetSchema",
    "AssetSpec",
    "AuthenticationError",
    "BackfillError",
    "BackfillResult",
    "Backfiller",
    "BackfillerSpec",
    "CircularDependencyError",
    "Config",
    "ConfigError",
    "ConfigSpec",
    "DAGError",
    "DAGSpec",
    "DataNotFoundError",
    "DependencyNotFoundError",
    "Event",
    "EventBus",
    "EventError",
    "EventLogger",
    "EventType",
    "ExecutionContext",
    "ExecutionStatus",
    "FileIO",
    "HTTPBearerAuth",
    "IOContext",
    "IOSpec",
    "InterloperError",
    "InterloperIOError",
    "LogLevel",
    "MaterializationStrategy",
    "MemoryIO",
    "MultiProcessRunner",
    "MultiThreadRunner",
    "Normalizer",
    "NormalizerError",
    "OAuth2Auth",
    "OAuth2ClientCredentialsAuth",
    "OAuth2RefreshTokenAuth",
    "Partition",
    "PartitionConfig",
    "PartitionError",
    "PartitionWindow",
    "RESTClient",
    "RunResult",
    "Runner",
    "RunnerError",
    "RunnerSpec",
    "SchemaError",
    "ScriptLoadError",
    "SerialBackfiller",
    "SerialRunner",
    "Source",
    "SourceDefinition",
    "SourceError",
    "TableNotFoundError",
    "TimePartition",
    "TimePartitionConfig",
    "TimePartitionWindow",
    "asset",
    "disable_event_forwarding",
    "emit",
    "enable_event_forwarding",
    "source",
    "subscribe",
    "unsubscribe",
]
