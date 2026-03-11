"""Interloper - A Python framework for building and executing data pipelines."""

from interloper.assets import Asset, AssetDefinition, asset
from interloper.assets.context import EventLogger, ExecutionContext
from interloper.assets.keys import AssetDefinitionKey, AssetInstanceKey
from interloper.backfillers import Backfiller
from interloper.backfillers.results import BackfillResult
from interloper.backfillers.serial import SerialBackfiller
from interloper.dag.base import DAG
from interloper.destination import CsvDestination, Destination, DestinationContext, FileDestination, MemoryDestination
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
    InterloperDestinationError,
    InterloperError,
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
from interloper.serialization import (
    AssetInstanceSpec,
    ComponentInstanceSpec,
    ConfigInstanceSpec,
    DAGInstanceSpec,
)
from interloper.source import Source, SourceDefinition, source
from interloper.source.config import Config

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "AdapterError",
    "Asset",
    "AssetDefinition",
    "AssetDefinitionKey",
    "AssetError",
    "AssetExecutionInfo",
    "AssetInstanceKey",
    "AssetInstanceSpec",
    "AssetNotFoundError",
    "AssetSchema",
    "AuthenticationError",
    "BackfillError",
    "BackfillResult",
    "Backfiller",
    "CircularDependencyError",
    "ComponentInstanceSpec",
    "Config",
    "ConfigError",
    "ConfigInstanceSpec",
    "CsvDestination",
    "DAGError",
    "DAGInstanceSpec",
    "DataNotFoundError",
    "DependencyNotFoundError",
    "Destination",
    "DestinationContext",
    "Event",
    "EventBus",
    "EventError",
    "EventLogger",
    "EventType",
    "ExecutionContext",
    "ExecutionStatus",
    "FileDestination",
    "HTTPBearerAuth",
    "InterloperDestinationError",
    "InterloperError",
    "LogLevel",
    "MaterializationStrategy",
    "MemoryDestination",
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
