"""Interloper - A Python framework for building and executing data pipelines."""

from interloper.assets import Asset, AssetDefinition, asset
from interloper.assets.context import ExecutionContext
from interloper.assets.keys import AssetDefinitionKey, AssetInstanceKey
from interloper.backfillers import Backfiller
from interloper.backfillers.results import BackfillResult
from interloper.backfillers.serial import SerialBackfiller
from interloper.dag.base import DAG
from interloper.events.base import Event, EventBus, EventType, emit, subscribe, unsubscribe
from interloper.io import IO, FileIO, IOContext, MemoryIO
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
from interloper.serialization import AssetSpec, BackfillerSpec, ConfigSpec, DAGSpec, IOSpec, RunnerSpec
from interloper.source import Source, SourceDefinition, source
from interloper.source.config import Config

__version__ = "0.1.0"

__all__ = [
    "asset",
    "Asset",
    "AssetDefinition",
    "AssetDefinitionKey",
    "AssetExecutionInfo",
    "AssetInstanceKey",
    "AssetSpec",
    "Backfiller",
    "BackfillerSpec",
    "BackfillResult",
    "Config",
    "ConfigSpec",
    "DAG",
    "DAGSpec",
    "emit",
    "Event",
    "EventBus",
    "EventType",
    "ExecutionContext",
    "ExecutionStatus",
    "FileIO",
    "HTTPBearerAuth",
    "IO",
    "IOContext",
    "IOSpec",
    "MemoryIO",
    "MultiProcessRunner",
    "MultiThreadRunner",
    "OAuth2Auth",
    "OAuth2ClientCredentialsAuth",
    "OAuth2RefreshTokenAuth",
    "Partition",
    "PartitionConfig",
    "PartitionWindow",
    "RESTClient",
    "Runner",
    "RunnerSpec",
    "RunResult",
    "SerialBackfiller",
    "SerialRunner",
    "source",
    "Source",
    "SourceDefinition",
    "subscribe",
    "TimePartition",
    "TimePartitionConfig",
    "TimePartitionWindow",
    "unsubscribe",
]
