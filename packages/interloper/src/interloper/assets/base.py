"""Asset definition and execution."""

from __future__ import annotations

import copy
import inspect
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from interloper.assets.context import ExecutionContext
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.io.memory import MemoryIO
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.asset import AssetSpec
from interloper.serialization.base import Serializable
from interloper.utils.imports import get_object_path

if TYPE_CHECKING:
    from interloper.assets.context import ExecutionContext
    from interloper.dag.base import DAG
    from interloper.source.base import Source, SourceDefinition


@dataclass(frozen=True)
class AssetDefinition:
    """Definition of an asset created by the @asset decorator."""

    func: Callable[..., Any]
    source_definition: SourceDefinition | None = None
    name: str = ""
    schema: type[BaseModel] | None = None
    config: type[BaseSettings] | None = None
    io: IO | dict[str, IO] | None = None
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    default_io_key: str | None = None
    deps: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Set name to function name if not provided."""
        if not self.name:
            object.__setattr__(self, "name", getattr(self.func, "__name__", "unknown"))

    @property
    def key(self) -> str:
        """Return the unique key for this asset definition: dataset.name or name."""
        return f"{self.dataset}.{self.name}" if self.dataset else self.name

    def __call__(
        self,
        *,
        name: str | None = None,
        config: BaseSettings | None = None,
        io: IO | dict[str, IO] | None = None,
        deps: dict[str, str] | None = None,
        dataset: str | None = None,
        default_io_key: str | None = None,
        materializable: bool = True,
    ) -> Asset:
        """Instantiate the asset with runtime parameters."""
        # If config is provided, check it's the correct type (if self.config is set)
        if config is not None and self.config is not None and not issubclass(type(config), self.config):
            raise TypeError(
                f"Config provided to asset '{self.name}' must be of type {self.config.__name__}, "
                f"got {type(config).__name__}."
            )

        # Resolve config
        resolved_config = config
        if resolved_config is None and self.config is not None:
            # Try to load from environment
            try:
                resolved_config = self.config()
            except Exception as e:
                raise ValueError(
                    f"Config {self.config.__name__} is configured but cannot be resolved. "
                    f"Provide config explicitly or set environment variables. Error: {e}"
                ) from e

        # Merge deps: runtime override > definition-level
        merged_deps: dict[str, str] = dict(self.deps)
        if deps:
            merged_deps.update(deps)

        return Asset(
            func=self.func,
            name=name or self.name,
            schema=self.schema,
            config=resolved_config,
            io=io or self.io,
            partitioning=self.partitioning,
            dataset=dataset or self.dataset,
            default_io_key=default_io_key or self.default_io_key,
            deps=merged_deps,
            definition=self,
            materializable=materializable,
        )


@dataclass
class Asset(Serializable[AssetSpec]):
    """Runtime instance of an asset."""

    func: Callable
    name: str
    definition: AssetDefinition
    schema: type[BaseModel] | None = None
    config: BaseSettings | None = None
    io: IO | dict[str, IO] | None = None
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    default_io_key: str | None = None
    deps: dict[str, str] = field(default_factory=dict)
    source: Source | None = field(default=None, init=False, repr=False)
    materializable: bool = True

    def __post_init__(self) -> None:
        """Apply defaults after initialization.

        - If no IO is configured at definition or call-time, default to MemoryIO singleton.
        """
        if self.io is None:
            self.io = MemoryIO.singleton()

    def copy(
        self,
        config: BaseSettings | None = None,
        io: IO | dict[str, IO] | None = None,
        deps: dict[str, str] | None = None,
        dataset: str | None = None,
        materializable: bool | None = None,
    ) -> Asset:
        """Create a new asset instance with runtime parameters."""
        # Create a shallow copy and set attrs, since dataclasses.replace() fails on frozen/field-removed
        asset = copy.copy(self)
        if config is not None:
            asset.config = config
        if io is not None:
            asset.io = io
        if deps is not None:
            asset.deps = deps
        if dataset is not None:
            asset.dataset = dataset
        if materializable is not None:
            asset.materializable = materializable
        return asset

    @property
    def path(self) -> str:
        """Return the full import path of the decorated asset function.

        Overrides the base class path property.

        If the asset is a source, the path is the import path of the source function plus the asset name.
        If the asset is not a source, the path is the import path of the asset function.
        """
        if self.source:
            path = f"{get_object_path(self.source.func)}:{self.name}"
        else:
            path = get_object_path(self.func)  # Points to the actual function
        return path

    @property
    def key(self) -> str:
        """Return the unique key for this asset."""
        if self.source:
            return f"{self.source.name}.{self.name}"
        else:
            return self.name

    def run(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to IO.

        This method handles:
        - Context and config parameters
        - Upstream dependencies (loaded from IO via DAG)
        - Schema validation

        Args:
            partition_or_window: Either a Partition or PartitionWindow object
            dag: DAG for dependency resolution (required for assets with dependencies)
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id)

        Returns:
            The execution result
        """
        # Warn if partition provided for non-partitioned asset
        if self.partitioning is None and partition_or_window is not None:
            warnings.warn(f"Asset '{self.name}' is not partitioned, partition/partition_window will be ignored")

        # Create context
        context = ExecutionContext(
            partition_or_window=partition_or_window,
            asset_name=self.name,
            partitioning=self.partitioning,
            metadata=metadata,
        )

        # Build function kwargs with dependency resolution
        sig = inspect.signature(self.func)
        kwargs = self._build_kwargs(sig, context, partition_or_window, dag)

        # Execute function
        result = self.func(**kwargs)

        # Validate schema if provided
        if self.schema is not None:
            self._validate_schema(result)

        return result

    def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and write the result to all configured IOs.

        This method is equivalent to: run + write to IO(s)

        Args:
            partition_or_window: Either a Partition or PartitionWindow object
            dag: DAG for dependency resolution (required for assets with dependencies)
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id)

        Returns:
            The execution result
        """
        if not self.materializable:
            return None

        metadata = metadata or {}
        result = self.run(partition_or_window, dag, metadata)
        self._io_write(partition_or_window, metadata, result)
        return result

    def _io_write(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
        result: Any,
    ) -> None:
        """Write the result of the asset execution to all configured IO targets.

        Args:
            partition_or_window: Either a Partition or PartitionWindow object
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id)
            result: The execution result
        """
        if self.io is None:
            return

        io_context = IOContext(
            asset=self,
            partition_or_window=partition_or_window if self.partitioning is not None else None,
            metadata=metadata,
        )

        # Build list of (io_key, io) tuples
        if isinstance(self.io, dict):
            ios = list(self.io.items())
        else:
            ios = [(None, self.io)]

        partition_str = str(partition_or_window) if partition_or_window else None

        for io_key, io in ios:
            io_metadata = {
                **metadata,
                "asset_key": self.key,
                "partition_or_window": partition_str,
                "io_key": io_key,
            }
            emit(EventType.IO_WRITE_STARTED, metadata=io_metadata)
            try:
                io.write(io_context, result)  # ty:ignore[unresolved-attribute]
                emit(EventType.IO_WRITE_COMPLETED, metadata=io_metadata)
            except Exception as e:
                emit(EventType.IO_WRITE_FAILED, metadata={**io_metadata, "error": str(e)})
                raise e

    def _build_kwargs(
        self,
        sig: inspect.Signature,
        context: ExecutionContext,
        partition_or_window: Partition | PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build kwargs for asset function including dependencies.

        Handles:
        - Context and config parameters
        - Upstream dependencies (loaded from IO via DAG)

        Args:
            sig: Function signature
            context: Context object
            partition_or_window: Either a Partition or PartitionWindow object
            dag: DAG for dependency resolution

        Returns:
            Dictionary of kwargs for the function

        Raises:
            ValueError: If dependencies cannot be resolved
        """
        kwargs: dict[str, Any] = {}

        for param_name in sig.parameters:
            if param_name == "context":
                kwargs["context"] = context
            elif param_name == "config":
                kwargs["config"] = self.config
            else:
                # This is a dependency - load from IO via DAG
                if dag is None:
                    raise ValueError(
                        f"Asset '{self.name}' has dependencies but no DAG provided. "
                        "Pass a DAG to run() or materialize() for dependency resolution."
                    )

                # Resolve dependency key: check explicit mapping first, then infer
                if param_name in self.deps:
                    # Explicit mapping provided
                    upstream_key = self.deps[param_name]
                else:
                    # Infer from parameter name - assume same dataset
                    if self.dataset:
                        upstream_key = f"{self.dataset}.{param_name}"
                    else:
                        upstream_key = param_name

                if upstream_key not in dag.asset_map:
                    raise ValueError(f"Dependency '{upstream_key}' not found in DAG for asset '{self.name}'")

                upstream_asset = dag.asset_map[upstream_key]

                # Determine which IO to read from
                read_io = None
                read_io_key = None
                if isinstance(upstream_asset.io, dict):
                    # Use default_io_key
                    read_io_key = upstream_asset.default_io_key
                    if read_io_key:
                        read_io = upstream_asset.io[read_io_key]
                else:
                    read_io = upstream_asset.io

                if read_io is None:
                    raise ValueError(f"No IO found for upstream asset '{upstream_asset.name}'")

                # Load data from IO using upstream's partitioning rules
                if upstream_asset.partitioning is not None:
                    effective_partition_or_window = partition_or_window
                else:
                    effective_partition_or_window = None

                io_context = IOContext(
                    asset=upstream_asset,
                    partition_or_window=effective_partition_or_window,
                    metadata=context.metadata,
                )

                partition_str = str(effective_partition_or_window) if effective_partition_or_window else None
                io_metadata = {
                    **context.metadata,
                    **get_asset_event_metadata(self),
                    "partition_or_window": partition_str,
                    "io_key": read_io_key,
                }
                emit(EventType.IO_READ_STARTED, metadata=io_metadata)
                try:
                    kwargs[param_name] = read_io.read(io_context)
                    emit(EventType.IO_READ_COMPLETED, metadata=io_metadata)
                except Exception as e:
                    emit(EventType.IO_READ_FAILED, metadata={**io_metadata, "error": str(e)})
                    raise ValueError(f"Failed to load data from upstream asset '{upstream_asset.name}': {e}") from e

        return kwargs

    def _validate_schema(self, data: Any) -> None:
        """Validate data against schema.

        Note: intentionally left unimplemented for now per requirements.
        """
        # TODO
        return

    def to_spec(self) -> AssetSpec:
        """Convert to serializable spec."""
        # Serialize IO if present
        io_spec = None
        if isinstance(self.io, dict):
            io_spec = {k: v.to_spec() for k, v in self.io.items()}  # type: ignore[unresolved-attribute]
        elif self.io is not None:
            io_spec = self.io.to_spec()

        return AssetSpec(
            path=self.path,
            io=io_spec,  # ty:ignore[invalid-argument-type]
            materializable=self.materializable,
            config=self.config.model_dump() if self.config is not None else None,
        )
