"""Asset definition and execution."""

from __future__ import annotations

import copy
import inspect
import traceback
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel

from interloper.assets.context import ExecutionContext
from interloper.assets.keys import AssetDefinitionKey, AssetInstanceKey
from interloper.errors import AssetError, ConfigError, DependencyNotFoundError, PartitionError
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.io.base import IO
from interloper.io.context import IOContext
from interloper.io.memory import MemoryIO
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.asset import AssetSpec
from interloper.serialization.base import Serializable
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_name

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.normalizer.base import Normalizer
    from interloper.normalizer.strategy import MaterializationStrategy
    from interloper.source.base import Source, SourceDefinition
    from interloper.source.config import Config


@dataclass(frozen=True)
class AssetDefinition:
    """Definition of an asset created by the @asset decorator."""

    func: Callable[..., Any]
    source_definition: SourceDefinition | None = None
    name: str = ""
    label: str = ""
    schema: type[BaseModel] | None = None
    config: type[Config] | None = None
    io: IO | None = None
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None
    tags: tuple[str, ...] = ()
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    requires: dict[str, AssetDefinitionKey] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Set name to function name if not provided, validate."""
        if not self.name:
            object.__setattr__(self, "name", getattr(self.func, "__name__", "unknown"))

        validate_name(self.name)

        if not self.label:
            object.__setattr__(self, "label", to_label(self.name))

    @property
    def definition_key(self) -> AssetDefinitionKey:
        """Return the asset definition key.

        Format: ``{source-definition-key}:{asset-name}`` for source-bound assets,
        or just ``{asset-name}`` for standalone assets.
        """
        if self.source_definition:
            return AssetDefinitionKey(f"{self.source_definition.name}:{self.name}")
        return AssetDefinitionKey(self.name)

    def __call__(
        self,
        *,
        name: str | None = None,
        config: Config | None = None,
        io: IO | dict[str, IO] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
        dataset: str | None = None,
        default_io_key: str | None = None,
        materializable: bool = True,
        strategy: MaterializationStrategy | None = None,
    ) -> Asset:
        """Instantiate the asset with runtime parameters.

        Args:
            name: Override asset name.
            config: Override config instance.
            io: Override IO backend.
            deps: Explicit dependency mapping (param name → asset instance key).
            dataset: Override dataset name.
            default_io_key: Override default IO key for multi-IO setups.
            materializable: Whether the asset can be materialized.
            strategy: Override materialization strategy.
        """
        if name is not None:
            validate_name(name)

        # If config is provided, check it's the correct type (if self.config is set)
        if config is not None and self.config is not None and not issubclass(type(config), self.config):
            raise ConfigError(
                f"Config provided to asset '{self.name}' must be of type {self.config.__name__}, "
                f"got {type(config).__name__}."
            )

        if config is not None and self.config is None and not self.source_definition:
            warnings.warn(
                f"Config provided to asset '{self.name}' but no config type is configured "
                f"on the @asset decorator. The config will be used but cannot be type-checked.",
                UserWarning,
                stacklevel=2,
            )

        # Resolve config
        resolved_config = config
        if resolved_config is None and self.config is not None:
            # Try to load from environment
            try:
                resolved_config = self.config()
            except Exception as e:
                raise ConfigError(
                    f"Config {self.config.__name__} is configured but cannot be resolved. "
                    f"Provide config explicitly or set environment variables. Error: {e}"
                ) from e

        return Asset(
            func=self.func,
            name=name or self.name,
            schema=self.schema,
            config=resolved_config,
            io=io or self.io,
            normalizer=self.normalizer,
            strategy=strategy or self.strategy,
            partitioning=self.partitioning,
            dataset=dataset or self.dataset,
            default_io_key=default_io_key,
            deps=deps or {},
            definition=self,
            materializable=materializable,
        )


@dataclass
class Asset(Serializable[AssetSpec]):
    """Runtime instance of an asset."""

    func: Callable
    definition: AssetDefinition
    name: str
    label: str = ""
    schema: type[BaseModel] | None = None
    config: Config | None = None
    io: IO | dict[str, IO] | None = None
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    default_io_key: str | None = None
    deps: dict[str, AssetInstanceKey] = field(default_factory=dict)
    source: Source | None = field(default=None, init=False, repr=False)
    materializable: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Apply defaults after initialization."""
        if not self.label:
            object.__setattr__(self, "label", self.definition.label)

        if self.io is None:
            self.io = MemoryIO.singleton()

    @property
    def instance_key(self) -> AssetInstanceKey:
        """Return the unique key for this asset instance."""
        if self.source:
            return AssetInstanceKey(f"{self.source.instance_key}:{self.name}")
        return AssetInstanceKey(self.name)

    @property
    def definition_key(self) -> AssetDefinitionKey:
        """Return the asset definition key."""
        return self.definition.definition_key

    def copy(
        self,
        config: Config | None = None,
        io: IO | dict[str, IO] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
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

        If the asset belongs to a source, the path is the source class path plus the asset name.
        If the asset is standalone, the path is the import path of the asset function.
        """
        if self.source:
            path = f"{get_object_path(self.source.definition.cls)}:{self.name}"
        else:
            path = get_object_path(self.func)  # Points to the actual function
        return path

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

        if self.partitioning is not None and partition_or_window is None:
            raise PartitionError(f"Asset '{self.name}' is partitioned, but no partition/partition_window provided")

        if (
            self.partitioning is not None
            and isinstance(partition_or_window, PartitionWindow)
            and not self.partitioning.allow_window
        ):
            raise PartitionError(
                f"Asset '{self.instance_key}' does not support windowed runs (allow_window=False). "
                "Use a partition window with backfill(windowed=False) to run one partition per run."
            )

        # Create context
        context = ExecutionContext(
            asset_key=self.instance_key,
            partition_or_window=partition_or_window,
            partitioning=self.partitioning,
            metadata=metadata,
        )

        # Build function kwargs with dependency resolution
        kwargs = self._build_kwargs(context, partition_or_window, dag)

        # Execute core function
        exec_metadata = {
            **(metadata or {}),
            **get_asset_event_metadata(self),
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }
        emit(EventType.ASSET_EXEC_STARTED, metadata=exec_metadata)
        try:
            result = self.func(**kwargs)
            emit(EventType.ASSET_EXEC_COMPLETED, metadata=exec_metadata)
        except Exception as e:
            emit(
                EventType.ASSET_EXEC_FAILED,
                metadata={
                    **exec_metadata,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                },
            )
            raise

        # Apply normalizer if configured
        if self.normalizer is not None:
            from interloper.normalizer.strategy import MaterializationStrategy

            result = self.normalizer.normalize(result)
            strategy = self.strategy or MaterializationStrategy.AUTO

            if strategy == MaterializationStrategy.RECONCILE:
                if self.schema is None:
                    raise AssetError(f"Asset '{self.name}': strategy='reconcile' requires a schema.")
                result = self.normalizer.reconcile(result, self.schema)

            elif strategy == MaterializationStrategy.STRICT:
                if self.schema is None:
                    raise AssetError(f"Asset '{self.name}': strategy='strict' requires a schema.")
                self.normalizer.validate_schema(result, self.schema, strict=True)

            else:
                if self.schema is None and self.normalizer.infer:
                    self.schema = self.normalizer.infer_schema(result)
                elif self.schema is not None:
                    self.normalizer.validate_schema(result, self.schema)

        elif self.schema is not None:
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
                **get_asset_event_metadata(self),
                "partition_or_window": partition_str,
                "io_key": io_key,
            }
            emit(EventType.IO_WRITE_STARTED, metadata=io_metadata)
            try:
                io.write(io_context, result)
                emit(EventType.IO_WRITE_COMPLETED, metadata=io_metadata)
            except Exception as e:
                emit(
                    EventType.IO_WRITE_FAILED,
                    metadata={
                        **io_metadata,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                    },
                )
                raise

    def _build_kwargs(
        self,
        context: ExecutionContext,
        partition_or_window: Partition | PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build kwargs for asset function including dependencies.

        Handles:
        - Context and config parameters
        - Upstream dependencies (loaded from IO via DAG)

        Args:
            context: Context object
            partition_or_window: Either a Partition or PartitionWindow object
            dag: DAG for dependency resolution

        Returns:
            Dictionary of kwargs for the function

        Raises:
            AssetError: If dependencies cannot be resolved
        """
        kwargs: dict[str, Any] = {}
        sig = inspect.signature(self.func)

        for param_name in sig.parameters:
            if param_name == "context":
                kwargs["context"] = context
            elif param_name == "config":
                kwargs["config"] = self.config
            else:
                # This is a dependency - load from IO via DAG
                if dag is None:
                    raise AssetError(
                        f"Asset '{self.name}' has dependencies but no DAG provided. "
                        "Pass a DAG to run() or materialize() for dependency resolution."
                    )

                upstream_key = dag.resolve_dependency_key(self, param_name)

                if upstream_key not in dag.asset_map:
                    raise DependencyNotFoundError(
                        f"Dependency '{upstream_key}' not found in DAG for asset '{self.name}'"
                    )

                upstream_asset = dag.asset_map[upstream_key]

                # Determine which IO to read from
                read_io = None
                read_io_key = None
                if isinstance(upstream_asset.io, dict):
                    # Use default_io_key
                    io_dict = cast(dict[str, IO], upstream_asset.io)
                    read_io_key = upstream_asset.default_io_key
                    if read_io_key:
                        read_io = io_dict[read_io_key]
                else:
                    read_io = upstream_asset.io

                if read_io is None:
                    raise AssetError(f"No IO found for upstream asset '{upstream_asset.name}'")

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
                    emit(
                        EventType.IO_READ_FAILED,
                        metadata={
                            **io_metadata,
                            "error": str(e),
                            "traceback": traceback.format_exc(),
                        },
                    )
                    raise AssetError(f"Failed to load data from upstream asset '{upstream_asset.name}': {e}") from e

        return kwargs

    def _validate_schema(self, data: Any) -> None:
        """Validate data against schema.

        Delegates to :func:`~interloper.normalizer.schema.validate_schema`
        when data is ``list[dict]``.
        """
        if self.schema is None:
            return

        if isinstance(data, list) and data and isinstance(data[0], dict):
            from interloper.normalizer.schema import validate_schema

            validate_schema(data, self.schema)

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
            io=io_spec,
            materializable=self.materializable,
            config=self.config.model_dump() if self.config is not None else None,
        )
