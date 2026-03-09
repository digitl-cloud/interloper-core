"""Asset definition and execution."""

from __future__ import annotations

import copy
import inspect
import traceback
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from interloper.assets.context import ExecutionContext
from interloper.assets.keys import AssetDefinitionKey, AssetInstanceKey
from interloper.errors import AssetError, ConfigError, DependencyNotFoundError, PartitionError
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.io.base import IO, validate_io_keys
from interloper.io.context import IOContext
from interloper.io.memory import MemoryIO
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.asset import AssetDefinitionSpec, AssetInstanceSpec
from interloper.serialization.base import Component, Serializable
from interloper.serialization.schema import extract_schema_fields
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_key

if TYPE_CHECKING:
    from interloper.dag.base import DAG
    from interloper.normalizer.base import Normalizer
    from interloper.normalizer.strategy import MaterializationStrategy
    from interloper.source.base import Source, SourceDefinition
    from interloper.source.config import Config


@dataclass(frozen=True)
class AssetDefinition(Serializable):
    """Definition of an asset created by the @asset decorator."""

    func: Callable[..., Any]
    source_definition: SourceDefinition | None = None
    key: str = ""
    label: str = ""
    schema: type[BaseModel] | None = None
    config: type[Config] | None = None
    io: IO | list[IO] | None = None
    default_io_key: str | None = None
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None
    tags: tuple[str, ...] = ()
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    requires: dict[str, AssetDefinitionKey] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Set key to function name if not provided, validate."""
        if not self.key:
            object.__setattr__(self, "key", getattr(self.func, "__name__", "unknown"))

        validate_key(self.key)

        if not self.label:
            object.__setattr__(self, "label", to_label(self.key))

    def to_spec(self) -> AssetDefinitionSpec:
        """Convert to a definition spec describing this asset's metadata.

        Returns:
            An AssetDefinitionSpec capturing key, label, description,
            tags, config_schema, dependencies, and schema fields.
        """
        config_schema = None
        if self.config is not None:
            config_schema = self.config.model_json_schema()

        return AssetDefinitionSpec(
            key=self.qualified_key,
            label=self.label,
            description=self.func.__doc__ or "",
            tags=list(self.tags),
            config_schema=config_schema,
            requires=dict(self.requires) if self.requires else None,
            schema_fields=extract_schema_fields(self.schema),
        )

    @property
    def qualified_key(self) -> AssetDefinitionKey:
        """Return the fully qualified asset definition key.

        Format: ``{source-key}:{asset-key}`` for source-bound assets,
        or just ``{asset-key}`` for standalone assets.
        """
        if self.source_definition:
            return AssetDefinitionKey(f"{self.source_definition.key}:{self.key}")
        return AssetDefinitionKey(self.key)

    def __call__(
        self,
        *,
        key: str | None = None,
        config: Config | None = None,
        io: IO | list[IO] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
        dataset: str | None = None,
        default_io_key: str | None = None,
        materializable: bool = True,
        strategy: MaterializationStrategy | None = None,
    ) -> Asset:
        """Instantiate an ``Asset`` from this definition with runtime overrides.

        Args:
            key: Override the asset key.
            config: Override the config instance.
            io: Override the IO backend (single IO or list of IOs keyed by ``io.key``).
            deps: Explicit dependency mapping (param name to asset instance key).
            dataset: Override the dataset name.
            default_io_key: Default IO key for multi-IO setups.
            materializable: Whether the asset can be materialized.
            strategy: Override the materialization strategy.

        Returns:
            A new Asset instance with the given overrides applied.

        Raises:
            ConfigError: If the provided config does not match the expected type.
        """
        if key is not None:
            validate_key(key)

        # If config is provided, check it's the correct type (if self.config is set)
        if config is not None and self.config is not None and not issubclass(type(config), self.config):
            raise ConfigError(
                f"Config provided to asset '{self.key}' must be of type {self.config.__name__}, "
                f"got {type(config).__name__}."
            )

        if config is not None and self.config is None and not self.source_definition:
            warnings.warn(
                f"Config provided to asset '{self.key}' but no config type is configured "
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
            key=AssetInstanceKey(key or self.key),
            schema=self.schema,
            config=resolved_config,
            io=io or self.io,
            normalizer=self.normalizer,
            strategy=strategy or self.strategy,
            partitioning=self.partitioning,
            dataset=dataset or self.dataset,
            default_io_key=default_io_key or self.default_io_key,
            deps=deps or {},
            definition=self,
            materializable=materializable,
        )


class Asset(Component):
    """Runtime instance of an asset."""

    func: Callable
    definition: AssetDefinition
    key: AssetInstanceKey = AssetInstanceKey("")
    label: str = ""
    schema: type[BaseModel] | None = None
    config: Config | None = None
    io: IO | list[IO] | None = None
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    default_io_key: str | None = None
    deps: dict[str, AssetInstanceKey] = Field(default_factory=dict)
    source: Source | None = Field(default=None, init=False, exclude=True, repr=False)
    materializable: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any, /) -> None:
        """Apply defaults after initialization.

        Raises:
            ConfigError: If multiple IOs are configured without a valid
                ``default_io_key``, or if IO keys collide.
        """
        if not self.label:
            self.label = self.definition.label

        if self.io is None:
            self.io = MemoryIO.singleton()

        if isinstance(self.io, list) and len(self.io) == 1:
            self.io = self.io[0]

        if isinstance(self.io, list):
            validate_io_keys(self.io, self.key)
            if not self.default_io_key:
                raise ConfigError(
                    f"Asset '{self.key}' has multiple IOs but no default_io_key. "
                    "Set default_io_key to specify which IO to use for upstream reads."
                )
            if not any(io.key == self.default_io_key for io in self.io):
                available = sorted(io.key for io in self.io)
                raise ConfigError(
                    f"default_io_key '{self.default_io_key}' not found in IO list "
                    f"for asset '{self.key}'. Available keys: {available}"
                )

        if self.partitioning is not None and self.schema is not None:
            schema_fields = set(self.schema.model_fields.keys())
            if self.partitioning.column not in schema_fields:
                import warnings

                warnings.warn(
                    f"Asset '{self.key}': partition column '{self.partitioning.column}' "
                    f"not found in schema fields {sorted(schema_fields)}.",
                    UserWarning,
                    stacklevel=2,
                )

    @property
    def local_key(self) -> str:
        """Return the local (unqualified) key, stripping the source prefix if present."""
        return self.key.rsplit(":", 1)[-1]

    @property
    def qualified_key(self) -> AssetDefinitionKey:
        """Return the asset definition key."""
        return self.definition.qualified_key

    def copy(
        self,
        config: Config | None = None,
        io: IO | list[IO] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
        dataset: str | None = None,
        materializable: bool | None = None,
    ) -> Asset:
        """Return a shallow copy of this asset with optional overrides."""
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
        """Return the fully-qualified path used to locate this asset.

        For source-bound assets: ``{source-class-path}:{asset-local-key}``.
        For standalone assets: the import path of the decorated function.
        """
        if self.source:
            path = f"{get_object_path(self.source.definition.cls)}:{self.local_key}"
        else:
            path = get_object_path(self.func)  # Points to the actual function
        return path

    def _event_metadata(
        self,
        metadata: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None = None,
        *,
        io_key: str | None = None,
    ) -> dict[str, Any]:
        """Build the base event metadata dict for this asset.

        Merges run-level metadata with asset identity fields.  IO methods
        pass ``io_key`` to include the IO key in the metadata.

        Args:
            metadata: Run-level metadata (e.g. run_id, backfill_id).
            partition_or_window: Current partition scope.
            io_key: IO key for IO read/write events.

        Returns:
            The merged metadata dict (without ``message``).
        """
        base: dict[str, Any] = {
            **metadata,
            **get_asset_event_metadata(self),
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }
        if io_key is not None:
            base["io_key"] = io_key
        return base

    def run(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to IO.

        Resolves context, config, and upstream dependencies (via DAG), then
        runs the decorated function and applies schema validation.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has deps).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.

        Raises:
            AssetError: If schema validation or normalizer reconciliation fails.
            PartitionError: If partitioning requirements are not met.
        """
        # Warn if partition provided for non-partitioned asset
        if self.partitioning is None and partition_or_window is not None:
            warnings.warn(f"Asset '{self.key}' is not partitioned, partition/partition_window will be ignored")

        if self.partitioning is not None and partition_or_window is None:
            raise PartitionError(f"Asset '{self.key}' is partitioned, but no partition/partition_window provided")

        if (
            self.partitioning is not None
            and isinstance(partition_or_window, PartitionWindow)
            and not self.partitioning.allow_window
        ):
            raise PartitionError(
                f"Asset '{self.key}' does not support windowed runs (allow_window=False). "
                "Use a partition window with backfill(windowed=False) to run one partition per run."
            )

        # Create context
        context = ExecutionContext(
            asset_key=self.key,
            partition_or_window=partition_or_window,
            partitioning=self.partitioning,
            metadata=metadata,
        )

        # Build function kwargs with dependency resolution
        kwargs = self._build_kwargs(context, partition_or_window, dag)

        # Execute core function
        exec_metadata = self._event_metadata(metadata or {}, partition_or_window)
        msg = f"Executing '{self.key}'"
        emit(EventType.ASSET_EXEC_STARTED, metadata={**exec_metadata, "message": msg})
        try:
            result = self.func(**kwargs)
            msg = f"Executed '{self.key}'"
            emit(EventType.ASSET_EXEC_COMPLETED, metadata={**exec_metadata, "message": msg})
        except Exception as e:
            emit(
                EventType.ASSET_EXEC_FAILED,
                metadata={
                    **exec_metadata,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Execution of '{self.key}' failed: {e}",
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
                    raise AssetError(f"Asset '{self.key}': strategy='reconcile' requires a schema.")
                result = self.normalizer.reconcile(result, self.schema)

            elif strategy == MaterializationStrategy.STRICT:
                if self.schema is None:
                    raise AssetError(f"Asset '{self.key}': strategy='strict' requires a schema.")
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

        Equivalent to calling ``run()`` followed by writing to every IO target.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has deps).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The execution result, or ``None`` if the asset is not materializable.
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
        """Write the execution result to all configured IO targets.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
            result: The value to write.
        """
        if self.io is None:
            return

        io_context = IOContext(
            asset=self,
            partition_or_window=partition_or_window if self.partitioning is not None else None,
            metadata=metadata,
        )

        # Build list of IOs to write to
        ios = self.io if isinstance(self.io, list) else [self.io]

        for io in ios:
            io_key = io.key
            io_label = f"{io}[{io_key}]"
            io_metadata = self._event_metadata(metadata, partition_or_window, io_key=io_key)
            msg = f"Writing '{self.key}' to {io_label}"
            emit(EventType.IO_WRITE_STARTED, metadata={**io_metadata, "message": msg})
            try:
                io.write(io_context, result)
                msg = f"Wrote '{self.key}' to {io_label}"
                emit(EventType.IO_WRITE_COMPLETED, metadata={**io_metadata, "message": msg})
            except Exception as e:
                emit(
                    EventType.IO_WRITE_FAILED,
                    metadata={
                        **io_metadata,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                        "message": f"Failed to write '{self.key}' to {io_label}: {e}",
                    },
                )
                raise

    def _io_read(
        self,
        upstream_asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> Any:
        """Read data from an upstream asset's IO.

        Args:
            upstream_asset: The upstream asset to read from.
            partition_or_window: Partition or PartitionWindow for this run.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The data read from the upstream asset's IO.

        Raises:
            AssetError: If no IO is found or the read fails.
        """
        if isinstance(upstream_asset.io, list):
            # default_io_key is guaranteed non-None (validated in model_post_init)
            read_io_key = upstream_asset.default_io_key
            read_io = next(io for io in upstream_asset.io if io.key == read_io_key)
        else:
            read_io_key = None
            read_io = upstream_asset.io

        if read_io is None:
            raise AssetError(f"No IO found for upstream asset '{upstream_asset.key}'")

        if upstream_asset.partitioning is not None:
            effective_partition_or_window = partition_or_window
        else:
            effective_partition_or_window = None

        io_context = IOContext(
            asset=upstream_asset,
            partition_or_window=effective_partition_or_window,
            metadata=metadata,
        )

        io_label = f"{read_io}[{read_io_key}]" if read_io_key else str(read_io)
        io_metadata = self._event_metadata(metadata, effective_partition_or_window, io_key=read_io_key)
        msg = f"Reading '{upstream_asset.key}' from {io_label}"
        emit(EventType.IO_READ_STARTED, metadata={**io_metadata, "message": msg})
        try:
            result = read_io.read(io_context)
            msg = f"Read '{upstream_asset.key}' from {io_label}"
            emit(EventType.IO_READ_COMPLETED, metadata={**io_metadata, "message": msg})
        except Exception as e:
            emit(
                EventType.IO_READ_FAILED,
                metadata={
                    **io_metadata,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Failed to read '{upstream_asset.key}' from {io_label}: {e}",
                },
            )
            raise AssetError(f"Failed to load data from upstream asset '{upstream_asset.key}': {e}") from e

        return result

    def _build_kwargs(
        self,
        context: ExecutionContext,
        partition_or_window: Partition | PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build kwargs for the asset function.

        Maps function parameters to their values: ``context`` and ``config``
        are injected directly, all other parameters are treated as upstream
        dependencies and loaded from IO via the DAG.

        Args:
            context: Execution context for this run.
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution.

        Returns:
            Keyword arguments to pass to the asset function.

        Raises:
            AssetError: If a dependency cannot be resolved or read.
            DependencyNotFoundError: If a dependency key is not present in the DAG.
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
                        f"Asset '{self.key}' has dependencies but no DAG provided. "
                        "Pass a DAG to run() or materialize() for dependency resolution."
                    )

                upstream_key = dag.resolve_dependency_key(self, param_name)

                if upstream_key not in dag.asset_map:
                    raise DependencyNotFoundError(
                        f"Dependency '{upstream_key}' not found in DAG for asset '{self.key}'"
                    )

                upstream_asset = dag.asset_map[upstream_key]
                kwargs[param_name] = self._io_read(upstream_asset, partition_or_window, context.metadata)

        return kwargs

    def _resolve_io(self, io_key: str | None = None) -> IO:
        """Resolve a single IO from this asset.

        Args:
            io_key: For multi-IO assets, the key identifying which IO to use.
                When ``None``, uses :attr:`default_io_key`.

        Returns:
            The resolved IO instance.

        Raises:
            ConfigError: If *io_key* is not found or no IO is configured.
        """
        if isinstance(self.io, list):
            # default_io_key is guaranteed non-None (validated in model_post_init)
            target_key = io_key or self.default_io_key
            match = next((io for io in self.io if io.key == target_key), None)
            if match is None:
                available = sorted(io.key for io in self.io)
                raise ConfigError(f"IO key '{target_key}' not found on asset '{self.key}'. Available keys: {available}")
            return match

        if self.io is None:
            raise ConfigError(f"Asset '{self.key}' has no IO configured.")

        return self.io

    def partition_row_counts(self, *, io_key: str | None = None) -> dict[str, int]:
        """Return row counts grouped by this asset's partition column.

        Delegates to :meth:`IO.partition_row_counts` using the resolved IO.

        Args:
            io_key: For multi-IO assets, the IO key to query.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            PartitionError: If this asset is not partitioned.
        """
        if self.partitioning is None:
            raise PartitionError(
                f"Asset '{self.key}' is not partitioned. "
                "Cannot compute partition row counts without a partition column."
            )

        io = self._resolve_io(io_key)
        context = IOContext(asset=self)
        return io.partition_row_counts(context)

    def _validate_schema(self, data: Any) -> None:
        """Validate data against schema.

        Delegates to :func:`~interloper.schema.validate_schema`
        when data is ``list[dict]``.
        """
        if self.schema is None:
            return

        if isinstance(data, list) and data and isinstance(data[0], dict):
            from interloper.schema import validate_schema

            validate_schema(data, self.schema)

    def to_spec(self) -> AssetInstanceSpec:
        """Convert to serializable spec.

        Returns:
            An AssetSpec representing this asset.
        """
        # Serialize IO if present
        io_spec = None
        if isinstance(self.io, list):
            io_spec = [io.to_spec() for io in self.io]
        elif self.io is not None:
            io_spec = self.io.to_spec()

        return AssetInstanceSpec(
            path=self.path,
            io=io_spec,
            materializable=self.materializable,
            config=self.config.model_dump() if self.config is not None else None,
            default_io_key=self.default_io_key,
        )
