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
from interloper.destination.base import Destination, validate_destination_keys
from interloper.destination.context import DestinationContext
from interloper.destination.memory import MemoryDestination
from interloper.errors import AssetError, ConfigError, DependencyNotFoundError, PartitionError
from interloper.events import get_asset_event_metadata
from interloper.events.base import EventType, emit
from interloper.partitioning.base import Partition, PartitionConfig, PartitionWindow
from interloper.serialization.asset import AssetDefinitionSpec, AssetInstanceSpec
from interloper.serialization.base import Component, HasDefinitionSpec
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
class AssetDefinition(HasDefinitionSpec):
    """Definition of an asset created by the @asset decorator."""

    func: Callable[..., Any]
    source_definition: SourceDefinition | None = None
    key: str = ""
    label: str = ""
    schema: type[BaseModel] | None = None
    config: type[Config] | None = None
    destination: Destination | list[Destination] | None = None
    default_destination_key: str | None = None
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

    def definition_spec(self) -> AssetDefinitionSpec:
        """Produce a definition spec describing this asset's metadata.

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
            partitioned=self.partitioning is not None,
            partition_column=self.partitioning.column if self.partitioning else None,
        )

    @property
    def qualified_key(self) -> AssetDefinitionKey:
        """Return the fully qualified asset definition key.

        Format: ``{source-key}:{asset-key}`` for source-bound assets,
        or just ``{asset-key}`` for standalone assets.
        """
        if self.source_definition:
            return AssetDefinitionKey(f"{self.source_definition.key}.{self.key}")
        return AssetDefinitionKey(self.key)

    def __call__(
        self,
        *,
        key: str | None = None,
        config: Config | None = None,
        destination: Destination | list[Destination] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
        dataset: str | None = None,
        default_destination_key: str | None = None,
        materializable: bool = True,
        strategy: MaterializationStrategy | None = None,
    ) -> Asset:
        """Instantiate an ``Asset`` from this definition with runtime overrides.

        Args:
            key: Override the asset key.
            config: Override the config instance.
            destination: Override the destination backend (single or list keyed by ``destination.key``).
            deps: Explicit dependency mapping (param name to asset instance key).
            dataset: Override the dataset name.
            default_destination_key: Default destination key for multi-destination setups.
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
            destination=destination or self.destination,
            normalizer=self.normalizer,
            strategy=strategy or self.strategy,
            partitioning=self.partitioning,
            dataset=dataset or self.dataset,
            default_destination_key=default_destination_key or self.default_destination_key,
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
    destination: Destination | list[Destination] | None = None
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None
    partitioning: PartitionConfig | None = None
    dataset: str | None = None
    default_destination_key: str | None = None
    deps: dict[str, AssetInstanceKey] = Field(default_factory=dict)
    source: Source | None = Field(default=None, init=False, exclude=True, repr=False)
    materializable: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any, /) -> None:
        """Apply defaults after initialization."""
        if not self.label:
            self.label = self.definition.label

        if self.destination is None:
            self.destination = MemoryDestination.singleton()

        if isinstance(self.destination, list) and len(self.destination) == 1:
            self.destination = self.destination[0]

        if isinstance(self.destination, list):
            validate_destination_keys(self.destination, self.qualified_key)

        if self.partitioning is not None and self.schema is not None:
            schema_fields = set(self.schema.model_fields.keys())
            if self.partitioning.column not in schema_fields:
                import warnings

                warnings.warn(
                    f"Asset '{self.qualified_key}': partition column '{self.partitioning.column}' "
                    f"not found in schema fields {sorted(schema_fields)}.",
                    UserWarning,
                    stacklevel=2,
                )

    @property
    def qualified_key(self) -> AssetInstanceKey:
        """Return the fully qualified instance key.

        Format: ``{source-instance-key}:{asset-key}`` for source-bound assets,
        or just ``{asset-key}`` for standalone assets.
        """
        if self.source:
            return AssetInstanceKey(f"{self.source.key}.{self.key}")
        return AssetInstanceKey(self.key)

    def copy(
        self,
        config: Config | None = None,
        destination: Destination | list[Destination] | None = None,
        deps: dict[str, AssetInstanceKey] | None = None,
        dataset: str | None = None,
        materializable: bool | None = None,
    ) -> Asset:
        """Return a shallow copy of this asset with optional overrides."""
        # Create a shallow copy and set attrs, since dataclasses.replace() fails on frozen/field-removed
        asset = copy.copy(self)
        if config is not None:
            asset.config = config
        if destination is not None:
            asset.destination = destination
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
            path = f"{get_object_path(self.source.definition.cls)}:{self.key}"
        else:
            path = get_object_path(self.func)  # Points to the actual function
        return path

    def _event_metadata(
        self,
        metadata: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None = None,
        *,
        destination_key: str | None = None,
    ) -> dict[str, Any]:
        """Build the base event metadata dict for this asset.

        Merges run-level metadata with asset identity fields.  Destination methods
        pass ``destination_key`` to include the destination key in the metadata.

        Args:
            metadata: Run-level metadata (e.g. run_id, backfill_id).
            partition_or_window: Current partition scope.
            destination_key: Destination key for destination read/write events.

        Returns:
            The merged metadata dict (without ``message``).
        """
        base: dict[str, Any] = {
            **metadata,
            **get_asset_event_metadata(self),
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }
        if destination_key is not None:
            base["destination_key"] = destination_key
        return base

    def run(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

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
            warnings.warn(
                f"Asset '{self.qualified_key}' is not partitioned, partition/partition_window will be ignored"
            )

        if self.partitioning is not None and partition_or_window is None:
            raise PartitionError(
                f"Asset '{self.qualified_key}' is partitioned, but no partition/partition_window provided"
            )

        if (
            self.partitioning is not None
            and isinstance(partition_or_window, PartitionWindow)
            and not self.partitioning.allow_window
        ):
            raise PartitionError(
                f"Asset '{self.qualified_key}' does not support windowed runs (allow_window=False). "
                "Use a partition window with backfill(windowed=False) to run one partition per run."
            )

        # Create context
        context = ExecutionContext(
            asset_key=self.qualified_key,
            partition_or_window=partition_or_window,
            partitioning=self.partitioning,
            metadata=metadata,
        )

        # Build function kwargs with dependency resolution
        kwargs = self._build_kwargs(context, partition_or_window, dag)

        # Execute core function
        exec_metadata = self._event_metadata(metadata or {}, partition_or_window)
        msg = f"Executing '{self.qualified_key}'"
        emit(EventType.ASSET_EXEC_STARTED, metadata={**exec_metadata, "message": msg})
        try:
            result = self.func(**kwargs)
            msg = f"Executed '{self.qualified_key}'"
            emit(EventType.ASSET_EXEC_COMPLETED, metadata={**exec_metadata, "message": msg})
        except Exception as e:
            emit(
                EventType.ASSET_EXEC_FAILED,
                metadata={
                    **exec_metadata,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Execution of '{self.qualified_key}' failed: {e}",
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
                    raise AssetError(f"Asset '{self.qualified_key}': strategy='reconcile' requires a schema.")
                result = self.normalizer.reconcile(result, self.schema)

            elif strategy == MaterializationStrategy.STRICT:
                if self.schema is None:
                    raise AssetError(f"Asset '{self.qualified_key}': strategy='strict' requires a schema.")
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
        """Execute the asset and write the result to all configured destinations.

        Equivalent to calling ``run()`` followed by writing to every destination target.

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
        self._destination_write(partition_or_window, metadata, result)
        return result

    def _destination_write(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
        result: Any,
    ) -> None:
        """Write the execution result to all configured destination targets.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
            result: The value to write.
        """
        if self.destination is None:
            return

        dest_context = DestinationContext(
            asset=self,
            partition_or_window=partition_or_window if self.partitioning is not None else None,
            metadata=metadata,
        )

        # Build list of destinations to write to
        destinations = self.destination if isinstance(self.destination, list) else [self.destination]

        for dest in destinations:
            dest_key = dest.key
            dest_label = f"{dest}[{dest_key}]"
            dest_metadata = self._event_metadata(metadata, partition_or_window, destination_key=dest_key)
            msg = f"Writing '{self.qualified_key}' to {dest_label}"
            emit(EventType.DEST_WRITE_STARTED, metadata={**dest_metadata, "message": msg})
            try:
                dest.write(dest_context, result)
                msg = f"Wrote '{self.qualified_key}' to {dest_label}"
                emit(EventType.DEST_WRITE_COMPLETED, metadata={**dest_metadata, "message": msg})
            except Exception as e:
                emit(
                    EventType.DEST_WRITE_FAILED,
                    metadata={
                        **dest_metadata,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                        "message": f"Failed to write '{self.qualified_key}' to {dest_label}: {e}",
                    },
                )
                raise

    def _destination_read(
        self,
        upstream_asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> Any:
        """Read data from an upstream asset's destination.

        Args:
            upstream_asset: The upstream asset to read from.
            partition_or_window: Partition or PartitionWindow for this run.
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The data read from the upstream asset's destination.

        Raises:
            AssetError: If no destination is found or the read fails.
            ConfigError: If the upstream asset has multiple destinations but no default_destination_key.
        """
        if isinstance(upstream_asset.destination, list):
            if not self.default_destination_key:
                raise ConfigError(
                    f"Asset '{self.qualified_key}' has multiple destinations but no default_destination_key. "
                    "Set default_destination_key to specify which destination to use for upstream reads."
                )
            read_dest_key = upstream_asset.default_destination_key
            read_dest = next(d for d in upstream_asset.destination if d.key == read_dest_key)
        else:
            read_dest_key = None
            read_dest = upstream_asset.destination

        if read_dest is None:
            raise AssetError(
                f"No destination found for upstream asset '{upstream_asset.qualified_key}'"
            )

        if upstream_asset.partitioning is not None:
            effective_partition_or_window = partition_or_window
        else:
            effective_partition_or_window = None

        dest_context = DestinationContext(
            asset=upstream_asset,
            partition_or_window=effective_partition_or_window,
            metadata=metadata,
        )

        dest_label = f"{read_dest}[{read_dest_key}]" if read_dest_key else str(read_dest)
        dest_metadata = self._event_metadata(
            metadata, effective_partition_or_window, destination_key=read_dest_key
        )
        msg = f"Reading '{upstream_asset.qualified_key}' from {dest_label}"
        emit(EventType.DEST_READ_STARTED, metadata={**dest_metadata, "message": msg})
        try:
            result = read_dest.read(dest_context)
            msg = f"Read '{upstream_asset.qualified_key}' from {dest_label}"
            emit(EventType.DEST_READ_COMPLETED, metadata={**dest_metadata, "message": msg})
        except Exception as e:
            emit(
                EventType.DEST_READ_FAILED,
                metadata={
                    **dest_metadata,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": (
                        f"Failed to read '{upstream_asset.qualified_key}' from {dest_label}: {e}"
                    ),
                },
            )
            raise AssetError(
                f"Failed to load data from upstream asset '{upstream_asset.qualified_key}': {e}"
            ) from e

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
        dependencies and loaded from destination via the DAG.

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
                # This is a dependency - load from destination via DAG
                if dag is None:
                    raise AssetError(
                        f"Asset '{self.qualified_key}' has dependencies but no DAG provided. "
                        "Pass a DAG to run() or materialize() for dependency resolution."
                    )

                upstream_key = dag.resolve_dependency_key(self, param_name)

                if upstream_key not in dag.asset_map:
                    raise DependencyNotFoundError(
                        f"Dependency '{upstream_key}' not found in DAG for asset '{self.qualified_key}'"
                    )

                upstream_asset = dag.asset_map[upstream_key]
                kwargs[param_name] = self._destination_read(
                    upstream_asset, partition_or_window, context.metadata
                )

        return kwargs

    def _resolve_destination(self, destination_key: str | None = None) -> Destination:
        """Resolve a single destination from this asset.

        Args:
            destination_key: For multi-destination assets, the key identifying which
                destination to use. When ``None``, uses :attr:`default_destination_key`.

        Returns:
            The resolved destination instance.

        Raises:
            ConfigError: If *destination_key* is not found or no destination is configured.
        """
        if isinstance(self.destination, list):
            target_key = destination_key or self.default_destination_key
            match = next((d for d in self.destination if d.key == target_key), None)
            if match is None:
                available = sorted(d.key for d in self.destination)
                raise ConfigError(
                    f"Destination key '{target_key}' not found on asset "
                    f"'{self.qualified_key}'. Available keys: {available}"
                )
            return match

        if self.destination is None:
            raise ConfigError(f"Asset '{self.qualified_key}' has no destination configured.")

        return self.destination

    def partition_row_counts(self, *, destination_key: str | None = None) -> dict[str, int]:
        """Return row counts grouped by this asset's partition column.

        Delegates to :meth:`Destination.partition_row_counts` using the resolved destination.

        Args:
            destination_key: For multi-destination assets, the destination key to query.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            PartitionError: If this asset is not partitioned.
        """
        if self.partitioning is None:
            raise PartitionError(
                f"Asset '{self.qualified_key}' is not partitioned. "
                "Cannot compute partition row counts without a partition column."
            )

        dest = self._resolve_destination(destination_key)
        context = DestinationContext(asset=self)
        return dest.partition_row_counts(context)

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

    def definition_spec(self) -> AssetDefinitionSpec:
        """Produce a definition spec by delegating to the underlying definition.

        Returns:
            An AssetDefinitionSpec describing this asset's metadata.
        """
        return self.definition.definition_spec()

    def to_spec(self) -> AssetInstanceSpec:
        """Convert to serializable spec.

        Returns:
            An AssetSpec representing this asset.
        """
        # Serialize destination if present
        dest_spec = None
        if isinstance(self.destination, list):
            dest_spec = [d.to_spec() for d in self.destination]
        elif self.destination is not None:
            dest_spec = self.destination.to_spec()

        return AssetInstanceSpec(
            path=self.path,
            destinations=dest_spec,
            materializable=self.materializable,
            config=self.config.model_dump() if self.config is not None else None,
            default_destination_key=self.default_destination_key,
        )
