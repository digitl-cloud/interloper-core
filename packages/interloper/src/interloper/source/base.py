"""Source definition for grouping related assets."""

from __future__ import annotations

import copy
import functools
import inspect
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from pydantic import Field

from interloper.assets.base import Asset, AssetDefinition
from interloper.destination.base import Destination, validate_destination_keys
from interloper.errors import ConfigError, SourceError
from interloper.normalizer.base import Normalizer
from interloper.normalizer.strategy import MaterializationStrategy
from interloper.serialization.base import Component, HasDefinitionSpec
from interloper.serialization.source import SourceDefinitionSpec, SourceInstanceSpec
from interloper.source.config import Config
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_key


@dataclass(frozen=True)
class SourceDefinition(HasDefinitionSpec):
    """Definition of a source created by the @source class decorator."""

    cls: type
    asset_defs: dict[str, AssetDefinition] = field(default_factory=dict)
    key: str = ""
    label: str = ""
    dataset: str | None = None
    config: type[Config] | None = None
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None

    def __post_init__(self):
        """Set key to class name if not provided, validate."""
        if not self.key:
            object.__setattr__(self, "key", self.cls.__name__)

        validate_key(self.key)

        if not self.label:
            object.__setattr__(self, "label", to_label(self.key))

        self._wire_asset_defs()
        self._infer_requires()

    def _wire_asset_defs(self) -> None:
        for asset_def in self.asset_defs.values():
            object.__setattr__(asset_def, "source_definition", self)

    def _infer_requires(self) -> None:
        """Auto-populate requires for params whose names match sibling assets."""
        for asset_def in self.asset_defs.values():
            sig = inspect.signature(asset_def.func)
            for param_name in sig.parameters:
                if param_name in ("self", "context", "config"):
                    continue
                if param_name in asset_def.requires:
                    continue
                if param_name in self.asset_defs and param_name != asset_def.key:
                    asset_def.requires[param_name] = self.asset_defs[param_name].qualified_key

    def definition_spec(self) -> SourceDefinitionSpec:
        """Produce a definition spec describing this source's metadata.

        Returns:
            A SourceDefinitionSpec capturing key, label, description,
            tags, config_schema, and nested asset definition specs.
        """
        config_schema = None
        if self.config is not None:
            config_schema = self.config.model_json_schema()

        return SourceDefinitionSpec(
            key=self.key,
            label=self.label,
            description=self.cls.__doc__ or "",
            tags=list(self.tags),
            config_schema=config_schema,
            assets=[ad.definition_spec() for ad in self.asset_defs.values()],
        )

    @property
    def path(self) -> str:
        """Return the full import path of the decorated source class."""
        return get_object_path(self.cls)

    def __call__(
        self,
        *,
        key: str | None = None,
        dataset: str | None = None,
        config: Config | None = None,
        destination: Destination | list[Destination] | None = None,
        default_destination_key: str | None = None,
        assets: Sequence[str] | dict[str, str] | None = None,
        strategy: MaterializationStrategy | None = None,
    ) -> Source:
        """Instantiate the source with optional runtime parameter override.

        Returns:
            A configured Source instance.
        """

        def instantiate_class(config: Config | None) -> Any:
            """Instantiate the source class, passing config to __init__ if accepted.

            Returns:
                The instantiated source class instance.

            Raises:
                ConfigError: If config is required but not provided.
            """
            sig = inspect.signature(self.cls.__init__)
            if "config" in sig.parameters:
                if config is None:
                    raise ConfigError(
                        f"Source class '{self.cls.__name__}' accepts a 'config' parameter in __init__, "
                        f"but no config is configured for source '{self.key}'. "
                        f"Define a config type on the @source decorator or provide one at instantiation time."
                    )
                instance = self.cls(config=config)
            else:
                instance = self.cls()
            if config is not None:
                instance.config = config
            return instance

        def resolve_source_config() -> Config | None:
            """Resolve the config for the source.

            Returns:
                The resolved Config instance, or None.

            Raises:
                ConfigError: If config type is incompatible or cannot be resolved.
            """
            if config is not None and self.config is not None and not issubclass(type(config), self.config):
                raise ConfigError(
                    f"Config provided to source '{self.key}' must be of type {self.config.__name__}, "
                    f"got {type(config).__name__}."
                )

            if config is not None and self.config is None:
                warnings.warn(
                    f"Config provided to source '{self.key}' but no config type is configured "
                    f"on the @source decorator. The config will be used but cannot be type-checked.",
                    UserWarning,
                    stacklevel=2,
                )
                return config

            if config is not None or self.config is None:
                return config

            try:
                return self.config()
            except Exception as e:
                raise ConfigError(
                    f"Config {self.config.__name__} is configured but cannot be resolved. "
                    f"Provide config explicitly or set environment variables. Error: {e}"
                ) from e

        def resolve_asset_config(
            asset_def: AssetDefinition,
            source_config: Config | None,
        ) -> Config | None:
            """Resolve the config for an asset.

            Returns:
                The resolved Config instance for the asset, or None.

            Raises:
                ConfigError: If the asset config cannot be resolved.
            """
            if config is not None and asset_def.config is not None and not issubclass(type(config), asset_def.config):
                print(
                    f"Warning: Config provided to source '{self.key}' is not of compatible with asset "
                    f"'{asset_def.key}' (Expecting {asset_def.config.__name__}, got {type(config).__name__}). "
                    "Ignoring config override for this asset."
                )
                return None

            if config is not None:
                return source_config

            if asset_def.config is not None:
                try:
                    return asset_def.config()
                except (TypeError, ValueError) as e:
                    raise ConfigError(
                        f"Config {asset_def.config.__name__} is configured but cannot be resolved. "
                        f"Provide config explicitly or set environment variables. Error: {e}"
                    )

            return source_config

        def resolve_asset_defs(
            defs: list[AssetDefinition],
        ) -> tuple[list[AssetDefinition], dict[str, str]]:
            """Filter and validate asset definitions based on the ``assets`` parameter.

            Returns:
                The filtered definitions and a rename map.

            Raises:
                SourceError: If invalid asset names are provided or renames collide.
            """
            if assets is None:
                return list(defs), {}

            if isinstance(assets, dict):
                assets_map = cast(dict[str, str], assets)
                selected: list[str] = list(assets_map.keys())
                rename_map: dict[str, str] = dict(assets_map)
            else:
                selected = list(cast(Sequence[str], assets))
                rename_map = {}

            keys = {d.key for d in defs}
            invalid = set(selected) - keys
            if invalid:
                raise SourceError(f"Invalid asset keys: {sorted(invalid)}. Valid asset keys are: {sorted(keys)}.")
            renamed = [rename_map.get(k, k) for k in selected]
            if len(set(renamed)) != len(renamed):
                raise SourceError(f"Renamed asset keys must be unique. Got duplicates after rename: {sorted(renamed)}.")

            return [d for d in defs if d.key in selected], rename_map

        def bind_asset_method(func: Callable, instance: Any) -> Callable:
            """Bind an unbound method to an instance, removing ``self`` from the signature.

            Returns:
                The bound callable with updated signature.
            """

            @functools.wraps(func)
            def bound(*args: Any, **kwargs: Any) -> Any:
                return func(instance, *args, **kwargs)

            original_sig = inspect.signature(func)
            new_params = [p for name, p in original_sig.parameters.items() if name != "self"]
            bound.__signature__ = original_sig.replace(parameters=new_params)  # type: ignore[attr-defined]
            return bound

        if key is not None:
            validate_key(key)

        resolved_config = resolve_source_config()

        # Instantiate the source class
        cls_instance = instantiate_class(resolved_config)

        # Resolve asset definitions (filter + rename)
        filtered_defs, rename_map = resolve_asset_defs(list(self.asset_defs.values()))

        asset_instances: dict[str, Asset] = {}
        for asset_def in filtered_defs:
            asset_config = resolve_asset_config(asset_def, resolved_config)
            asset_key = rename_map.get(asset_def.key, asset_def.key)

            asset_instance = asset_def(
                key=asset_key,
                config=asset_config,
                destination=destination,
                default_destination_key=default_destination_key,
                dataset=self.dataset if asset_def.dataset is None else None,
            )

            # Bind the unbound method to the class instance
            asset_instance.func = bind_asset_method(asset_def.func, cls_instance)
            if asset_def.key != asset_key:
                asset_instance.metadata["source_original_key"] = asset_def.key

            # Inherit source-level normalizer if asset doesn't have its own
            if asset_instance.normalizer is None and self.normalizer is not None:
                asset_instance.normalizer = self.normalizer

            # Inherit source-level strategy if asset doesn't have its own
            resolved_strategy = strategy or self.strategy
            if asset_instance.strategy is None and resolved_strategy is not None:
                asset_instance.strategy = resolved_strategy

            asset_instances[asset_instance.key] = asset_instance

        return Source(
            definition=self,
            key=key or self.key,
            dataset=dataset or key or self.dataset or self.key,
            config=resolved_config,
            destination=destination,
            default_destination_key=default_destination_key,
            assets=asset_instances,
        )

    def __getattr__(self, name: str) -> AssetDefinition:
        """Access asset definitions by key as attributes.

        Returns:
            The matching AssetDefinition.

        Raises:
            SourceError: If no asset definition with the given key exists.
        """
        try:
            return self.asset_defs[name]
        except KeyError:
            raise SourceError(f"Source '{self.key}' has no asset definition with key '{name}'")


class Source(Component):
    """Runtime instance of a source containing multiple assets."""

    definition: SourceDefinition
    key: str = ""
    label: str = ""
    dataset: str | None = None
    config: Config | None = None
    destination: Destination | list[Destination] | None = None
    default_destination_key: str | None = None
    assets: dict[str, Asset] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def model_post_init(self, __context: Any, /) -> None:
        """Link assets back to this source and set composite keys."""
        if not self.label:
            self.label = self.definition.label

        if isinstance(self.destination, list):
            validate_destination_keys(self.destination, self.key)

        for asset in self.assets.values():
            asset.source = self
            asset.dataset = asset.dataset or self.dataset or self.key

    def copy(
        self,
        config: Config | None = None,
        destination: Destination | list[Destination] | None = None,
    ) -> Source:
        """Create an independent copy of this source with optional overrides.

        Returns:
            A new Source instance with deep-copied assets.
        """
        source = copy.copy(self)
        if config is not None:
            source.config = config
        if destination is not None:
            source.destination = destination

        # Deep-copy assets so the new source is fully independent
        source.assets = {k: copy.copy(asset) for k, asset in self.assets.items()}
        for asset in source.assets.values():
            asset.source = source

        return source

    def __getattr__(self, name: str) -> Asset:
        """Access assets by key as attributes.

        Returns:
            The matching Asset instance.

        Raises:
            AttributeError: If the name starts with ``_`` (Pydantic internals).
            SourceError: If no asset with the given key exists.
        """
        # Let Python/Pydantic handle private and dunder lookups normally
        if name.startswith("_"):
            raise AttributeError(name)
        # Use object.__getattribute__ to avoid recursion when accessing self.assets
        try:
            assets = object.__getattribute__(self, "assets")
            return assets[name]
        except KeyError:
            raise SourceError(f"Source has no asset with key '{name}'")

    def definition_spec(self) -> SourceDefinitionSpec:
        """Produce a definition spec by delegating to the underlying definition.

        Returns:
            A SourceDefinitionSpec describing this source's metadata.
        """
        return self.definition.definition_spec()

    def to_spec(self) -> SourceInstanceSpec:
        """Convert to serializable spec.

        Returns:
            The serialized SourceSpec representation.
        """
        # TODO: serialize assets by setting the source spec `assets` field

        dest_spec = None
        if isinstance(self.destination, list):
            dest_spec = [d.to_spec() for d in self.destination]
        elif self.destination is not None:
            dest_spec = self.destination.to_spec()

        materializable_assets = [str(asset.key) for asset in self.assets.values() if asset.materializable]

        return SourceInstanceSpec(
            path=self.path,
            destinations=dest_spec,
            assets=materializable_assets,
            config=self.config.model_dump() if self.config is not None else None,
            default_destination_key=self.default_destination_key,
        )


# ---------------------------------------------------------------------------
# Deferred model resolution
# ---------------------------------------------------------------------------
# Asset and Source reference each other's types behind TYPE_CHECKING to avoid
# circular imports.  Now that both classes are fully defined we can rebuild
# their Pydantic schemas, supplying the missing names via _types_namespace.

Source.model_rebuild()
Asset.model_rebuild(
    _types_namespace={
        "Source": Source,
        "SourceDefinition": SourceDefinition,
        "Config": Config,
        "Normalizer": Normalizer,
        "MaterializationStrategy": MaterializationStrategy,
    }
)
