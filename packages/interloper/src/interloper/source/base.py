"""Source definition for grouping related assets."""

from __future__ import annotations

import copy
import functools
import inspect
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from interloper.assets.base import Asset, AssetDefinition
from interloper.errors import ConfigError, SourceError
from interloper.io.base import IO
from interloper.serialization.base import Serializable
from interloper.serialization.source import SourceSpec
from interloper.source.config import Config
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label, validate_name

if TYPE_CHECKING:
    from interloper.normalizer.base import Normalizer
    from interloper.normalizer.strategy import MaterializationStrategy


@dataclass(frozen=True)
class SourceDefinition:
    """Definition of a source created by the @source class decorator."""

    cls: type
    asset_defs: dict[str, AssetDefinition] = field(default_factory=dict)
    name: str = ""
    label: str = ""
    dataset: str | None = None
    config: type[Config] | None = None
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    normalizer: Normalizer | None = None
    strategy: MaterializationStrategy | None = None

    def __post_init__(self):
        """Set name to class name if not provided, validate."""
        if not self.name:
            object.__setattr__(self, "name", self.cls.__name__)

        validate_name(self.name)

        if not self.label:
            object.__setattr__(self, "label", to_label(self.name))

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
                if param_name in self.asset_defs and param_name != asset_def.name:
                    asset_def.requires[param_name] = self.asset_defs[param_name].definition_key

    @property
    def path(self) -> str:
        """Return the full import path of the decorated source class."""
        return get_object_path(self.cls)

    def __call__(
        self,
        *,
        name: str | None = None,
        dataset: str | None = None,
        config: Config | None = None,
        io: IO | dict[str, IO] | None = None,
        assets: Sequence[str] | dict[str, str] | None = None,
        strategy: MaterializationStrategy | None = None,
    ) -> Source:
        """Instantiate the source with optional runtime parameter override."""

        def instantiate_class(config: Config | None) -> Any:
            """Instantiate the source class, passing config to __init__ if accepted."""
            sig = inspect.signature(self.cls.__init__)
            if "config" in sig.parameters:
                if config is None:
                    raise ConfigError(
                        f"Source class '{self.cls.__name__}' accepts a 'config' parameter in __init__, "
                        f"but no config is configured for source '{self.name}'. "
                        f"Define a config type on the @source decorator or provide one at instantiation time."
                    )
                instance = self.cls(config=config)
            else:
                instance = self.cls()
            if config is not None:
                instance.config = config
            return instance

        def resolve_source_config() -> Config | None:
            """Resolve the config for the source."""
            if config is not None and self.config is not None and not issubclass(type(config), self.config):
                raise ConfigError(
                    f"Config provided to source '{self.name}' must be of type {self.config.__name__}, "
                    f"got {type(config).__name__}."
                )

            if config is not None and self.config is None:
                warnings.warn(
                    f"Config provided to source '{self.name}' but no config type is configured "
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
            """Resolve the config for an asset."""
            if config is not None and asset_def.config is not None and not issubclass(type(config), asset_def.config):
                print(
                    f"Warning: Config provided to source '{self.name}' is not of compatible with asset "
                    f"'{asset_def.name}' (Expecting {asset_def.config.__name__}, got {type(config).__name__}). "
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

            Returns the filtered definitions and a rename map.
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

            names = {d.name for d in defs}
            invalid = set(selected) - names
            if invalid:
                raise SourceError(f"Invalid asset names: {sorted(invalid)}. Valid asset names are: {sorted(names)}.")
            renamed = [rename_map.get(n, n) for n in selected]
            if len(set(renamed)) != len(renamed):
                raise SourceError(
                    f"Renamed asset names must be unique. Got duplicates after rename: {sorted(renamed)}."
                )

            return [d for d in defs if d.name in selected], rename_map

        def bind_asset_method(func: Callable, instance: Any) -> Callable:
            """Bind an unbound method to an instance, removing ``self`` from the signature."""

            @functools.wraps(func)
            def bound(*args: Any, **kwargs: Any) -> Any:
                return func(instance, *args, **kwargs)

            original_sig = inspect.signature(func)
            new_params = [p for name, p in original_sig.parameters.items() if name != "self"]
            bound.__signature__ = original_sig.replace(parameters=new_params)  # type: ignore[attr-defined]
            return bound

        if name is not None:
            validate_name(name)

        resolved_config = resolve_source_config()

        # Instantiate the source class
        cls_instance = instantiate_class(resolved_config)

        # Resolve asset definitions (filter + rename)
        filtered_defs, rename_map = resolve_asset_defs(list(self.asset_defs.values()))

        asset_instances: dict[str, Asset] = {}
        for asset_def in filtered_defs:
            asset_config = resolve_asset_config(asset_def, resolved_config)
            asset_name = rename_map.get(asset_def.name, asset_def.name)

            asset_instance = asset_def(
                name=asset_name,
                config=asset_config,
                io=io,
                dataset=self.dataset if asset_def.dataset is None else None,
            )

            # Bind the unbound method to the class instance
            asset_instance.func = bind_asset_method(asset_def.func, cls_instance)
            if asset_def.name != asset_name:
                asset_instance.metadata["source_original_name"] = asset_def.name

            # Inherit source-level normalizer if asset doesn't have its own
            if asset_instance.normalizer is None and self.normalizer is not None:
                asset_instance.normalizer = self.normalizer

            # Inherit source-level strategy if asset doesn't have its own
            resolved_strategy = strategy or self.strategy
            if asset_instance.strategy is None and resolved_strategy is not None:
                asset_instance.strategy = resolved_strategy

            asset_instances[asset_instance.name] = asset_instance

        return Source(
            definition=self,
            name=name or self.name,
            dataset=dataset or name or self.dataset or self.name,
            config=resolved_config,
            io=io,
            assets=asset_instances,
        )

    def __getattr__(self, name: str) -> AssetDefinition:
        """Access assets by name as attributes."""
        try:
            return self.asset_defs[name]
        except KeyError:
            raise SourceError(f"Source {self.name} has no asset definition named '{name}'")


@dataclass
class Source(Serializable[SourceSpec]):
    """Runtime instance of a source containing multiple assets."""

    definition: SourceDefinition
    name: str
    label: str = ""
    dataset: str | None = None
    config: Config | None = None
    io: IO | dict[str, IO] | None = None
    assets: dict[str, Asset] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Link assets back to this source."""
        if not self.label:
            object.__setattr__(self, "label", self.definition.label)

        for asset in self.assets.values():
            asset.source = self
            asset.dataset = asset.dataset or self.dataset or self.name

    @property
    def instance_key(self) -> str:
        """Return the source instance key (the source name)."""
        return self.name

    def copy(
        self,
        config: Config | None = None,
        io: IO | dict[str, IO] | None = None,
    ) -> Source:
        """Create an independent copy of this source with optional overrides."""
        source = copy.copy(self)
        if config is not None:
            source.config = config
        if io is not None:
            source.io = io

        # Deep-copy assets so the new source is fully independent
        source.assets = {name: copy.copy(asset) for name, asset in self.assets.items()}
        for asset in source.assets.values():
            asset.source = source

        return source

    def __getattr__(self, name: str) -> Asset:
        """Access assets by name as attributes."""
        # Let Python handle dunder lookups normally (required for copy, pickle, etc.)
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        # Use object.__getattribute__ to avoid recursion when accessing self.assets
        try:
            assets = object.__getattribute__(self, "assets")
            return assets[name]
        except KeyError:
            raise SourceError(f"Source has no asset named '{name}'")

    def to_spec(self) -> SourceSpec:
        """Convert to serializable spec."""
        # TODO: serialize assets by setting the source spec `assets` field

        io_spec = None
        if isinstance(self.io, dict):
            io_spec = {k: v.to_spec() for k, v in self.io.items()}  # type: ignore[unresolved-attribute]
        elif self.io is not None:
            io_spec = self.io.to_spec()

        materializable_assets = [asset.name for asset in self.assets.values() if asset.materializable]

        return SourceSpec(
            path=self.path,
            io=io_spec,
            assets=materializable_assets,
            config=self.config.model_dump() if self.config is not None else None,
        )
