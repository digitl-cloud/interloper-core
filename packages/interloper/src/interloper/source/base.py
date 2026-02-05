"""Source definition for grouping related assets."""

from __future__ import annotations

import copy
import hashlib
import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from pydantic_settings import BaseSettings

from interloper.assets.base import Asset, AssetDefinition
from interloper.io.base import IO
from interloper.serialization.base import Serializable
from interloper.serialization.source import SourceSpec
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_display


@dataclass(frozen=True)
class SourceDefinition:
    """Definition of a source created by the @source decorator."""

    _id: str = field(default="", init=False, repr=False)
    func: Callable[..., Sequence[AssetDefinition]]
    name: str = ""
    display_name: str = ""
    dataset: str | None = None
    config: type[BaseSettings] | None = None
    io: IO | dict[str, IO] | None = None
    default_io_key: str | None = None

    def __post_init__(self):
        """Set name to function name if not provided, and compute the ID."""
        self._compute_id()

        if not self.name:
            object.__setattr__(self, "name", getattr(self.func, "__name__", "unknown"))

        if not self.display_name:
            object.__setattr__(self, "display_name", to_display(self.name))

    def _compute_id(self) -> None:
        """Compute and cache the definition ID based on current state."""
        path = get_object_path(self.func)
        hash_value = hashlib.sha256(path.encode()).hexdigest()[:12]
        object.__setattr__(self, "_id", f"dfs_{hash_value}")

    @property
    def path(self) -> str:
        """Return the full import path of the decorated source function."""
        return get_object_path(self.func)

    @property
    def id(self) -> str:
        """Return the definition ID (hashed import path)."""
        return self._id

    def __call__(
        self,
        *,
        name: str | None = None,
        dataset: str | None = None,
        config: BaseSettings | None = None,
        io: IO | dict[str, IO] | None = None,
        assets: Sequence[str] | dict[str, str] | None = None,
    ) -> Source:
        """Instantiate the source with optional runtime parameter override."""

        def resolve_source_config() -> BaseSettings | None:
            if config is not None and self.config is not None and not issubclass(type(config), self.config):
                raise TypeError(
                    f"Config provided to source '{self.name}' must be of type {self.config.__name__}, "
                    f"got {type(config).__name__}."
                )

            if config is not None or self.config is None:
                return config

            try:
                return self.config()
            except Exception as e:
                raise ValueError(
                    f"Config {self.config.__name__} is configured but cannot be resolved. "
                    f"Provide config explicitly or set environment variables. Error: {e}"
                ) from e

        def resolve_asset_config(
            asset_def: AssetDefinition,
            source_config: BaseSettings | None,
        ) -> BaseSettings | None:
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
                except Exception as e:
                    raise ValueError(
                        f"Config {asset_def.config.__name__} is configured but cannot be resolved. "
                        f"Provide config explicitly or set environment variables. Error: {e}"
                    )

            return source_config

        def load_asset_defs(source_config: BaseSettings | None) -> Sequence[AssetDefinition]:
            sig = inspect.signature(self.func)
            kwargs = {"config": source_config} if "config" in sig.parameters else {}
            asset_defs = self.func(**kwargs)
            if not all(isinstance(asset_def, AssetDefinition) for asset_def in asset_defs):
                raise TypeError("All items returned from the source function must be of type AssetDefinition.")
            return asset_defs

        def normalize_assets(
            asset_defs: Sequence[AssetDefinition],
        ) -> tuple[list[str] | None, dict[str, str]]:
            if assets is None:
                return None, {}

            if isinstance(assets, dict):
                assets_map = cast(dict[str, str], assets)
                asset_filter: list[str] = list(assets_map.keys())
                rename_map: dict[str, str] = dict(assets_map)
            else:
                assets_list = cast(Sequence[str], assets)
                asset_filter = list(assets_list)
                rename_map = {}

            asset_def_names = {asset_def.name for asset_def in asset_defs}
            invalid_names = set(asset_filter) - asset_def_names
            if invalid_names:
                raise ValueError(
                    f"Invalid asset names: {sorted(invalid_names)}. Valid asset names are: {sorted(asset_def_names)}."
                )
            renamed_names = [rename_map.get(name, name) for name in asset_filter]
            if len(set(renamed_names)) != len(renamed_names):
                raise ValueError(
                    "Renamed asset names must be unique. "
                    f"Got duplicates after rename: {sorted(renamed_names)}."
                )

            return asset_filter, rename_map

        def filter_asset_defs(
            asset_defs: Sequence[AssetDefinition],
            asset_filter: list[str] | None,
        ) -> list[AssetDefinition]:
            if asset_filter is None:
                return list(asset_defs)

            return [asset_def for asset_def in asset_defs if asset_def.name in asset_filter]

        resolved_config = resolve_source_config()
        asset_defs = load_asset_defs(resolved_config)
        asset_filter, rename_map = normalize_assets(asset_defs)
        asset_defs = filter_asset_defs(asset_defs, asset_filter)

        asset_instances: dict[str, Asset] = {}
        for asset_def in asset_defs:
            asset_config = resolve_asset_config(asset_def, resolved_config)
            asset_io = io or asset_def.io or self.io or None
            asset_dataset = self.dataset if asset_def.dataset is None else None
            asset_default_io_key = self.default_io_key if asset_def.default_io_key is None else None
            asset_name = rename_map.get(asset_def.name, asset_def.name)

            asset_instance = asset_def(
                name=asset_name,
                config=asset_config,
                io=asset_io,
                dataset=asset_dataset,
                default_io_key=asset_default_io_key,
            )
            if asset_def.name != asset_name:
                asset_instance.metadata["source_original_name"] = asset_def.name
            asset_instances[asset_instance.name] = asset_instance

        return Source(
            func=self.func,
            definition=self,
            name=name or self.name,
            dataset=dataset or name or self.dataset or self.name,
            config=resolved_config,
            io=io or self.io,
            default_io_key=self.default_io_key,
            assets=asset_instances,
        )


@dataclass
class Source(Serializable[SourceSpec]):
    """Runtime instance of a source containing multiple assets."""

    func: Callable
    definition: SourceDefinition
    name: str
    display_name: str = ""
    dataset: str | None = None
    config: BaseSettings | None = None
    io: IO | dict[str, IO] | None = None
    default_io_key: str | None = None
    assets: dict[str, Asset] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Link assets back to this source."""
        if not self.display_name:
            object.__setattr__(self, "display_name", self.definition.display_name)

        for asset in self.assets.values():
            asset.source = self
            asset.dataset = asset.dataset or self.dataset or self.name

    def copy(
        self,
        config: BaseSettings | None = None,
        io: IO | dict[str, IO] | None = None,
    ) -> Source:
        """Create a new source instance with runtime parameters."""
        source = copy.copy(self)
        if config is not None:
            source.config = config
        if io is not None:
            source.io = io
        return source

    def __getattr__(self, name: str) -> Asset:
        """Access assets by name as attributes."""
        # Use object.__getattribute__ to avoid recursion when accessing self.assets
        try:
            assets = object.__getattribute__(self, "assets")
            return assets[name]
        except KeyError:
            raise AttributeError(f"Source has no asset named '{name}'")

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
            io=io_spec,  # ty:ignore[invalid-argument-type]
            assets=materializable_assets,
            config=self.config.model_dump() if self.config is not None else None,
        )
