"""Source definition for grouping related assets."""

from __future__ import annotations

import copy
import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from pydantic_settings import BaseSettings

from interloper.assets.base import Asset, AssetDefinition
from interloper.io.base import IO
from interloper.serialization.base import Serializable
from interloper.serialization.source import SourceSpec
from interloper.utils.imports import get_object_path


@dataclass(frozen=True)
class SourceDefinition:
    """Definition of a source created by the @source decorator."""

    func: Callable[..., Sequence[AssetDefinition]]
    name: str = ""
    dataset: str = ""
    config: type[BaseSettings] | None = None
    io: IO | dict[str, IO] | None = None
    default_io_key: str | None = None

    def __post_init__(self):
        """Set name to function name if not provided."""
        if not self.name:
            object.__setattr__(self, "name", getattr(self.func, "__name__", "unknown"))

        if not self.dataset:
            object.__setattr__(self, "dataset", self.name)

    @property
    def path(self) -> str:
        """Return the full import path of the decorated source function."""
        return get_object_path(self.func)

    def __call__(
        self,
        config: BaseSettings | None = None,
        io: IO | dict[str, IO] | None = None,
    ) -> Source:
        """Instantiate the source with optional runtime parameter override."""
        # If config is provided, check it's the correct type (if self.config is set)
        if config is not None and self.config is not None and not issubclass(type(config), self.config):
            raise TypeError(
                f"Config provided to source '{self.name}' must be of type {self.config.__name__}, "
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

        # Call source function to get the asset definitions
        sig = inspect.signature(self.func)
        kwargs = {}
        if "config" in sig.parameters:
            kwargs["config"] = resolved_config
        asset_defs = self.func(**kwargs)

        if not all(isinstance(asset_def, AssetDefinition) for asset_def in asset_defs):
            raise TypeError("All items returned from the source function must be of type AssetDefinition.")

        # Convert asset definitions to asset instances
        assets: dict[str, Asset] = {}
        for asset_def in asset_defs:
            asset_config = None

            # If config is provided, check it's compatible with the asset config
            if config is not None and asset_def.config is not None and not issubclass(type(config), asset_def.config):
                print(
                    f"Warning: Config provided to source '{self.name}' is not of compatible with asset "
                    f"'{asset_def.name}' (Expecting {asset_def.config.__name__}, got {type(config).__name__}). "
                    "Ignoring config override for this asset."
                )

            # Use config override if provided and compatible
            elif config is not None:
                asset_config = resolved_config

            # Use asset config if configured
            elif asset_def.config is not None:
                try:
                    asset_config = asset_def.config()
                except Exception as e:
                    raise ValueError(
                        f"Config {asset_def.config.__name__} is configured but cannot be resolved. "
                        f"Provide config explicitly or set environment variables. Error: {e}"
                    )

            # Fall back to source config (which is also resolved_config even if config override is None)
            else:
                asset_config = resolved_config

            asset_io = io or asset_def.io or self.io or None
            asset_dataset = self.dataset if asset_def.dataset is None else None
            asset_default_io_key = self.default_io_key if asset_def.default_io_key is None else None

            # Create asset instance
            asset_instance = asset_def(
                config=asset_config,
                io=asset_io,
                dataset=asset_dataset,
                default_io_key=asset_default_io_key,
            )
            assets[asset_instance.name] = asset_instance

        return Source(
            func=self.func,
            definition=self,
            name=self.name,
            dataset=self.dataset,
            config=resolved_config,
            io=io or self.io,
            default_io_key=self.default_io_key,
            assets=assets,
        )


@dataclass
class Source(Serializable[SourceSpec]):
    """Runtime instance of a source containing multiple assets."""

    func: Callable
    name: str
    definition: SourceDefinition
    config: BaseSettings | None = None
    dataset: str | None = None
    io: IO | dict[str, IO] | None = None
    default_io_key: str | None = None
    assets: dict[str, Asset] = field(default_factory=dict)

    def __post_init__(self):
        """Link assets back to this source."""
        for asset in self.assets.values():
            asset.source = self

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
