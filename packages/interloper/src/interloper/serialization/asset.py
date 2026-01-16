"""Serialization specs for assets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field
from pydantic_settings.main import BaseSettings

from interloper.serialization.base import Spec
from interloper.serialization.io import IOSpec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.assets.base import Asset, AssetDefinition
    from interloper.io.base import IO
    from interloper.source.base import SourceDefinition


class AssetSpec(Spec):
    """Serializable Asset specification."""

    type: Literal["asset"] = Field(default="asset", init=False, frozen=True)
    path: str
    io: IOSpec | dict[str, IOSpec] | None = None
    config: dict[str, Any] | None = None  # dict to initialize the config Pydantic model
    materializable: bool = True

    def reconstruct(self) -> Asset:
        """Reconstruct Asset from spec."""
        io = self._reconstruct_io(self.io)

        if ":" in self.path:
            return self._from_source_def(io)
        else:
            return self._from_asset_def(io)

    def _reconstruct_io(self, io: IOSpec | dict[str, IOSpec] | None) -> IO | dict[str, IO] | None:
        if isinstance(io, IOSpec):
            return io.reconstruct()
        elif isinstance(io, dict):
            return {k: v.reconstruct() for k, v in io.items()}
        return io

    def _reconstruct_config(
        self,
        definition: SourceDefinition | AssetDefinition,
        data: dict[str, Any] | None,
    ) -> BaseSettings | None:
        if definition.config is not None and data is not None:
            return definition.config.model_validate(data)
        return None

    def _from_asset_def(self, io: IO | dict[str, IO] | None) -> Asset:
        """Reconstruct asset from AssetDefinition."""
        from interloper.assets.base import AssetDefinition

        asset_def = import_from_path(self.path, AssetDefinition)
        config = self._reconstruct_config(asset_def, self.config)
        return asset_def(io=io, config=config, materializable=self.materializable)

    def _from_source_def(self, io: IO | dict[str, IO] | None) -> Asset:
        """Reconstruct asset from SourceDefinition."""
        from interloper.source.base import SourceDefinition

        source_path, asset_name = self.path.split(":")
        source_def = import_from_path(source_path, SourceDefinition)
        config = self._reconstruct_config(source_def, self.config)
        source = source_def(config=config)

        try:
            asset: Asset = getattr(source, asset_name)
        except AttributeError:
            raise ValueError(f"Asset '{asset_name}' not found in source '{source_path}'") from None

        copy = asset.copy(materializable=self.materializable)
        if io is not None:
            copy.io = io
        return copy
