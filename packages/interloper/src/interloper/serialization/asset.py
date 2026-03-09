"""Serialization specs for assets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from interloper.errors import AssetError
from interloper.serialization.base import (
    ComponentDefinitionSpec,
    ComponentInstanceSpec,
    InstanceSpec,
    reconstruct_components,
    reconstruct_config,
)
from interloper.serialization.schema import SchemaFieldSpec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.assets.base import Asset
    from interloper.io.base import IO


class AssetDefinitionSpec(ComponentDefinitionSpec):
    """Spec describing an asset definition's metadata.

    Used for API responses, frontend display, and introspection —
    not for reconstruction.
    """

    requires: dict[str, str] | None = None
    schema_fields: list[SchemaFieldSpec] | None = None
    partitioned: bool = False
    partition_column: str | None = None


class AssetInstanceSpec(InstanceSpec):
    """InstanceSpec for a single Asset.

    The ``path`` field supports two formats:
        - A dotted import path to an ``AssetDefinition``.
        - A ``"source.path:asset_name"`` pair referencing an asset within a source.
    """

    type: Literal["asset"] = Field(default="asset", init=False, frozen=True)
    path: str
    io: ComponentInstanceSpec | list[ComponentInstanceSpec] | None = None
    config: dict[str, Any] | None = None  # dict to initialize the config Pydantic model
    materializable: bool = True
    default_io_key: str | None = None

    def reconstruct(self) -> Asset:
        """Reconstruct an Asset from this spec.

        Returns:
            The reconstructed Asset instance.
        """
        io = reconstruct_components(self.io)

        if ":" in self.path:
            return self._from_source_def(io)
        else:
            return self._from_asset_def(io)

    def _from_asset_def(self, io: IO | list[IO] | None) -> Asset:
        """Reconstruct asset from a standalone AssetDefinition.

        Returns:
            The reconstructed Asset.
        """
        from interloper.assets.base import AssetDefinition

        asset_def = import_from_path(self.path, AssetDefinition)
        config = reconstruct_config(asset_def, self.config)
        return asset_def(
            io=io,
            config=config,
            materializable=self.materializable,
            default_io_key=self.default_io_key,
        )

    def _from_source_def(self, io: IO | list[IO] | None) -> Asset:
        """Reconstruct asset by extracting it from a SourceDefinition.

        Returns:
            The reconstructed Asset.

        Raises:
            AssetError: If the named asset is not found in the source.
        """
        from interloper.source.base import SourceDefinition

        source_path, asset_name = self.path.split(":")
        source_def = import_from_path(source_path, SourceDefinition)
        config = reconstruct_config(source_def, self.config)
        source = source_def(config=config)

        try:
            asset: Asset = getattr(source, asset_name)
        except AttributeError:
            raise AssetError(f"Asset '{asset_name}' not found in source '{source_path}'") from None

        copy = asset.copy(materializable=self.materializable)
        if io is not None:
            copy.io = io
        if self.default_io_key is not None:
            copy.default_io_key = self.default_io_key
        return copy
