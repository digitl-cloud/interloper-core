"""Serialization specs for sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from interloper.serialization.asset import AssetDefinitionSpec
from interloper.serialization.base import (
    ComponentDefinitionSpec,
    ComponentInstanceSpec,
    InstanceSpec,
    reconstruct_components,
    reconstruct_config,
)
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.source.base import Source


class SourceDefinitionSpec(ComponentDefinitionSpec):
    """Spec describing a source definition's metadata.

    Used for API responses, frontend display, and introspection —
    not for reconstruction.
    """

    assets: list[AssetDefinitionSpec] = Field(default_factory=list)


class SourceInstanceSpec(InstanceSpec):
    """InstanceSpec for a Source, identified by a dotted import path to a SourceDefinition.

    When ``assets`` is provided, only the listed asset local keys are marked as
    materializable; all others are set to non-materializable.
    """

    type: Literal["source"] = Field(default="source", init=False, frozen=True)
    path: str
    io: ComponentInstanceSpec | list[ComponentInstanceSpec] | None = None
    config: dict[str, Any] | None = None  # dict to initialize the config Pydantic model
    assets: list[str] | None = None  # asset local keys to mark as materializable
    default_io_key: str | None = None

    def reconstruct(self) -> Source:
        """Reconstruct a Source from this spec.

        Returns:
            The reconstructed Source instance.
        """
        from interloper.source.base import SourceDefinition

        io = reconstruct_components(self.io)
        source_def = import_from_path(self.path, SourceDefinition)
        config = reconstruct_config(source_def, self.config)

        source = source_def(config=config, io=io, default_io_key=self.default_io_key)

        if self.assets is not None:
            for asset in source.assets.values():
                asset.materializable = asset.local_key in self.assets

        return source
