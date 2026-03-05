"""Serialization specs for sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from interloper.serialization.base import Spec
from interloper.serialization.io import IOSpec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.assets.base import AssetDefinition
    from interloper.io.base import IO
    from interloper.source.base import Source, SourceDefinition
    from interloper.source.config import Config


class SourceSpec(Spec):
    """Spec for a Source, identified by a dotted import path to a SourceDefinition.

    When ``assets`` is provided, only the listed asset names are marked as
    materializable; all others are set to non-materializable.
    """

    type: Literal["source"] = Field(default="source", init=False, frozen=True)
    path: str
    io: IOSpec | dict[str, IOSpec] | None = None
    config: dict[str, Any] | None = None  # dict to initialize the config Pydantic model
    assets: list[str] | None = None  # asset names to mark as materializable

    def reconstruct(self) -> Source:
        """Reconstruct a Source from this spec."""
        from interloper.source.base import SourceDefinition

        io = self._reconstruct_io(self.io)
        source_def = import_from_path(self.path, SourceDefinition)
        config = self._reconstruct_config(source_def, self.config)

        source = source_def(config=config, io=io)
        source.to_spec()

        if self.assets is not None:
            for asset in source.assets.values():
                asset.materializable = asset.name in self.assets

        return source

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
    ) -> Config | None:
        if definition.config is not None and data is not None:
            return definition.config.model_validate(data)
        return None
