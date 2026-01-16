"""Serialization specs for sources."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field
from pydantic_settings import BaseSettings

from interloper.serialization.base import Spec
from interloper.serialization.io import IOSpec
from interloper.utils.imports import import_from_path

if TYPE_CHECKING:
    from interloper.assets.base import AssetDefinition
    from interloper.io.base import IO
    from interloper.source.base import Source, SourceDefinition


class SourceSpec(Spec):
    """Serializable Source specification."""

    type: Literal["source"] = Field(default="source", init=False, frozen=True)
    path: str
    io: IOSpec | dict[str, IOSpec] | None = None
    config: dict[str, Any] | None = None  # dict to initialize the config Pydantic model
    assets: list[str] | None = None  # list of asset to mark as materializable (names not keys, for better UX in config)

    def reconstruct(self) -> Source:
        """Reconstruct Source from spec."""
        from interloper.source.base import SourceDefinition

        io = self._reconstruct_io(self.io)
        source_def = import_from_path(self.path, SourceDefinition)
        config = self._reconstruct_config(source_def, self.config)

        source = source_def(config=config, io=io)
        source.to_spec()

        if self.assets is not None:
            for asset in source.assets.values():
                asset.materializable = True if asset.name in self.assets else False

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
    ) -> BaseSettings | None:
        if definition.config is not None and data is not None:
            return definition.config.model_validate(data)
        return None
