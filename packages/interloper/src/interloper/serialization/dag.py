"""Serialization specs for DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper.serialization.asset import AssetInstanceSpec
from interloper.serialization.base import InstanceSpec
from interloper.serialization.source import SourceInstanceSpec

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class DAGInstanceSpec(InstanceSpec):
    """InstanceSpec for a DAG, containing a list of asset and source InstanceSpecs."""

    assets: list[AssetInstanceSpec | SourceInstanceSpec]

    def reconstruct(self) -> DAG:
        """Reconstruct DAG from spec.

        Returns:
            The reconstructed DAG instance.
        """
        from interloper.dag.base import DAG

        assets = [spec.reconstruct() for spec in self.assets]
        return DAG(*assets)
