"""Serialization specs for DAGs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from interloper.serialization.asset import AssetSpec
from interloper.serialization.base import Spec
from interloper.serialization.source import SourceSpec

if TYPE_CHECKING:
    from interloper.dag.base import DAG


class DAGSpec(Spec):
    """Spec for a DAG, containing a list of asset and source specs."""

    assets: list[AssetSpec | SourceSpec]

    def reconstruct(self) -> DAG:
        """Reconstruct DAG from spec.

        Returns:
            The reconstructed DAG instance.
        """
        from interloper.dag.base import DAG

        assets = [spec.reconstruct() for spec in self.assets]
        return DAG(*assets)
