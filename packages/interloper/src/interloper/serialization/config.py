"""Serialization specs for config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

from interloper.serialization.base import ComponentInstanceSpec, InstanceSpec
from interloper.serialization.dag import DAGInstanceSpec

if TYPE_CHECKING:
    from interloper.cli.config import Config


class ConfigInstanceSpec(InstanceSpec):
    """Top-level InstanceSpec that bundles a DAG with its runner, IO, and backfiller settings."""

    backfiller: ComponentInstanceSpec | None = None
    runner: ComponentInstanceSpec | None = None
    io: list[ComponentInstanceSpec] = Field(default_factory=list)
    dag: DAGInstanceSpec

    def reconstruct(self) -> Config:
        """Reconstruct the config from the spec.

        Returns:
            The reconstructed Config instance.
        """
        from interloper.cli.config import Config

        dag = self.dag.reconstruct()
        io = [v.reconstruct() for v in self.io]
        backfiller = self.backfiller.reconstruct() if self.backfiller is not None else None
        runner = self.runner.reconstruct() if self.runner is not None else None

        return Config(
            dag=dag,
            io=io,
            backfiller=backfiller,
            runner=runner,
        )
