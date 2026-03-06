"""Serialization specs for config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

from interloper.serialization.backfiller import BackfillerInstanceSpec
from interloper.serialization.base import InstanceSpec
from interloper.serialization.dag import DAGInstanceSpec
from interloper.serialization.io import IOInstanceSpec
from interloper.serialization.runner import RunnerInstanceSpec

if TYPE_CHECKING:
    from interloper.cli.config import Config


class ConfigInstanceSpec(InstanceSpec):
    """Top-level InstanceSpec that bundles a DAG with its runner, IO, and backfiller settings."""

    backfiller: BackfillerInstanceSpec | None = None
    runner: RunnerInstanceSpec | None = None
    io: dict[str, IOInstanceSpec] = Field(default_factory=dict)
    dag: DAGInstanceSpec

    def reconstruct(self) -> Config:
        """Reconstruct the config from the spec.

        Returns:
            The reconstructed Config instance.
        """
        from interloper.cli.config import Config

        dag = self.dag.reconstruct()
        io = {k: v.reconstruct() for k, v in self.io.items()}
        backfiller = self.backfiller.reconstruct() if self.backfiller is not None else None
        runner = self.runner.reconstruct() if self.runner is not None else None

        return Config(
            dag=dag,
            io=io,
            backfiller=backfiller,
            runner=runner,
        )
