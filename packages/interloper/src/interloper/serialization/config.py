"""Serialization specs for config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

from interloper.serialization.backfiller import BackfillerSpec
from interloper.serialization.base import Spec
from interloper.serialization.dag import DAGSpec
from interloper.serialization.io import IOSpec
from interloper.serialization.runner import RunnerSpec

if TYPE_CHECKING:
    from interloper.cli.config import Config


class ConfigSpec(Spec):
    """Top-level spec that bundles a DAG with its runner, IO, and backfiller settings."""

    backfiller: BackfillerSpec | None = None
    runner: RunnerSpec | None = None
    io: dict[str, IOSpec] = Field(default_factory=dict)
    dag: DAGSpec

    def reconstruct(self) -> Config:
        """Reconstruct the config from the spec."""
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
