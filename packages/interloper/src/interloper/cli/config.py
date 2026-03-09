"""Configuration for the CLI."""

from dataclasses import dataclass, field

from interloper.backfillers.base import Backfiller
from interloper.dag.base import DAG
from interloper.io.base import IO
from interloper.runners.base import Runner
from interloper.serialization.config import ConfigInstanceSpec


@dataclass(frozen=True)
class Config:
    """The configuration for the CLI."""

    dag: DAG
    backfiller: Backfiller | None = None
    runner: Runner | None = None
    io: list[IO] = field(default_factory=list)

    def to_spec(self) -> ConfigInstanceSpec:
        """Convert to a serializable ConfigSpec.

        Returns:
            The serializable ConfigSpec representation.
        """
        return ConfigInstanceSpec(
            backfiller=self.backfiller.to_spec() if self.backfiller is not None else None,
            runner=self.runner.to_spec() if self.runner is not None else None,
            io=[v.to_spec() for v in self.io],
            dag=self.dag.to_spec(),
        )

    def to_json(self) -> str:
        """Serialize the config to a JSON string.

        Returns:
            The JSON string representation of the config.
        """
        return self.to_spec().model_dump_json()

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        """Load the config from a dictionary.

        Returns:
            The reconstructed Config instance.
        """
        spec = ConfigInstanceSpec.model_validate(data)
        return spec.reconstruct()
