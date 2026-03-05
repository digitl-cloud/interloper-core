"""Configuration for the CLI."""

from dataclasses import dataclass, field

from interloper.backfillers.base import Backfiller
from interloper.dag.base import DAG
from interloper.io.base import IO
from interloper.runners.base import Runner
from interloper.serialization.config import ConfigSpec


@dataclass(frozen=True)
class Config:
    """The configuration for the CLI."""

    dag: DAG
    backfiller: Backfiller | None = None
    runner: Runner | None = None
    io: dict[str, IO] = field(default_factory=dict)

    def to_spec(self) -> ConfigSpec:
        """Convert to a serializable ConfigSpec.

        Returns:
            The serializable ConfigSpec representation.
        """
        return ConfigSpec(
            backfiller=self.backfiller.to_spec() if self.backfiller is not None else None,
            runner=self.runner.to_spec() if self.runner is not None else None,
            io={k: v.to_spec() for k, v in self.io.items()},
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
        spec = ConfigSpec.model_validate(data)
        return spec.reconstruct()
