import interloper as il
from pydantic_settings import BaseSettings

from interloper_assets.adservice.source import AdserviceConfig, adservice
from interloper_assets.adup.source import AdupConfig, adup
from interloper_assets.example.source import ExampleConfig, example_source


def get_source_and_config(id: str) -> tuple[il.SourceDefinition, type[BaseSettings] | None]:
    if id == "adservice":
        return adservice, AdserviceConfig
    elif id == "adup":
        return adup, AdupConfig
    elif id == "example":
        return example_source, ExampleConfig
    else:
        raise ValueError(f"Unknown source ID: {id}")


__all__ = [
    "adup",
    "adservice",
    "example_source",
    "get_source_and_config",
]
