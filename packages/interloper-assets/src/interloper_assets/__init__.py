import interloper as il
from pydantic_settings import BaseSettings

from interloper_assets.adservice.source import AdserviceConfig, adservice
from interloper_assets.adup.source import AdupConfig, adup
from interloper_assets.demo.source import DemoConfig, demo_source


def get_source_and_config(id: str) -> tuple[il.SourceDefinition, type[BaseSettings] | None]:
    if id == "adservice":
        return adservice, AdserviceConfig
    elif id == "adup":
        return adup, AdupConfig
    elif id == "demo":
        return demo_source, DemoConfig
    else:
        raise ValueError(f"Unknown source ID: {id}")


__all__ = [
    "adup",
    "adservice",
    "demo_source",
    "get_source_and_config",
]
