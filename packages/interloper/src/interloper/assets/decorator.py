"""Decorator for defining assets."""

from collections.abc import Callable
from typing import Any, overload

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from interloper.assets.base import AssetDefinition
from interloper.io.base import IO
from interloper.partitioning.base import PartitionConfig


@overload
def asset(func: Callable[..., Any]) -> AssetDefinition: ...


@overload
def asset(
    *,
    name: str | None = None,
    schema: type[BaseModel] | None = None,
    config: type[BaseSettings] | None = None,
    io: IO | dict[str, IO] | None = None,
    partitioning: PartitionConfig | None = None,
    dataset: str | None = None,
    default_io_key: str | None = None,
    deps: dict[str, str] | None = None,
) -> Callable[[Callable[..., Any]], AssetDefinition]: ...


def asset(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    schema: type[BaseModel] | None = None,
    config: type[BaseSettings] | None = None,
    io: IO | dict[str, IO] | None = None,
    partitioning: PartitionConfig | None = None,
    dataset: str | None = None,
    default_io_key: str | None = None,
    deps: dict[str, str] | None = None,
) -> AssetDefinition | Callable[[Callable[..., Any]], AssetDefinition]:
    """Decorator to define an asset.

    Can be used with or without parentheses:
        @asset
        def my_asset(): ...

        @asset(schema=MySchema)
        def my_asset(): ...
    """
    # Validate default_io_key if multiple IOs
    if isinstance(io, dict) and len(io) > 1 and default_io_key is None:
        raise ValueError("default_io_key is required when io is a dict with multiple keys")
    
    if isinstance(io, dict) and default_io_key and default_io_key not in io:
        raise ValueError(f"default_io_key '{default_io_key}' not found in io dict keys: {list(io.keys())}")
    
    def decorator(f: Callable[..., Any]) -> AssetDefinition:
        return AssetDefinition(
            func=f,
            name=name or "",
            schema=schema,
            config=config,
            io=io,
            partitioning=partitioning,
            dataset=dataset,
            default_io_key=default_io_key,
            deps=deps or {},
        )
    
    # Called without parentheses: @asset
    if func is not None:
        return decorator(func)
    
    # Called with parentheses: @asset(...)
    return decorator

