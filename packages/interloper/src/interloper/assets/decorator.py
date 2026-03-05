"""Decorator for defining assets."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, overload

from pydantic import BaseModel

from interloper.assets.base import AssetDefinition
from interloper.assets.keys import AssetDefinitionKey
from interloper.io.base import IO
from interloper.partitioning.base import PartitionConfig
from interloper.source.config import Config

if TYPE_CHECKING:
    from interloper.normalizer.base import Normalizer
    from interloper.normalizer.strategy import MaterializationStrategy


@overload
def asset(func: Callable[..., Any]) -> AssetDefinition: ...


@overload
def asset(
    *,
    name: str | None = None,
    schema: type[BaseModel] | None = None,
    config: type[Config] | None = None,
    io: IO | None = None,
    normalizer: Normalizer | None = None,
    strategy: MaterializationStrategy | None = None,
    partitioning: PartitionConfig | None = None,
    dataset: str | None = None,
    requires: dict[str, AssetDefinitionKey] | None = None,
    tags: Sequence[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Callable[[Callable[..., Any]], AssetDefinition]: ...


def asset(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    schema: type[BaseModel] | None = None,
    config: type[Config] | None = None,
    io: IO | None = None,
    normalizer: Normalizer | None = None,
    strategy: MaterializationStrategy | None = None,
    partitioning: PartitionConfig | None = None,
    dataset: str | None = None,
    requires: dict[str, AssetDefinitionKey] | None = None,
    tags: Sequence[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetDefinition | Callable[[Callable[..., Any]], AssetDefinition]:
    """Decorator to define an asset.

    Can be used with or without parentheses::

        @asset
        def my_asset(): ...

        @asset(schema=MySchema, normalizer=Normalizer())
        def my_asset(): ...

    Returns:
        An AssetDefinition, or a decorator that produces one.
    """

    def decorator(f: Callable[..., Any]) -> AssetDefinition:
        return AssetDefinition(
            func=f,
            name=name or "",
            schema=schema,
            config=config,
            io=io,
            normalizer=normalizer,
            strategy=strategy,
            partitioning=partitioning,
            dataset=dataset,
            requires=requires or {},
            tags=tuple(tags) if tags else (),
            metadata=dict(metadata) if metadata else {},
        )

    # Called without parentheses: @asset
    if func is not None:
        return decorator(func)

    # Called with parentheses: @asset(...)
    return decorator
