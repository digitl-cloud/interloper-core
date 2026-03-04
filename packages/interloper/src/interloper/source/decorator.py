"""Decorator for defining sources."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, overload

from interloper.assets.base import AssetDefinition
from interloper.source.base import SourceDefinition
from interloper.source.config import Config

if TYPE_CHECKING:
    from interloper.normalizer.base import Normalizer
    from interloper.normalizer.strategy import MaterializationStrategy


@overload
def source(cls: type) -> SourceDefinition: ...


@overload
def source(
    *,
    name: str | None = None,
    config: type[Config] | None = None,
    tags: Sequence[str] | None = None,
    dataset: str | None = None,
    normalizer: Normalizer | None = None,
    strategy: MaterializationStrategy | None = None,
) -> Callable[[type], SourceDefinition]: ...


def source(
    cls: type | None = None,
    *,
    name: str | None = None,
    config: type[Config] | None = None,
    tags: Sequence[str] | None = None,
    dataset: str | None = None,
    normalizer: Normalizer | None = None,
    strategy: MaterializationStrategy | None = None,
) -> SourceDefinition | Callable[[type], SourceDefinition]:
    """Class decorator to define a source.

    Can be used with or without parentheses:
        @source
        class MySource: ...

        @source(config=MyConfig, normalizer=Normalizer())
        class MySource: ...
    """

    def decorator(cls: type) -> SourceDefinition:
        collected: list[AssetDefinition] = [
            attr_value for attr_value in cls.__dict__.values() if isinstance(attr_value, AssetDefinition)
        ]

        return SourceDefinition(
            cls=cls,
            asset_defs={ad.name: ad for ad in collected},
            name=name or "",
            config=config,
            tags=tuple(tags) if tags else (),
            dataset=dataset,
            normalizer=normalizer,
            strategy=strategy,
        )

    # Called without parentheses: @source
    if cls is not None:
        return decorator(cls)

    # Called with parentheses: @source(...)
    return decorator
