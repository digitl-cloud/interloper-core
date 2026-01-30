"""Decorator for defining sources."""

from collections.abc import Callable
from typing import Any, overload

from pydantic_settings import BaseSettings

from interloper.io.base import IO
from interloper.source.base import SourceDefinition


@overload
def source(func: Callable[..., Any]) -> SourceDefinition: ...


@overload
def source(
    *,
    name: str | None = None,
    config: type[BaseSettings] | None = None,
    dataset: str | None = None,
    io: IO | dict[str, IO] | None = None,
    default_io_key: str | None = None,
) -> Callable[[Callable[..., Any]], SourceDefinition]: ...


def source(
    func: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    config: type[BaseSettings] | None = None,
    dataset: str | None = None,
    io: IO | dict[str, IO] | None = None,
    default_io_key: str | None = None,
) -> SourceDefinition | Callable[[Callable[..., Any]], SourceDefinition]:
    """Decorator to define a source.

    Can be used with or without parentheses:
        @source
        def my_source(): ...

        @source(config=MyConfig)
        def my_source(config): ...
    """
    # Validate default_io_key if multiple IOs
    if isinstance(io, dict) and len(io) > 1 and default_io_key is None:
        raise ValueError("default_io_key is required when io is a dict with multiple keys")

    if isinstance(io, dict) and default_io_key and default_io_key not in io:
        raise ValueError(f"default_io_key '{default_io_key}' not found in io dict keys: {list(io.keys())}")

    def decorator(f: Callable[..., Any]) -> SourceDefinition:
        return SourceDefinition(
            func=f,
            name=name,  # type: ignore[arg-type]
            config=config,
            dataset=dataset,
            io=io,
            default_io_key=default_io_key,
        )

    # Called without parentheses: @source
    if func is not None:
        return decorator(func)

    # Called with parentheses: @source(...)
    return decorator
