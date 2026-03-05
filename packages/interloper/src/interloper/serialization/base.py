"""Base classes for the Spec/Serializable serialization pattern."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from interloper.utils.imports import get_object_path


class Spec(BaseModel, ABC):
    """Pydantic model that captures the state needed to reconstruct a framework object.

    Subclasses store constructor arguments and import paths as plain data,
    then rebuild the live object via ``reconstruct()``. This makes specs
    safe to serialize to JSON, send across processes, or persist to a database.
    """

    @abstractmethod
    def reconstruct(self) -> Any:
        """Reconstruct the live framework object from this spec."""


T = TypeVar("T", bound=Spec)


class Serializable(ABC, Generic[T]):
    """Mixin for framework objects that can produce a Spec of themselves.

    Implementors define ``to_spec()`` to return a ``Spec`` subclass capturing
    whatever state is needed to later ``reconstruct()`` the object.
    """

    @property
    def path(self) -> str:
        """Fully qualified import path for this object's class."""
        return get_object_path(type(self))

    @abstractmethod
    def to_spec(self) -> T:
        """Convert this object into a serializable Spec."""
