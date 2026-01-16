"""Serialization specs for framework entities."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from interloper.utils.imports import get_object_path


class Spec(BaseModel, ABC):
    """Abstract base class for all serialization specs."""

    @abstractmethod
    def reconstruct(self) -> Any:
        """Reconstruct the object from the spec."""
        pass


T = TypeVar("T", bound=Spec)


class Serializable(Generic[T], ABC):
    """Abstract base class for all serializable objects."""

    @property
    def path(self) -> str:
        """The import path or identifier for this object."""
        return get_object_path(type(self))

    @abstractmethod
    def to_spec(self) -> T:
        """Convert to serializable spec."""
        pass
