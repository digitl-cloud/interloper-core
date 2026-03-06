"""Base classes for the Spec/Serializable serialization pattern."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

from interloper.utils.imports import get_object_path, import_from_path


class Spec(BaseModel):
    """Common base for all serialization specs.

    Subclasses fall into two families:

    - **InstanceSpec** — captures the state needed to *reconstruct* a runtime
      object (import path, constructor args, etc.).
    - **DefinitionSpec** — captures *metadata* about a definition for display
      or API consumption (label, tags, schema fields, …).  No reconstruction
      is needed because definitions are loaded via import / registry.
    """


class DefinitionSpec(Spec):
    """Spec that describes a definition's metadata.

    Unlike :class:`InstanceSpec`, a definition spec does not need a
    ``reconstruct()`` method — definitions are discovered via registries
    or direct import, not rebuilt from serialized state.
    """


class InstanceSpec(Spec, ABC):
    """Spec that captures the state needed to reconstruct a framework object.

    Subclasses store constructor arguments and import paths as plain data,
    then rebuild the live object via ``reconstruct()``. This makes specs
    safe to serialize to JSON, send across processes, or persist to a database.
    """

    @abstractmethod
    def reconstruct(self) -> Any:
        """Reconstruct the live framework object from this spec."""


class PathInitSpec(InstanceSpec):
    """Concrete instance spec for objects identified by import path + constructor kwargs.

    Covers the common pattern used by IO handlers, runners, and backfillers
    where reconstruction is simply ``import_from_path(path)(**init)``.
    """

    path: str
    init: dict[str, Any] = Field(default_factory=dict)

    def reconstruct(self) -> Any:
        """Import the class from *path* and instantiate with *init* kwargs.

        Returns:
            The reconstructed object.
        """
        return import_from_path(self.path)(**self.init)


T = TypeVar("T", bound=Spec)


class Serializable(ABC, Generic[T]):
    """Mixin for framework objects that can produce a Spec of themselves.

    Implementors define ``to_spec()`` to return a :class:`Spec` subclass
    capturing whatever state is needed for serialization or later
    reconstruction.
    """

    @property
    def path(self) -> str:
        """Fully qualified import path for this object's class."""
        return get_object_path(type(self))

    @abstractmethod
    def to_spec(self) -> T:
        """Convert this object into a serializable Spec."""
