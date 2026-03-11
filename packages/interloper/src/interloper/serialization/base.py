"""Base classes for the Spec/Serializable/Component serialization pattern."""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from interloper.utils.imports import get_object_path, import_from_path

if TYPE_CHECKING:
    from interloper.source.config import Config


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


class ComponentInstanceSpec(InstanceSpec):
    """Universal instance spec for components identified by import path + config + init kwargs.

    Separates ``config`` (Pydantic model data) from ``init`` (other constructor
    kwargs) so that configuration is a first-class, declarative field in YAML
    and JSON specs.
    """

    path: str
    config: dict[str, Any] | None = None
    init: dict[str, Any] = Field(default_factory=dict)

    def reconstruct(self) -> Any:
        """Import the class from *path* and instantiate with *config* and *init* kwargs.

        Config is spread as keyword arguments (the component IS its config).

        Returns:
            The reconstructed object.
        """
        cls = import_from_path(self.path)
        kwargs = dict(self.init)
        if self.config:
            kwargs.update(self.config)
        return cls(**kwargs)


class ComponentDefinitionSpec(DefinitionSpec):
    """Universal definition spec for API exposure.

    Every entity type (IO, Runner, Backfiller, Source, Asset) produces a
    ``ComponentDefinitionSpec`` (or subclass) so the daemon can serve them
    all with a consistent shape.
    """

    key: str
    label: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    config_schema: dict[str, Any] | None = None


class HasInstanceSpec(ABC):
    """Mixin for runtime objects that can produce reconstruction specs.

    Implementors define ``to_spec()`` to return an :class:`InstanceSpec`
    subclass capturing whatever state is needed for cross-process
    reconstruction.
    """

    @property
    def path(self) -> str:
        """Fully qualified import path for this object's class."""
        return get_object_path(type(self))

    @abstractmethod
    def to_spec(self) -> InstanceSpec:
        """Convert this object into an InstanceSpec for reconstruction."""


class HasDefinitionSpec(ABC):
    """Mixin for objects that can produce definition metadata specs.

    Implementors define ``definition_spec()`` to return a
    :class:`DefinitionSpec` subclass capturing metadata for API
    exposure and display.
    """

    @abstractmethod
    def definition_spec(self) -> DefinitionSpec:
        """Produce a DefinitionSpec describing this object's metadata."""


# Pydantic warns when a field name shadows an inherited attribute.
# ``schema`` shadows the deprecated ``BaseModel.schema()`` classmethod —
# we declare it on Component so subclasses (Asset) inherit it cleanly.
warnings.filterwarnings("ignore", message='Field name "schema"')


class Component(BaseModel, HasInstanceSpec, HasDefinitionSpec, ABC):
    """Fundamental building block: locatable, configurable, serializable.

    Every entity in the framework (IO, Runner, Backfiller, Source, Asset)
    extends ``Component``. It provides:

    - **Auto ``to_spec()``** — generates a :class:`ComponentInstanceSpec` from
      the instance's model fields via ``model_dump()``.
      Source/Asset override this for their nested-IO complexity.
    - **``definition_spec()``** — classmethod that generates a
      :class:`ComponentDefinitionSpec` from class-level metadata.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    key: str = ""

    def to_spec(self) -> ComponentInstanceSpec:
        """Generate an instance spec for reconstruction.

        Uses ``model_dump()`` to produce the ``config`` dict directly.
        Source and Asset override this method entirely.

        Returns:
            A ComponentInstanceSpec capturing this instance's state.
        """
        config_data = self.model_dump(mode="json", exclude={"key"}, exclude_none=True) or None
        init_kwargs = {"key": self.key} if self.key else {}
        return ComponentInstanceSpec(path=self.path, config=config_data, init=init_kwargs)

    @classmethod
    def definition_spec(cls) -> ComponentDefinitionSpec:
        """Generate a definition spec for API exposure.

        Builds a :class:`ComponentDefinitionSpec` from the class's JSON
        schema, name, and docstring.  Used by IO, Runner, and Backfiller.

        Returns:
            A ComponentDefinitionSpec describing this component type.
        """
        # Derive label by stripping known suffixes from the class name
        label = cls.__name__
        for suffix in ("IO", "Runner", "Backfiller", "Adapter"):
            if label.endswith(suffix) and len(label) > len(suffix):
                label = label[: -len(suffix)]
                break

        config_schema = cls.model_json_schema(mode="serialization")
        return ComponentDefinitionSpec(
            key=cls.__name__,
            label=label,
            description=cls.__doc__ or "",
            config_schema=config_schema,
        )


# ---------------------------------------------------------------------------
# Shared reconstruction helpers
# ---------------------------------------------------------------------------

def _reconstruct_one(v: Any) -> Any:
    """Reconstruct a single component from a spec, dict, or import-path string.

    Args:
        v: A ``ComponentInstanceSpec``, a dict with at least a ``"path"`` key,
            or a dotted import-path string.

    Returns:
        The reconstructed component instance.
    """
    if isinstance(v, ComponentInstanceSpec):
        return v.reconstruct()
    if isinstance(v, dict):
        return ComponentInstanceSpec(**v).reconstruct()
    if isinstance(v, str):
        return ComponentInstanceSpec(path=v).reconstruct()
    return v


def reconstruct_components(
    spec: Any,
) -> Any | list[Any] | None:
    """Reconstruct component instance(s) from spec(s).

    Accepts a single item, a list, or ``None``.  Each item may be a
    ``ComponentInstanceSpec``, a dict (spread into ``ComponentInstanceSpec``),
    a dotted import-path string, or an already-instantiated component
    (returned as-is).

    Used by ``SourceInstanceSpec``, ``AssetInstanceSpec``, and
    ``DatabaseIO`` to reconstruct nested components.

    Args:
        spec: A single spec/dict/string, a list thereof, or ``None``.

    Returns:
        The reconstructed component(s), or ``None``.
    """
    if spec is None:
        return None
    if isinstance(spec, list):
        return [_reconstruct_one(v) for v in spec]
    return _reconstruct_one(spec)


def reconstruct_config(
    definition: Any,
    data: dict[str, Any] | None,
) -> Config | None:
    """Reconstruct a Config instance from a definition's config type and raw data.

    Used by both ``SourceInstanceSpec`` and ``AssetInstanceSpec`` to avoid
    duplication.

    Args:
        definition: A SourceDefinition or AssetDefinition with a ``config`` attribute
            pointing to the Config class (or None).
        data: Raw config data dictionary, or None.

    Returns:
        The validated Config instance, or None.
    """
    if definition.config is not None and data is not None:
        return definition.config.model_validate(data)
    return None
