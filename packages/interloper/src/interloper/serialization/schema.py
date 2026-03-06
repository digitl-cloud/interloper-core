"""Schema field spec and extraction utilities."""

from __future__ import annotations

import inspect
from typing import Any, get_args, get_origin

from pydantic import BaseModel


class SchemaFieldSpec(BaseModel):
    """A single field in an asset's output schema."""

    name: str
    type: str
    description: str = ""


def _type_name(annotation: Any) -> str:
    """Convert a Python type annotation to a human-readable string.

    Returns:
        A human-readable string representation of the annotation.
    """
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        args_str = ", ".join(_type_name(a) for a in args)
        origin_name = getattr(origin, "__name__", str(origin))
        return f"{origin_name}[{args_str}]" if args else origin_name
    if inspect.isclass(annotation):
        return annotation.__name__
    return str(annotation)


def extract_schema_fields(
    schema: type[BaseModel] | None,
) -> list[SchemaFieldSpec] | None:
    """Extract field metadata from a Pydantic model class.

    Args:
        schema: A Pydantic model class whose fields should be extracted,
            or ``None``.

    Returns:
        A list of :class:`SchemaFieldSpec` instances, or ``None`` if
        *schema* is ``None``.
    """
    if schema is None:
        return None
    fields: list[SchemaFieldSpec] = []
    for name, field_info in schema.model_fields.items():
        fields.append(
            SchemaFieldSpec(
                name=name,
                type=_type_name(field_info.annotation),
                description=field_info.description or "",
            )
        )
    return fields
