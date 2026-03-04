"""Schema inference and validation for normalized asset data."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ValidationError, create_model


def infer_schema(
    rows: list[dict[str, Any]],
    name: str = "InferredSchema",
) -> type[BaseModel]:
    """Infer a Pydantic model from a list of row dicts.

    Examines the values across all rows for each key and maps Python types
    to Pydantic field types.  All fields are ``Optional`` because any key
    may be absent in some rows.

    Args:
        rows: Non-empty list of dicts to infer from.
        name: Class name for the generated model.

    Returns:
        A dynamically created Pydantic BaseModel subclass.

    Raises:
        ValueError: If *rows* is empty.
    """
    if not rows:
        raise ValueError("Cannot infer schema from empty data.")

    # Collect all non-None types seen for each key
    key_types: dict[str, set[type]] = {}
    for row in rows:
        for k, v in row.items():
            if k not in key_types:
                key_types[k] = set()
            if v is not None:
                key_types[k].add(type(v))

    # Build field definitions: (type | None, default_value)
    field_definitions: dict[str, Any] = {}
    for key, types_seen in key_types.items():
        field_type = _resolve_field_type(types_seen)
        field_definitions[key] = (field_type | None, None)

    return create_model(name, **field_definitions)


def validate_schema(
    rows: list[dict[str, Any]],
    schema: type[BaseModel],
) -> None:
    """Validate each row against a Pydantic schema.

    Stops at the first row that fails validation.

    Args:
        rows: List of row dicts.
        schema: Pydantic model class to validate against.

    Raises:
        ValueError: If any row fails validation.
    """
    for i, row in enumerate(rows):
        try:
            schema.model_validate(row)
        except ValidationError as e:
            raise ValueError(f"Schema validation failed on row {i}: {e}") from e


def _resolve_field_type(types_seen: set[type]) -> type:
    """Resolve a set of observed Python types into a single Pydantic-compatible type.

    Rules:
    - Empty set (all values None) → ``Any``
    - Single type → that type
    - ``{int, float}`` → ``float`` (numeric widening)
    - Multiple incompatible types → ``Any``
    """
    if not types_seen:
        return Any  # type: ignore[return-value]

    if len(types_seen) == 1:
        return types_seen.pop()

    # Numeric widening: int + float -> float
    if types_seen == {int, float}:
        return float

    return Any  # type: ignore[return-value]
