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
    *,
    strict: bool = False,
) -> None:
    """Validate each row against a Pydantic schema.

    Stops at the first row that fails validation.

    Args:
        rows: List of row dicts.
        schema: Pydantic model class to validate against.
        strict: When ``True``, reject rows that contain keys not defined
            in the schema and rows that are missing required schema fields.

    Raises:
        ValueError: If any row fails validation.
    """
    schema_fields = set(schema.model_fields.keys()) if strict else None
    for i, row in enumerate(rows):
        if schema_fields is not None:
            extra = set(row.keys()) - schema_fields
            if extra:
                raise ValueError(f"Schema validation failed on row {i}: extra fields not in schema: {sorted(extra)}")
            missing = schema_fields - set(row.keys())
            # Only flag missing keys that are required (no default)
            required_missing = {k for k in missing if schema.model_fields[k].is_required()}
            if required_missing:
                raise ValueError(
                    f"Schema validation failed on row {i}: missing required fields: {sorted(required_missing)}"
                )
        try:
            schema.model_validate(row)
        except ValidationError as e:
            raise ValueError(f"Schema validation failed on row {i}: {e}") from e


def reconcile_schema(
    rows: list[dict[str, Any]],
    schema: type[BaseModel],
) -> list[dict[str, Any]]:
    """Reconcile rows against a Pydantic schema.

    For each row:
    1. Filter to only the keys defined in *schema* (drop extras).
    2. For missing keys that have a default, omit them so Pydantic applies
       the default.  For missing *required* keys, supply ``None`` — Pydantic
       will accept it when the field is nullable (e.g. ``str | None``) and
       reject it otherwise, which is the desired behaviour.
    3. Coerce values to the schema's types using ``model_validate()``.

    This is more permissive than :func:`validate_schema` — it actively
    transforms data to match the schema rather than rejecting mismatches.

    Args:
        rows: List of row dicts.
        schema: Pydantic model class describing the target shape.

    Returns:
        A new list of row dicts with columns aligned and types coerced.

    Raises:
        ValueError: If any row cannot be coerced (e.g. ``"abc"`` → ``int``)
            or a required non-nullable field is missing.
    """
    if not rows:
        return []

    schema_fields = set(schema.model_fields.keys())

    result: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        filtered = {k: row[k] for k in schema_fields if k in row}
        # For missing required fields, fill with None and let Pydantic
        # accept (nullable) or reject (non-nullable).  Fields with
        # defaults are omitted so Pydantic applies the default value.
        for k in schema_fields - filtered.keys():
            if schema.model_fields[k].is_required():
                filtered[k] = None
        try:
            instance = schema.model_validate(filtered)
        except ValidationError as e:
            raise ValueError(f"Reconciliation failed on row {i}: {e}") from e
        result.append(instance.model_dump())
    return result


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
