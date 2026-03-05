"""Normalizer: type-native data normalization, transformation, and schema inference/validation."""

from __future__ import annotations

import types
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from interloper.errors import NormalizerError
from interloper.schema import infer_schema, reconcile_schema, validate_schema
from interloper.utils.text import to_snake_case


@dataclass
class Normalizer:
    """Type-native normalizer for ``list[dict]`` asset data.

    Accepts arbitrary return types (``dict``, ``list[dict]``, ``BaseModel``,
    ``list[BaseModel]``, ``Generator``), coerces to ``list[dict]``, then
    applies optional transformations (column-name normalization, nested-dict
    flattening, missing-column fill).

    Usage::

        @asset(normalizer=Normalizer())
        def my_asset(context):
            return [{"UserName": "alice", "Address": {"City": "NYC"}}]

    Attributes:
        normalize_columns: Convert column names to snake_case.
        flatten_max_level: Maximum nesting depth to flatten.  ``0`` disables
            flattening, ``None`` flattens without limit, a positive ``int``
            flattens up to that many levels.
        flatten_separator: Separator for flattened key names.
        fill_missing: Fill missing keys across rows with ``None`` so every row
            has the same columns.
        infer: When ``True`` and no schema is provided, infer a Pydantic model
            from the data.
    """

    normalize_columns: bool = True
    flatten_max_level: int | None = 0
    flatten_separator: str = "_"
    fill_missing: bool = True
    infer: bool = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalize(self, data: Any) -> list[dict[str, Any]]:
        """Normalize *data* to ``list[dict]`` with configured transformations.

        Coerces the input to ``list[dict]``, then applies flatten, column
        rename, and fill-missing in order.

        Args:
            data: Raw asset output (any supported type).

        Returns:
            Normalized list of row dicts.
        """
        rows = self._coerce(data)

        if self.flatten_max_level is None or self.flatten_max_level > 0:
            rows = [self._flatten_dict(row) for row in rows]

        if self.normalize_columns:
            rows = [{self.column_name(k): v for k, v in row.items()} for row in rows]

        if self.fill_missing:
            rows = self._fill_missing_keys(rows)

        return rows

    def infer_schema(self, data: list[dict[str, Any]]) -> type[BaseModel]:
        """Infer a Pydantic model from normalized data.

        Args:
            data: Normalized list of row dicts (output of :meth:`normalize`).

        Returns:
            A dynamically created Pydantic ``BaseModel`` subclass.
        """
        return infer_schema(data)

    def validate_schema(
        self,
        data: list[dict[str, Any]],
        schema: type[BaseModel],
        *,
        strict: bool = False,
    ) -> None:
        """Validate normalized data against a Pydantic schema.

        Args:
            data: Normalized list of row dicts.
            schema: Pydantic model class to validate against.
            strict: When ``True``, reject extra and missing required fields.
        """
        validate_schema(data, schema, strict=strict)

    def reconcile(
        self,
        data: list[dict[str, Any]],
        schema: type[BaseModel],
    ) -> list[dict[str, Any]]:
        """Reconcile normalized data against a Pydantic schema.

        Aligns columns to the schema (drops extras, adds missing) and
        coerces values to the schema's types via Pydantic ``model_validate``.

        Args:
            data: Normalized list of row dicts.
            schema: Pydantic model class describing the target shape.

        Returns:
            Reconciled list of row dicts.
        """
        return reconcile_schema(data, schema)

    def column_name(self, name: str) -> str:
        """Transform a column name according to the normalizer's convention.

        The default implementation converts to ``snake_case``.

        Args:
            name: Original column name.

        Returns:
            Transformed column name.
        """
        return to_snake_case(name)

    # ------------------------------------------------------------------
    # Type coercion
    # ------------------------------------------------------------------

    def _coerce(self, data: Any) -> list[dict[str, Any]]:
        """Coerce arbitrary data to ``list[dict]``.

        Supported types: ``dict``, ``list[dict]``, ``BaseModel``,
        ``list[BaseModel]``, ``Generator`` / ``Iterator``, ``None``.

        Returns:
            The coerced list of row dicts.

        Raises:
            NormalizerError: If the data type is unsupported.
        """
        if data is None:
            return []

        # Generator / Iterator -> consume then re-process
        if isinstance(data, (Generator, Iterator, types.GeneratorType)):
            return self._coerce(list(data))

        # Single Pydantic model
        if isinstance(data, BaseModel):
            return [data.model_dump()]

        # list
        if isinstance(data, list):
            if not data:
                return []
            first = data[0]
            if isinstance(first, dict):
                return data
            if isinstance(first, BaseModel):
                return [item.model_dump() for item in data]
            raise NormalizerError(
                f"Normalizer received list[{type(first).__name__}], expected list[dict] or list[BaseModel]."
            )

        # Single dict
        if isinstance(data, dict):
            return [data]

        raise NormalizerError(
            f"Normalizer does not support type {type(data).__name__}. "
            "Supported: dict, list[dict], BaseModel, list[BaseModel], Generator."
        )

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def _flatten_dict(
        self,
        d: dict[str, Any],
        parent_key: str = "",
        level: int = 0,
    ) -> dict[str, Any]:
        """Flatten nested dicts using separator-joined keys.

        Returns:
            A flat dict with separator-joined keys.
        """
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{self.flatten_separator}{k}" if parent_key else k
            if isinstance(v, dict) and (self.flatten_max_level is None or level < self.flatten_max_level):
                items.extend(self._flatten_dict(v, new_key, level + 1).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def _fill_missing_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Ensure every row has the same set of keys, filling gaps with ``None``.

        Returns:
            Rows with a uniform set of keys.
        """
        if not rows:
            return rows

        # Preserve insertion order of keys
        all_keys: dict[str, None] = {}
        for row in rows:
            for k in row:
                all_keys.setdefault(k, None)

        key_set = all_keys.keys()
        return [{k: row.get(k) for k in key_set} for row in rows]
