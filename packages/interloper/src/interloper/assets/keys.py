"""Typed key classes for identifying assets.

These are ``str`` subclasses that add no behaviour — they exist purely as type
markers so that pyright (and humans) can distinguish between, e.g., an asset
definition key and an asset instance key.
"""

from __future__ import annotations

from typing import Any


class AssetDefinitionKey(str):
    """Key identifying an asset definition.

    Format: ``{source-name}:{asset-name}`` for source-bound assets,
    or just ``{asset-name}`` for standalone assets.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, source: type, handler: Any) -> Any:
        """Accept plain strings — these are pure type markers.

        Returns:
            The core schema for plain string validation.
        """
        return handler(str)


class AssetInstanceKey(str):
    """Key identifying an asset instance.

    Format: ``{source-instance-key}:{asset-name}`` for source-bound assets,
    or just ``{asset-name}`` for standalone assets.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, source: type, handler: Any) -> Any:
        """Accept plain strings — these are pure type markers.

        Returns:
            The core schema for plain string validation.
        """
        return handler(str)
