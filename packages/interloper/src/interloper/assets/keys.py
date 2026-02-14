"""Typed key classes for identifying assets.

These are ``str`` subclasses that add no behaviour — they exist purely as type
markers so that pyright (and humans) can distinguish between, e.g., an asset
definition key and an asset instance key.
"""


class AssetDefinitionKey(str):
    """Key identifying an asset definition.

    Format: ``{source-name}:{asset-name}`` for source-bound assets,
    or just ``{asset-name}`` for standalone assets.
    """


class AssetInstanceKey(str):
    """Key identifying an asset instance.

    Format: ``{source-name}:{asset-name}`` for source-bound assets,
    or just ``{asset-name}`` for standalone assets.
    """
