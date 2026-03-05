"""Source and asset configuration base class."""

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Base class for source and asset configuration.

    Subclass ``Config`` to declare typed, validated settings for a source or
    asset.  Values are resolved from environment variables automatically via
    ``pydantic-settings``, or can be supplied explicitly at instantiation time.

    Example:
        >>> class MyConfig(Config):
        ...     api_key: str
        ...     base_url: str = "https://api.example.com"
    """



