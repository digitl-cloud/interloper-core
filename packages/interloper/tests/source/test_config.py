"""Tests for source configuration base class."""

from interloper.source.config import Config


class TestConfig:
    """Tests for the Config base class."""

    def test_subclass_with_fields(self):
        """Config subclass accepts typed fields."""
        class MyConfig(Config):
            api_key: str = "default"
            base_url: str = "https://example.com"

        config = MyConfig()
        assert config.api_key == "default"
        assert config.base_url == "https://example.com"

    def test_subclass_with_override(self):
        """Config subclass accepts explicit values."""
        class MyConfig(Config):
            api_key: str = "default"

        config = MyConfig(api_key="override")
        assert config.api_key == "override"
