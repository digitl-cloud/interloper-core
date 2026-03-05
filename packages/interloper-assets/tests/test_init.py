"""Tests for interloper_assets package exports."""

import interloper as il


class TestPackageExports:
    """Key symbols are importable from interloper_assets."""

    def test_import_source_registry(self):
        """SOURCE_REGISTRY is importable and is a dict."""
        from interloper_assets import SOURCE_REGISTRY

        assert isinstance(SOURCE_REGISTRY, dict)

    def test_import_get_source_and_config(self):
        """get_source_and_config is importable and callable."""
        from interloper_assets import get_source_and_config

        assert callable(get_source_and_config)

    def test_import_get_all_sources(self):
        """get_all_sources is importable and callable."""
        from interloper_assets import get_all_sources

        assert callable(get_all_sources)

    def test_import_demo_source(self):
        """DemoSource is importable from the package."""
        from interloper_assets import DemoSource

        assert isinstance(DemoSource, il.SourceDefinition)

    def test_import_demo_config(self):
        """DemoConfig is importable from the package."""
        from interloper_assets import DemoConfig

        assert issubclass(DemoConfig, il.Config)


class TestSourceRegistry:
    """SOURCE_REGISTRY contents and structure."""

    def test_registry_is_non_empty(self):
        """Registry contains at least one entry."""
        from interloper_assets import SOURCE_REGISTRY

        assert len(SOURCE_REGISTRY) > 0

    def test_demo_source_in_registry(self):
        """DemoSource is registered."""
        from interloper_assets import SOURCE_REGISTRY, DemoSource

        assert DemoSource.name in SOURCE_REGISTRY

    def test_registry_values_are_tuples(self):
        """Each registry value is a (SourceDefinition, config_type|None) tuple."""
        from interloper_assets import SOURCE_REGISTRY

        for key, value in SOURCE_REGISTRY.items():
            assert isinstance(value, tuple), f"Registry entry '{key}' is not a tuple"
            assert len(value) == 2, f"Registry entry '{key}' does not have two elements"
            source_def, config_type = value
            assert isinstance(source_def, il.SourceDefinition), (
                f"Registry entry '{key}' first element is not a SourceDefinition"
            )
            if config_type is not None:
                assert issubclass(config_type, il.Config), (
                    f"Registry entry '{key}' config type is not a Config subclass"
                )

    def test_registry_keys_match_source_names(self):
        """Registry keys match the name attribute of their SourceDefinition."""
        from interloper_assets import SOURCE_REGISTRY

        for key, (source_def, _) in SOURCE_REGISTRY.items():
            assert key == source_def.name, (
                f"Registry key '{key}' does not match source name '{source_def.name}'"
            )


class TestGetSourceAndConfig:
    """get_source_and_config lookup function."""

    def test_returns_demo_source(self):
        """Looking up 'DemoSource' returns the correct definition and config."""
        from interloper_assets import DemoConfig, DemoSource, get_source_and_config

        source_def, config_type = get_source_and_config(DemoSource.name)
        assert source_def is DemoSource
        assert config_type is DemoConfig

    def test_unknown_id_raises(self):
        """Looking up an unknown ID raises SourceError."""
        import pytest

        from interloper_assets import get_source_and_config

        with pytest.raises(il.SourceError):
            get_source_and_config("nonexistent_source_xyz")


class TestGetAllSources:
    """get_all_sources function."""

    def test_returns_dict(self):
        """Returns a dict."""
        from interloper_assets import get_all_sources

        result = get_all_sources()
        assert isinstance(result, dict)

    def test_contains_demo_source(self):
        """Returned dict contains the DemoSource."""
        from interloper_assets import DemoSource, get_all_sources

        result = get_all_sources()
        assert DemoSource.name in result

    def test_returns_copy(self):
        """Returned dict is a copy, not the original registry."""
        from interloper_assets import SOURCE_REGISTRY, get_all_sources

        result = get_all_sources()
        assert result is not SOURCE_REGISTRY
        assert result == SOURCE_REGISTRY
