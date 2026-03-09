"""Tests for interloper_assets package exports."""

import interloper as il
import pytest
from interloper.io.base import IO


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

        assert DemoSource.key in SOURCE_REGISTRY

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

    def test_registry_keys_match_source_keys(self):
        """Registry keys match the key attribute of their SourceDefinition."""
        from interloper_assets import SOURCE_REGISTRY

        for key, (source_def, _) in SOURCE_REGISTRY.items():
            assert key == source_def.key, (
                f"Registry key '{key}' does not match source key '{source_def.key}'"
            )


class TestGetSourceAndConfig:
    """get_source_and_config lookup function."""

    def test_returns_demo_source(self):
        """Looking up 'DemoSource' returns the correct definition and config."""
        from interloper_assets import DemoConfig, DemoSource, get_source_and_config

        source_def, config_type = get_source_and_config(DemoSource.key)
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
        assert DemoSource.key in result

    def test_returns_copy(self):
        """Returned dict is a copy, not the original registry."""
        from interloper_assets import SOURCE_REGISTRY, get_all_sources

        result = get_all_sources()
        assert result is not SOURCE_REGISTRY
        assert result == SOURCE_REGISTRY


class TestPackageIOExports:
    """IO-related symbols are importable from interloper_assets."""

    def test_import_io_registry(self):
        """IO_REGISTRY is importable and is a dict."""
        from interloper_assets import IO_REGISTRY

        assert isinstance(IO_REGISTRY, dict)

    def test_import_get_io(self):
        """get_io is importable and callable."""
        from interloper_assets import get_io

        assert callable(get_io)

    def test_import_get_all_ios(self):
        """get_all_ios is importable and callable."""
        from interloper_assets import get_all_ios

        assert callable(get_all_ios)


class TestIORegistry:
    """IO_REGISTRY contents and structure."""

    def test_registry_is_non_empty(self):
        """Registry contains at least one entry."""
        from interloper_assets import IO_REGISTRY

        assert len(IO_REGISTRY) > 0

    def test_postgres_in_registry(self):
        """PostgreSQL is registered."""
        from interloper_assets import IO_REGISTRY

        assert "PostgreSQL" in IO_REGISTRY

    def test_mysql_in_registry(self):
        """MySQL is registered."""
        from interloper_assets import IO_REGISTRY

        assert "MySQL" in IO_REGISTRY

    def test_bigquery_in_registry(self):
        """BigQuery is registered."""
        from interloper_assets import IO_REGISTRY

        assert "BigQuery" in IO_REGISTRY

    def test_registry_values_are_io_classes(self):
        """Each registry value is an IO subclass."""
        from interloper_assets import IO_REGISTRY

        for key, io_cls in IO_REGISTRY.items():
            assert issubclass(io_cls, IO), f"Registry entry '{key}' is not an IO subclass"


class TestGetIO:
    """get_io lookup function."""

    def test_returns_postgres(self):
        """Looking up 'PostgreSQL' returns the correct IO class."""
        from interloper_sql import PostgresIO

        from interloper_assets import get_io

        io_cls = get_io("PostgreSQL")
        assert io_cls is PostgresIO

    def test_returns_mysql(self):
        """Looking up 'MySQL' returns the correct IO class."""
        from interloper_sql import MySQLIO

        from interloper_assets import get_io

        io_cls = get_io("MySQL")
        assert io_cls is MySQLIO

    def test_returns_bigquery(self):
        """Looking up 'BigQuery' returns the correct IO class."""
        from interloper_google_cloud import BigQueryIO

        from interloper_assets import get_io

        io_cls = get_io("BigQuery")
        assert io_cls is BigQueryIO

    def test_unknown_key_raises(self):
        """Looking up an unknown key raises ConfigError."""
        from interloper_assets import get_io

        with pytest.raises(il.ConfigError):
            get_io("nonexistent_io_xyz")


class TestGetAllIOs:
    """get_all_ios function."""

    def test_returns_dict(self):
        """Returns a dict."""
        from interloper_assets import get_all_ios

        result = get_all_ios()
        assert isinstance(result, dict)

    def test_contains_postgres(self):
        """Returned dict contains PostgreSQL."""
        from interloper_assets import get_all_ios

        result = get_all_ios()
        assert "PostgreSQL" in result

    def test_returns_copy(self):
        """Returned dict is a copy, not the original registry."""
        from interloper_assets import IO_REGISTRY, get_all_ios

        result = get_all_ios()
        assert result is not IO_REGISTRY
        assert result == IO_REGISTRY
