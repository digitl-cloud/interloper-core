"""Tests for dynamic import helpers and the require_import decorator."""

from unittest.mock import MagicMock

import pytest

from interloper.utils.imports import get_object_path, import_from_path, require_import


class TestImportFromPath:
    """Tests for import_from_path()."""

    def test_import_class(self):
        """Imports a class by dotted path."""
        cls = import_from_path("interloper.io.file.FileIO")
        assert cls.__name__ == "FileIO"

    def test_import_function(self):
        """Imports a function by dotted path."""
        func = import_from_path("interloper.assets.decorator.asset")
        assert callable(func)

    def test_type_validation_passes(self):
        """Type validation passes for matching types."""
        cls = import_from_path("interloper.io.file.FileIO", type)
        assert cls.__name__ == "FileIO"

    def test_type_validation_fails(self):
        """Type validation raises ValueError for mismatched types."""
        with pytest.raises(ValueError, match="is not a"):
            import_from_path("interloper.io.file.FileIO", int)

    def test_invalid_module(self):
        """Invalid module raises ImportError."""
        with pytest.raises((ImportError, ModuleNotFoundError)):
            import_from_path("nonexistent.module.Class")

    def test_invalid_attribute(self):
        """Missing attribute raises AttributeError."""
        with pytest.raises(AttributeError):
            import_from_path("interloper.io.file.NonExistentClass")


class TestGetObjectPath:
    """Tests for get_object_path()."""

    def test_class_path(self):
        """Returns correct dotted path for a class."""
        from interloper.io.file import FileIO

        assert get_object_path(FileIO) == "interloper.io.file.FileIO"

    def test_function_path(self):
        """Returns correct dotted path for a function."""
        from interloper.assets.decorator import asset

        assert get_object_path(asset) == "interloper.assets.decorator.asset"

    def test_mock_object(self):
        """Returns mock placeholder path for MagicMock."""
        mock = MagicMock()
        mock._mock_name = "my_mock"
        path = get_object_path(mock)
        assert path == "mock.my_mock"

    def test_mock_without_name(self):
        """Returns fallback path for unnamed mock."""
        mock = MagicMock()
        mock._mock_name = None
        path = get_object_path(mock)
        assert path == "mock.mock_object"


class TestRequireImport:
    """Tests for the require_import decorator."""

    def test_function_passes_when_package_exists(self):
        """Decorated function executes when package is available."""
        @require_import("json", "json is required")
        def my_func():
            return "ok"

        assert my_func() == "ok"

    def test_function_raises_when_package_missing(self):
        """Decorated function raises ImportError when package is missing."""
        @require_import("nonexistent_package_xyz", "Package required")
        def my_func():
            return "ok"

        with pytest.raises(ImportError, match="Package required"):
            my_func()

    def test_class_passes_when_package_exists(self):
        """Decorated class instantiates when package is available."""
        @require_import("json", "json is required")
        class MyClass:
            pass

        instance = MyClass()
        assert isinstance(instance, MyClass)

    def test_class_raises_when_package_missing(self):
        """Decorated class raises ImportError on instantiation when package is missing."""
        @require_import("nonexistent_package_xyz", "Package required")
        class MyClass:
            pass

        with pytest.raises(ImportError, match="Package required"):
            MyClass()
