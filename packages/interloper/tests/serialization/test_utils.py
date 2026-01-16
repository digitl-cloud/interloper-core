"""Test serialization utility functions."""

import pytest

from interloper.utils.imports import get_object_path, import_from_path


class TestSerializationUtils:
    """Test serialization utility functions."""

    def test_import_from_path(self):
        """Test importing objects from dotted paths."""
        # Test importing a class
        file_io_class = import_from_path("interloper.io.file.FileIO")
        assert file_io_class.__name__ == "FileIO"

        # Test importing a function
        asset_decorator = import_from_path("interloper.assets.decorator.asset")
        assert callable(asset_decorator)

    def test_get_object_path(self):
        """Test getting import paths for objects."""
        from interloper.io.file import FileIO

        path = get_object_path(FileIO)
        assert path == "interloper.io.file.FileIO"

        # Test with a function
        from interloper.assets import asset

        path = get_object_path(asset)
        assert path == "interloper.assets.decorator.asset"

    def test_import_from_path_invalid(self):
        """Test importing from invalid paths."""
        with pytest.raises(ImportError):
            import_from_path("nonexistent.module.Class")

        with pytest.raises(AttributeError):
            import_from_path("interloper.io.file.NonExistentClass")

