"""Dynamic import helpers and import-guard decorator."""

import importlib.util
from collections.abc import Callable
from typing import Any, TypeVar, cast, overload

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])
C = TypeVar("C", bound=type[Any])


@overload
def import_from_path(path: str) -> Any: ...
@overload
def import_from_path(path: str, target_type: type[T]) -> T: ...
def import_from_path(path: str, target_type: type[T] | None = None) -> Any:
    """Import an object from a dotted module path.

    Args:
        path: Dotted path like ``"module.submodule.ClassName"``.
        target_type: If provided, validates the imported object is an instance
            of this type.

    Returns:
        The imported object.

    Raises:
        ValueError: If *target_type* is given and the object does not match.

    Example:
        >>> import_from_path("interloper.io.file.FileIO")
        <class 'interloper.io.file.FileIO'>
    """
    parts = path.split(".")
    module_path = ".".join(parts[:-1])
    obj_name = parts[-1]

    module = importlib.import_module(module_path)
    obj = getattr(module, obj_name)
    if target_type is not None and not isinstance(obj, target_type):
        raise ValueError(f"Object {obj_name} is not a {target_type.__name__}")
    return obj


def get_object_path(obj: Any) -> str:
    """Return the dotted import path for a class or function.

    Args:
        obj: A class or function.

    Returns:
        Dotted path string like ``"module.submodule.ClassName"``.

    Example:
        >>> from interloper.io.file import FileIO
        >>> get_object_path(FileIO)
        'interloper.io.file.FileIO'
    """
    # Handle mock objects in tests
    if hasattr(obj, "_mock_name") and hasattr(obj, "_mock_parent"):
        # This is a MagicMock object, return a placeholder path
        return f"mock.{obj._mock_name or 'mock_object'}"

    return f"{obj.__module__}.{obj.__name__}"


def require_import(import_name: str, error_message: str) -> Callable[[F | C], F | C]:
    """Decorator that defers an ``ImportError`` until the decorated object is used.

    Works with both classes and functions.  For classes the check runs at
    instantiation; for functions it runs at call time.

    Args:
        import_name: Top-level package name to look for (e.g. ``"pandas"``).
        error_message: Message for the ``ImportError`` raised when the package
            is missing.

    Returns:
        A decorator that wraps the target class or function.
    """

    def decorator(obj: F | C) -> F | C:
        # Check if it's a class
        if isinstance(obj, type):
            # For classes, modify __new__ to check import on instantiation
            original_new = obj.__new__

            def checked_new(cls: type[Any], *args: Any, **kwargs: Any) -> Any:
                if importlib.util.find_spec(import_name) is None:
                    raise ImportError(error_message)
                if original_new is object.__new__:
                    return object.__new__(cls)
                return original_new(cls, *args, **kwargs)

            # Replace __new__ while preserving the class identity
            obj.__new__ = staticmethod(checked_new)
            return cast(C, obj)
        else:
            # For functions, wrap the function itself
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if importlib.util.find_spec(import_name) is None:
                    raise ImportError(error_message)
                return obj(*args, **kwargs)

            return cast(F, wrapper)

    return decorator
