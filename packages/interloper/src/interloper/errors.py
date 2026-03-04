"""Custom exception hierarchy for the Interloper framework.

All exceptions inherit from :class:`InterloperError`, allowing users to catch
any framework error with a single ``except InterloperError`` clause, or target
specific domains (``DAGError``, ``ConfigError``, etc.) for finer control.

Each domain exception also inherits from the built-in exception it replaces
(e.g., ``DAGError(InterloperError, ValueError)``), preserving backward
compatibility with existing ``except ValueError:`` handlers.
"""

from __future__ import annotations


class InterloperError(Exception):
    """Base exception for all Interloper framework errors."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ConfigError(InterloperError, ValueError):
    """A configuration value is missing, has the wrong type, or cannot be resolved."""


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


class DAGError(InterloperError, ValueError):
    """An error in DAG construction or validation."""


class CircularDependencyError(DAGError):
    """A circular dependency was detected in the DAG."""


class DependencyNotFoundError(DAGError):
    """A referenced dependency is not present in the DAG."""


class AssetNotFoundError(DAGError, KeyError):
    """An asset key was not found in the DAG."""


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


class AssetError(InterloperError, ValueError):
    """An error in asset definition, configuration, or execution setup."""


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------


class SourceError(InterloperError, ValueError):
    """An error in source definition or instantiation."""


# ---------------------------------------------------------------------------
# Partitioning
# ---------------------------------------------------------------------------


class PartitionError(InterloperError, ValueError):
    """An error related to partitioning configuration or constraints."""


# ---------------------------------------------------------------------------
# Schema / Normalizer
# ---------------------------------------------------------------------------


class SchemaError(InterloperError, ValueError):
    """An error in schema validation, reconciliation, or inference."""


class NormalizerError(InterloperError, TypeError):
    """The normalizer received data it cannot coerce to ``list[dict]``."""


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------


class InterloperIOError(InterloperError):
    """Base class for IO-related errors.

    Named ``InterloperIOError`` to avoid shadowing Python's built-in ``IOError``.
    """


class DataNotFoundError(InterloperIOError, KeyError):
    """No data was found in the IO backend for the requested key."""


class TableNotFoundError(InterloperIOError, ValueError):
    """A database table does not exist."""


class AdapterError(InterloperIOError, TypeError):
    """A data adapter received data of an unexpected type."""


# ---------------------------------------------------------------------------
# Runner / Execution
# ---------------------------------------------------------------------------


class RunnerError(InterloperError, RuntimeError):
    """An error during runner lifecycle or asset execution."""


# ---------------------------------------------------------------------------
# Backfiller
# ---------------------------------------------------------------------------


class BackfillError(InterloperError, RuntimeError):
    """An error during backfill lifecycle or execution."""


# ---------------------------------------------------------------------------
# REST / Authentication
# ---------------------------------------------------------------------------


class AuthenticationError(InterloperError, ValueError):
    """An authentication or token error in the REST client."""


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


class EventError(InterloperError, ValueError):
    """An error parsing or validating an event."""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class ScriptLoadError(InterloperError, ValueError):
    """An error loading a user script via the CLI."""
