"""Frozen context object passed to every IO read/write call."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from interloper.partitioning.base import Partition, PartitionWindow

if TYPE_CHECKING:
    from interloper.assets.base import Asset


@dataclass(frozen=True)
class IOContext:
    """Immutable context passed to :meth:`IO.read` and :meth:`IO.write`.

    Carries the target asset, optional partition scope, and arbitrary metadata
    (e.g. ``run_id``, ``backfill_id``) so that IO implementations can resolve
    the correct storage location without additional parameters.

    Attributes:
        asset: Asset being materialized.
        partition_or_window: Partition scope, or ``None`` for unpartitioned assets.
        metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).
    """

    asset: Asset
    partition_or_window: Partition | PartitionWindow | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

