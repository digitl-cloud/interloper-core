"""Pytest configuration and fixtures."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import interloper as il

from . import assets as test_assets


@pytest.fixture
def dag(tmp_path):
    """A DAG with multiple parallelizable levels.

    Topology levels (parallel groups):
    - L0: a, b
    - L1: c(a), d(a)
    - L2: e(b, c), f(d)
    - L3: g(e, f)

    DAG Structure:
      ↗ d → f ↘
    a → c ↘     g
            e ↗
        b ↗

    Returns:
        il.DAG: A DAG with multiple parallelizable levels.
    """
    io = il.FileIO(tmp_path)

    @il.asset
    def a(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 1}]

    @il.asset
    def b(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 2}]

    @il.asset
    def c(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
        return [{"v": a[0]["v"] + 1}]

    @il.asset
    def d(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
        return [{"v": a[0]["v"] * 3}]

    @il.asset
    def e(context: il.ExecutionContext, b: list[dict], c: list[dict]) -> list[dict]:
        return [{"v": int(b[0]["v"] + c[0]["v"])}]

    @il.asset
    def f(context: il.ExecutionContext, d: list[dict]) -> list[dict]:
        return [{"v": d[0]["v"] - 1}]

    @il.asset
    def g(context: il.ExecutionContext, e: list[dict], f: list[dict]) -> list[dict]:
        return [{"v": int(e[0]["v"] + f[0]["v"])}]

    # Build DAG and replace asset functions with mocks
    assets = [a(io=io), b(io=io), c(io=io), d(io=io), e(io=io), f(io=io), g(io=io)]
    dag = il.DAG(*assets)

    # Replace each asset's func with a MagicMock that preserves the original signature
    import inspect

    for asset in dag.assets:
        original_func = asset.func
        mock_func = MagicMock(side_effect=original_func)
        # Preserve the original function's signature
        mock_func.__signature__ = inspect.signature(original_func)
        asset.func = mock_func

    return dag


@pytest.fixture
def dag_partitioned(tmp_path):
    """A partitioned version of the complex DAG (daily time partitions).

    Partitioning applied to all assets to exercise partition propagation.

    Returns:
        il.DAG: A partitioned DAG with daily time partitions.
    """
    io = il.FileIO(tmp_path)
    part = il.TimePartitionConfig(column="date")

    @il.asset(partitioning=part)
    def a(context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "v": 1}]

    @il.asset(partitioning=part)
    def b(context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "v": 2}]

    @il.asset(partitioning=part)
    def c(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "v": int(a[0]["v"] + 1)}]

    @il.asset(partitioning=part)
    def d(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "v": int(a[0]["v"] * 3)}]

    @il.asset(partitioning=part)
    def e(context: il.ExecutionContext, b: list[dict], c: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "v": int(b[0]["v"] + c[0]["v"])}]

    @il.asset(partitioning=part)
    def f(context: il.ExecutionContext, d: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "v": int(d[0]["v"] - 1)}]

    @il.asset(partitioning=part)
    def g(context: il.ExecutionContext, e: list[dict], f: list[dict]) -> list[dict]:
        return [{"date": context.partition_date, "v": int(e[0]["v"] + f[0]["v"])}]

    # Build DAG and replace asset functions with mocks
    assets = [a(io=io), b(io=io), c(io=io), d(io=io), e(io=io), f(io=io), g(io=io)]
    dag = il.DAG(*assets)

    # Replace each asset's func with a MagicMock that preserves the original signature
    import inspect

    for asset in dag.assets:
        original_func = asset.func
        mock_func = MagicMock(side_effect=original_func)
        # Preserve the original function's signature
        mock_func.__signature__ = inspect.signature(original_func)
        asset.func = mock_func

    return dag


@pytest.fixture
def dag_mixed(tmp_path):
    """A mixed DAG with non-partitioned and partitioned assets.

    Valid edges (non-partitioned -> partitioned), no partitioned -> non-partitioned.
    Topology:
    - L0: a (non-partitioned), b (non-partitioned)
    - L1: c (partitioned, depends on a)
    - L2: e (partitioned, depends on b and c)

    Returns:
        il.DAG: A mixed DAG with non-partitioned and partitioned assets.
    """
    io = il.FileIO(tmp_path)
    part = il.TimePartitionConfig(column="date")

    @il.asset
    def a(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 1}]

    @il.asset
    def b(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 2}]

    @il.asset(partitioning=part)
    def c(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
        # partitioned, depends on non-partitioned a
        return [{"date": context.partition_date, "v": int(a[0]["v"] + 1)}]

    @il.asset(partitioning=part)
    def e(context: il.ExecutionContext, b: list[dict], c: list[dict]) -> list[dict]:
        # partitioned, depends on non-partitioned b and partitioned c
        return [{"date": context.partition_date, "v": int(b[0]["v"] + c[0]["v"])}]

    assets = [a(io=io), b(io=io), c(io=io), e(io=io)]
    dag = il.DAG(*assets)

    # Replace each asset's func with a MagicMock that preserves the original signature
    import inspect

    for asset in dag.assets:
        original_func = asset.func
        mock_func = MagicMock(side_effect=original_func)
        mock_func.__signature__ = inspect.signature(original_func)
        asset.func = mock_func

    return dag


@pytest.fixture
def double_source_dag(tmp_path):
    """A DAG with two sources.

    Returns:
        il.DAG: A DAG with two sources.
    """
    io = il.FileIO(tmp_path)
    part = il.TimePartitionConfig(column="date")

    @il.source
    class Source1:
        @il.asset(partitioning=part)
        def a(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "v": 1}]

        @il.asset(partitioning=part)
        def b(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "v": 2}]

    @il.source
    class Source2:
        @il.asset(partitioning=part)
        def a(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "v": 1}]

        @il.asset(partitioning=part)
        def b(self, context: il.ExecutionContext) -> list[dict]:
            return [{"date": context.partition_date, "v": 2}]

    # Build DAG and replace asset functions with mocks
    dag = il.DAG(Source1(io=io), Source2(io=io))

    # Replace each asset's func with a MagicMock that preserves the original signature
    import inspect

    for asset in dag.assets:
        original_func = asset.func
        mock_func = MagicMock(side_effect=original_func)
        # Preserve the original function's signature
        mock_func.__signature__ = inspect.signature(original_func)
        asset.func = mock_func

    return dag


@pytest.fixture
def file_based_dag(tmp_path):
    """A DAG with real, importable functions that write to files.

    This avoids issues with pickling mocks in multiprocessing tests.

    Returns:
        il.DAG: A DAG with real, importable functions that write to files.
    """
    io = il.FileIO(tmp_path)

    # Use the module-level assets but override their IO to use tmp_path
    asset_a = test_assets.asset_a()(io=io)  # type: ignore[attr-defined]
    asset_b = test_assets.asset_b()(io=io)  # type: ignore[attr-defined]
    asset_c = test_assets.asset_c(io=io, deps={"a": "asset_a"})  # type: ignore[attr-defined]

    return il.DAG(asset_a, asset_b, asset_c)
