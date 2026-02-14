"""Tests for backfillers."""

from __future__ import annotations

import interloper as il


def _build_simple_dag(tmp_path):
    io = il.FileIO(str(tmp_path / "data"))

    @il.asset
    def a() -> str:
        return "a"

    @il.asset
    def b(a: str) -> str:
        return f"b({a})"

    @il.asset
    def c(a: str) -> str:
        return f"c({a})"

    @il.asset
    def d(b: str, c: str) -> str:
        return f"d({b},{c})"

    return il.DAG(a(io=io), b(io=io), c(io=io), d(io=io))


# class TestInProcessBackfiller:
#     def test_materialize_serial(self, tmp_path):
#         dag = _build_simple_dag(tmp_path)
#         backfiller = il.InProcessBackfiller(runner=il.SerialRunner())
#         result = backfiller.backfill(dag=dag)
#         assert result.status == il.ExecutionStatus.COMPLETED

#     def test_materialize_threaded(self, tmp_path):
#         dag = _build_simple_dag(tmp_path)
#         backfiller = il.InProcessBackfiller(runner=il.MultiThreadRunner(max_workers=2))
#         result = backfiller.backfill(dag=dag)
#         assert result.status == il.ExecutionStatus.COMPLETED

#     def test_backfill_delegation(self, tmp_path):
#         partitioned = il.TimePartitionConfig(column="date")
#         io = il.FileIO(str(tmp_path / "data"))

#         @il.asset(io=io, partitioning=partitioned)
#         def a() -> str:
#             return "a"

#         dag = il.DAG(a)
#         backfiller = il.InProcessBackfiller(runner=il.SerialRunner())
#         result = backfiller.backfill(dag=dag, start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3))
#         assert len(result.partition_results) == 3
