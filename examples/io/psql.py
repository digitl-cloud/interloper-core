import datetime as dt
from pprint import pp
from typing import Any

import interloper as il
from interloper_sql import PostgresIO

il.subscribe(print)

io = PostgresIO(
    host="localhost",
    database="interloper",
    user="postgres",
    password="postgres",
)

partitioning = il.TimePartitionConfig(column="date")


@il.asset(io=io, partitioning=partitioning)
def a(context: il.ExecutionContext) -> list[dict[str, Any]]:
    return [
        {"date": context.partition_date, "value": 1},
        {"date": context.partition_date, "value": 2},
    ]


@il.asset(io=io, partitioning=partitioning)
def b(context: il.ExecutionContext, a: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"date": context.partition_date, "value": a[0]["value"] + 2},
        {"date": context.partition_date, "value": a[1]["value"] + 2},
    ]


dag = il.DAG(a, b)
dag.backfill(il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 7)))

pp(b().partition_row_counts())
