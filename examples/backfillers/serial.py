"""Example script demonstrating serial backfiller."""

import datetime as dt
from time import sleep

import interloper as il

io = il.FileIO("data")
partitioning = il.TimePartitionConfig(column="date")
partition_window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3))


@il.asset(io=io, partitioning=partitioning)
def a(
    context: il.ExecutionContext,
) -> None:
    print("A", context.partition_date)
    sleep(0.5)


@il.asset(io=io, partitioning=partitioning)
def b(context: il.ExecutionContext, a: str) -> None:
    print("B", context.partition_date)
    sleep(0.5)


@il.asset(io=io, partitioning=partitioning)
def c(context: il.ExecutionContext, a: str) -> None:
    print("C", context.partition_date)
    sleep(0.5)


@il.asset(io=io, partitioning=partitioning)
def d(context: il.ExecutionContext, a: str) -> None:
    print("D", context.partition_date)
    sleep(0.5)


@il.asset(io=io, partitioning=partitioning)
def e(context: il.ExecutionContext, b: str, c: str, d: str) -> None:
    print("E", context.partition_date)
    sleep(0.5)


dag = il.DAG(a, b, c, d, e)

#    ↗ b ↘
#  a → c → e
#    ↘ d ↗


if __name__ == "__main__":

    def on_event(event: il.Event) -> None:
        print(event)

    il.subscribe(on_event)

    backfiller = il.SerialBackfiller()
    backfiller.backfill(dag, partition_window)
