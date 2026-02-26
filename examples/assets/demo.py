import datetime as dt

import interloper as il
from interloper_assets.demo.source import DemoSource


def on_event(event: il.Event) -> None:
    print(event)


il.subscribe(on_event)

demo = DemoSource()
dag = il.DAG(demo)
backfiller = il.SerialBackfiller(runner=il.SerialRunner())
result = backfiller.backfill(
    dag=dag,
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 3),
    ),
    windowed=False,
)
