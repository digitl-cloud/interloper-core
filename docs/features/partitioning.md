# Partitioning

Partitioning allows assets to process data in slices, typically by date. This enables incremental processing and efficient backfilling.

## Time partitioning

Add `partitioning` to an asset and access the partition date via `context.partition_date`:

```py
import datetime as dt
import interloper as il

@il.source
class MySource:
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily_data(self, context: il.ExecutionContext):
        date = context.partition_date
        return [{"date": date.isoformat(), "value": 42}]
```

Run for a specific date:

```py
source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize(partition_or_window=il.TimePartition(dt.date(2025, 1, 15)))
```

Data is stored in partition-aware paths:

```
./data/{dataset}/{asset_name}/date=2025-01-15/data.pkl
```


## Windowed partitioning

Some assets need to process a range of dates in a single execution. Enable this with `allow_window=True`:

```py
@il.source
class MySource:
    @il.asset(
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def weekly_summary(self, context: il.ExecutionContext):
        start, end = context.partition_date_window
        return [{"start": start.isoformat(), "end": end.isoformat(), "value": 100}]
```

Run with a window:

```py
dag.materialize(
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 7),
    ),
)
```

!!! note

    `context.partition_date` is only available for single-partition runs.
    `context.partition_date_window` is only available when `allow_window=True`.


## TimePartitionConfig

```py
il.TimePartitionConfig(
    column="date",          # Column name for the partition value
    allow_window=False,     # Whether the asset supports windowed runs
)
```

## Partition types

| Type | Description |
|------|-------------|
| `TimePartition(date)` | A single date partition |
| `TimePartitionWindow(start, end)` | A date range (inclusive) |

A `TimePartitionWindow` can be iterated to yield individual `TimePartition` values:

```py
window = il.TimePartitionWindow(
    start=dt.date(2025, 1, 1),
    end=dt.date(2025, 1, 3),
)
for partition in window:
    print(partition.value)
    # 2025-01-01
    # 2025-01-02
    # 2025-01-03
```
