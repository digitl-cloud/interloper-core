# Backfilling

Backfilling allows you to process a range of partitions, either one at a time or as a single window.

## Multi-run backfill

By default, the backfiller iterates over each partition in the window and runs the DAG once per partition:

```py
import datetime as dt
import interloper as il

@il.source
class MySource:
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def daily_data(self, context: il.ExecutionContext):
        date = context.partition_date
        return [{"date": date.isoformat(), "value": 42}]

source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)

backfiller = il.SerialBackfiller(runner=il.SerialRunner())
result = backfiller.backfill(
    dag=dag,
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 7),
    ),
)

print(result.status)  # BackfillResult with per-partition results
```

This produces 7 separate runs, one for each day.


## Windowed backfill

When assets support `allow_window=True`, the entire window is passed as a single run:

```py
@il.source
class MySource:
    @il.asset(
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def weekly_data(self, context: il.ExecutionContext):
        start, end = context.partition_date_window
        return [{"start": start.isoformat(), "end": end.isoformat()}]
```


## Fail-fast

By default, the backfiller continues on failure. To stop on the first failed partition:

```py
backfiller = il.SerialBackfiller(
    runner=il.SerialRunner(fail_fast=True),
)
```


## Event monitoring

Subscribe to events during backfill for progress tracking:

```py
def on_event(event: il.Event):
    if event.type == il.EventType.RUN_COMPLETED:
        partition = event.metadata.get("partition_or_window")
        print(f"Completed partition: {partition}")

il.subscribe(on_event)
backfiller.backfill(dag=dag, partition_or_window=window)
il.unsubscribe(on_event)
```


## Distributed backfilling

For large backfills, use Docker or Kubernetes runners that execute each partition in an isolated environment:

```py
from interloper_docker import DockerRunner, DockerBackfiller

runner = DockerRunner(image="my-app:latest")
backfiller = DockerBackfiller(runner=runner)
backfiller.backfill(dag=dag, partition_or_window=window)
```

```py
from interloper_k8s import KubernetesRunner, KubernetesBackfiller

runner = KubernetesRunner(image="my-app:latest", namespace="data")
backfiller = KubernetesBackfiller(runner=runner)
backfiller.backfill(dag=dag, partition_or_window=window)
```
