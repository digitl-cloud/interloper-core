# Runners

Runners orchestrate the execution of assets in a DAG. They handle dependency ordering, concurrency, and error handling.

## Quick start

The simplest way to run a DAG is with `dag.materialize()`, which uses `MultiThreadRunner` internally:

```py
dag = il.DAG(source)
result = dag.materialize()
```

For more control, use a runner directly:

```py
with il.SerialRunner() as runner:
    result = runner.run(dag)
```

## SerialRunner

Executes assets one at a time in dependency order. Best for debugging and deterministic execution.

```py
with il.SerialRunner() as runner:
    result = runner.run(dag)
```

## MultiThreadRunner

Executes independent assets in parallel using threads.

```py
with il.MultiThreadRunner(max_workers=4) as runner:
    result = runner.run(dag)
```

Assets are scheduled dynamically: as soon as an asset's dependencies are met, it is submitted for execution.

## MultiProcessRunner

Executes assets in separate processes. Useful for CPU-bound workloads or to bypass the GIL.

```py
with il.MultiProcessRunner(max_workers=4) as runner:
    result = runner.run(dag)
```

!!! note

    Multi-process runners use serialization (specs) to transfer DAG information across process boundaries. All assets and IO backends must be serializable.

## Docker & Kubernetes runners

Available via extension packages:

```py
from interloper_docker import DockerRunner

runner = DockerRunner(image="my-app:latest")
result = runner.run(dag)
```

```py
from interloper_k8s import KubernetesRunner

runner = KubernetesRunner(image="my-app:latest", namespace="data")
result = runner.run(dag)
```

## Options

All runners support:

| Option | Default | Description |
|--------|---------|-------------|
| `fail_fast` | `True` | Stop on first failure vs. continue |
| `reraise` | `False` | Re-raise exceptions vs. capture in result |
| `on_event` | `None` | Callback for lifecycle events |

```py
with il.SerialRunner(fail_fast=False, reraise=True) as runner:
    result = runner.run(dag)
```

## RunResult

Every runner returns a `RunResult`:

```py
result = runner.run(dag)

result.status          # ExecutionStatus.COMPLETED or FAILED
result.elapsed         # Total execution time in seconds
result.asset_executions  # dict[AssetInstanceKey, AssetExecutionInfo]
```

Each `AssetExecutionInfo` contains:

```py
info.asset_key         # AssetInstanceKey
info.status            # ExecutionStatus (COMPLETED, FAILED, SKIPPED, etc.)
info.elapsed           # Asset execution time
info.error_message     # Error message if failed
```

## Partitioned runs

Pass a partition or window to the runner:

```py
with il.SerialRunner() as runner:
    result = runner.run(
        dag,
        partition_or_window=il.TimePartition(dt.date(2025, 1, 15)),
    )
```
