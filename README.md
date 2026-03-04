<h1 align="center">Interloper</h1>
<h3 align="center">A Python framework for building and executing data pipelines</h3>

<p align="center">
Define assets as functions, group them in sources, wire dependencies automatically, and materialize them with pluggable IO backends and runners.
</p>

## Install

```bash
uv add interloper-core
```

## Quick Start

```python
import interloper as il

@il.source
class MySource:
    @il.asset
    def users(self) -> list:
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(self, users: list) -> int:
        return len(users)

source = MySource(io=il.FileIO("data"))
dag = il.DAG(source)
dag.materialize()
```

Dependencies between assets are resolved automatically by matching parameter names to sibling asset names.

## Core Concepts

### Assets

An asset is a function that produces data. Use the `@asset` decorator:

```python
@il.asset
def my_asset() -> list:
    return [{"key": "value"}]
```

Assets accept optional parameters:

```python
@il.asset(
    partitioning=il.TimePartitionConfig(column="date"),
    tags=["daily"],
    dataset="analytics",
)
def daily_events(context: il.ExecutionContext) -> list:
    date = context.partition_date
    return fetch_events(date)
```

### Sources

A source groups related assets into a class. Use the `@source` class decorator:

```python
@il.source
class Sales:
    @il.asset
    def orders(self) -> list:
        return fetch_orders()

    @il.asset
    def revenue(self, orders: list) -> float:
        return sum(o["amount"] for o in orders)
```

Instantiate with IO and optional config overrides:

```python
source = Sales(io=il.FileIO("data"))
# or with multiple IO targets:
source = Sales(io={"file": il.FileIO("data"), "db": PostgresIO(url="...")})
```

### DAG

The DAG resolves dependencies and orchestrates execution:

```python
dag = il.DAG(source)          # from a source
dag = il.DAG(a, b, c)         # from standalone assets
dag.materialize()              # run with default MultiThreadRunner
```

### IO

IO backends handle reading and writing asset data.

| Backend | Package | Description |
|---------|---------|-------------|
| `FileIO` | `interloper` | Pickle files on disk |
| `MemoryIO` | `interloper` | In-memory (default) |
| `PostgresIO` | `interloper-sql` | PostgreSQL |
| `MySQLIO` | `interloper-sql` | MySQL |
| `SqliteIO` | `interloper-sql` | SQLite |

### Partitioning

Time-based partitioning for incremental pipelines:

```python
import datetime as dt

partitioning = il.TimePartitionConfig(column="date")

@il.asset(partitioning=partitioning)
def daily_data(context: il.ExecutionContext) -> list:
    return fetch_data(context.partition_date)
```

Run for a specific partition:

```python
dag.materialize(il.TimePartition(dt.date(2025, 1, 15)))
```

### Runners

Control how assets execute within a single partition run:

```python
with il.SerialRunner() as runner:
    result = runner.run(dag)

with il.MultiThreadRunner(max_workers=4) as runner:
    result = runner.run(dag)

with il.MultiProcessRunner(max_workers=4) as runner:
    result = runner.run(dag)
```

### Backfillers

Orchestrate runs across multiple partitions:

```python
window = il.TimePartitionWindow(
    start=dt.date(2025, 1, 1),
    end=dt.date(2025, 1, 7),
)

backfiller = il.SerialBackfiller(runner=il.SerialRunner())
result = backfiller.backfill(dag, window)
```

### Config

Environment-based configuration using Pydantic:

```python
class MyConfig(il.Config):
    api_key: str
    base_url: str = "https://api.example.com"

@il.source(config=MyConfig)
class MySource:
    @il.asset
    def data(self, config: MyConfig) -> list:
        return fetch(config.base_url, config.api_key)
```

### Events

Subscribe to lifecycle events:

```python
def on_event(event: il.Event) -> None:
    print(event.type, event.metadata)

il.subscribe(on_event)
dag.materialize()
```

Event types: `ASSET_EXEC_STARTED`, `ASSET_EXEC_COMPLETED`, `ASSET_EXEC_FAILED`, `IO_WRITE_STARTED`, `IO_WRITE_COMPLETED`, `IO_WRITE_FAILED`, `IO_READ_STARTED`, `IO_READ_COMPLETED`, `IO_READ_FAILED`.

### REST Client

Built-in HTTP client with pagination and OAuth2 support:

```python
from interloper import RESTClient, OAuth2ClientCredentialsAuth

client = RESTClient(
    base_url="https://api.example.com",
    auth=OAuth2ClientCredentialsAuth(
        base_url="https://auth.example.com",
        client_id="...",
        client_secret="...",
    ),
)

for page in client.paginate("/resources"):
    process(page)
```

## Packages

| Package | Description |
|---------|-------------|
| `interloper` | Core: assets, sources, DAG, runners, IO, partitioning |
| `interloper-sql` | SQL IO backends (Postgres, MySQL, SQLite) |
| `interloper-assets` | Pre-built source definitions and registry |
| `interloper-argo` | Argo Workflows integration |
| `interloper-dbos` | DBOS durable execution |
| `interloper-docker` | Docker runner and backfiller |
| `interloper-k8s` | Kubernetes runner and backfiller |

## Development

```bash
uv sync               # install dependencies
uv run pytest          # run tests
uv run ruff check .    # lint
uv run pyright         # type check
```
