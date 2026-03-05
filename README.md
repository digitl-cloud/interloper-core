<h1 align="center">Interloper</h1>
<h3 align="center">A lightweight Python framework for building data assets</h3>

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

## Packages

| Package             | Description                                           |
| ------------------- | ----------------------------------------------------- |
| `interloper`        | Core: assets, sources, DAG, runners, IO, partitioning |
| `interloper-sql`    | SQL IO backends (Postgres, MySQL, SQLite)             |
| `interloper-assets` | Pre-built source definitions and registry             |
| `interloper-argo`   | Argo Workflows integration                            |
| `interloper-dbos`   | DBOS durable execution                                |
| `interloper-docker` | Docker runner and backfiller                          |
| `interloper-k8s`    | Kubernetes runner and backfiller                      |

## Development

```bash
uv sync               # install dependencies
uv run pytest          # run tests
uv run ruff check .    # lint
uv run pyright         # type check
```
