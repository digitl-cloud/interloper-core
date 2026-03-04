# CLI

Interloper includes a command-line interface for running and backfilling DAGs.

## Usage

```sh
interloper run <script> [options]
interloper backfill <script> [options]
```

## Running a DAG

Execute a DAG defined in a Python script:

```sh
interloper run my_pipeline.py
```

The script should define a DAG at module level:

```py
# my_pipeline.py
import interloper as il

@il.source
class MySource:
    @il.asset
    def data(self):
        return [{"value": 42}]

source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)
```

### With a partition

```sh
interloper run my_pipeline.py --date 2025-01-15
```

### With a date window

```sh
interloper run my_pipeline.py --window 2025-01-01 2025-01-07
```

## Backfilling

```sh
interloper backfill my_pipeline.py --window 2025-01-01 2025-01-31
```

## Configuration

Pass configuration via YAML, JSON, or inline JSON:

```sh
interloper run my_pipeline.py --config config.yaml
interloper run my_pipeline.py --config config.json
interloper run my_pipeline.py --config '{"api_key": "abc123"}'
```

## Options

| Option | Description |
|--------|-------------|
| `--date DATE` | Single partition date |
| `--window START END` | Date range for partitioned runs |
| `--config PATH_OR_JSON` | Configuration file or inline JSON |
| `--serial` | Use SerialRunner instead of MultiThreadRunner |
