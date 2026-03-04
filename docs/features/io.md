# IO

IO backends control **where** and **how** asset data is stored. IO is completely separate from how data is produced, allowing you to swap destinations without changing asset logic.

## Configuring IO

### On a source

```py
source = MySource(io=il.FileIO("./data"))
```

### On an individual asset

```py
asset_instance = my_asset(io=il.FileIO("./data"))
```

### Multiple IO backends

Write to multiple destinations simultaneously:

```py
from interloper_sql import PostgresIO, SqliteIO

source = MySource(
    io={
        "postgres": PostgresIO(host="localhost", database="mydb", user="user", password="pass"),
        "sqlite": SqliteIO(database="data/local.db"),
    },
)

dag = il.DAG(source)
dag.materialize()
```

When using multiple IOs with upstream dependencies, set `default_io_key` to specify which backend to read from:

```py
asset_instance = my_asset(
    io={
        "postgres": PostgresIO(...),
        "file": il.FileIO("./data"),
    },
    default_io_key="postgres",
)
```


## Built-in IO backends

### MemoryIO

In-memory storage using Python dicts. This is the **default** IO when none is specified. Useful for testing and development.

```py
asset_instance = my_asset(io=il.MemoryIO())
asset_instance.materialize()
```

Data is stored in a class-level dictionary keyed by `{dataset}/{asset_name}`. Clear all stored data with:

```py
il.MemoryIO.clear()
```

### FileIO

Pickle-based storage on the local filesystem.

```py
asset_instance = my_asset(io=il.FileIO("./data"))
asset_instance.materialize()
# Writes to ./data/{dataset}/{asset_name}/data.pkl
```

With partitioning, files are organized by partition value:

```
./data/{dataset}/{asset_name}/{column}={partition_id}/data.pkl
```


## Extension packages

### interloper-sql

SQL-based IO backends using SQLAlchemy. Install with:

```sh
pip install interloper-sql
```

**PostgresIO**

```py
from interloper_sql import PostgresIO

io = PostgresIO(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="pass",
)
```

**MySQLIO**

```py
from interloper_sql import MySQLIO

io = MySQLIO(
    host="localhost",
    port=3306,
    database="mydb",
    user="user",
    password="pass",
)
```

**SqliteIO**

```py
from interloper_sql import SqliteIO

io = SqliteIO(database="data/local.db")
# or in-memory:
io = SqliteIO(database=":memory:")
```

All SQL IO backends support `write_disposition` and `chunk_size` options.

### interloper-google-cloud

BigQuery IO backend. Install with:

```sh
pip install interloper-google-cloud
```

```py
from interloper_google_cloud import BigQueryIO

io = BigQueryIO(
    project="my-gcp-project",
    default_dataset="my_dataset",
    location="EU",
)
```


## Custom IO

Create a custom IO by extending the `IO` base class:

```py
import interloper as il

class MyCustomIO(il.IO):
    def write(self, context: il.IOContext, data):
        # Write data to your destination
        ...

    def read(self, context: il.IOContext):
        # Read data from your destination
        ...

    def to_spec(self):
        # Return a serializable spec (needed for multi-process runners)
        ...
```

The `IOContext` provides:

- `context.asset` -- The asset being materialized
- `context.partition_or_window` -- Current partition or window (if partitioned)
- `context.metadata` -- Run metadata
