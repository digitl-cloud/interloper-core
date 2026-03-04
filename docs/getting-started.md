# Getting started

## Installation

```sh
pip install interloper
```

Or with [UV](https://docs.astral.sh/uv/):

```sh
uv add interloper
```

### Optional packages

```sh
pip install interloper-sql            # PostgreSQL, MySQL, SQLite IO
pip install interloper-google-cloud   # BigQuery IO
pip install interloper-assets         # Pre-built source definitions
```

## Quick example

### Define an asset

An asset is a function that produces data. Decorate it with `@asset`:

```py
import interloper as il

@il.asset
def greetings():
    return [
        {"name": "Alice", "message": "Hello"},
        {"name": "Bob", "message": "Hi"},
    ]
```

### Run it

```py
result = greetings().run()
print(result)
# [{'name': 'Alice', 'message': 'Hello'}, {'name': 'Bob', 'message': 'Hi'}]
```

### Materialize it

Add an IO backend and materialize -- this runs the asset **and** writes the result:

```py
greetings_asset = greetings(io=il.FileIO("./data"))
greetings_asset.materialize()
# Data is written to ./data/greetings/data.pkl
```

### Group assets in a source

```py
@il.source
class MySource:
    @il.asset
    def users(self):
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def orders(self):
        return [{"id": 1, "user_id": 1, "total": 99.90}]
```

### Build a DAG and materialize everything

```py
source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize()
```

## Next steps

- Follow the [Tutorial](tutorial.md) for a hands-on walkthrough
- Explore [Features](features/basic.md) for in-depth documentation
