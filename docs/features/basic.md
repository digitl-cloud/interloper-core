# Assets & Sources

## Standalone assets

The simplest way to define an asset is with the `@asset` decorator on a function:

```py
import interloper as il

@il.asset
def my_asset():
    return [{"key": "value"}]
```

The decorator returns an `AssetDefinition` -- an immutable blueprint. To create a runtime `Asset` instance, call it:

```py
asset_instance = my_asset()
result = asset_instance.run()
```

### Decorator options

The `@asset` decorator accepts several options:

```py
@il.asset(
    name="custom_name",             # Override the function name
    schema=MyPydanticModel,         # Pydantic model for validation
    partitioning=il.TimePartitionConfig(column="date"),
    normalizer=il.Normalizer(),     # Data normalization
    strategy=il.MaterializationStrategy.RECONCILE,
    dataset="my_dataset",           # Logical grouping
    tags=["daily", "api"],          # Arbitrary tags
    metadata={"owner": "team-a"},   # Arbitrary metadata
)
def my_asset():
    ...
```

### Running an asset

`run()` executes the asset function and returns the result **without** writing to IO:

```py
result = my_asset().run()
```

### Materializing an asset

`materialize()` executes the asset **and** writes the result to all configured IO backends:

```py
asset_instance = my_asset(io=il.FileIO("./data"))
asset_instance.materialize()
```


## Sources

A source groups related assets together. It is defined as a **class** decorated with `@il.source`:

```py
@il.source
class MySource:
    @il.asset
    def asset_a(self):
        return [{"key": "A"}]

    @il.asset
    def asset_b(self):
        return [{"key": "B"}]
```

### Source `__init__`

The source class can have an `__init__` to set up shared state (e.g. HTTP clients):

```py
@il.source
class MySource:
    def __init__(self):
        self.client = httpx.Client(base_url="https://api.example.com")

    @il.asset
    def users(self):
        return self.client.get("/users").json()

    @il.asset
    def orders(self):
        return self.client.get("/orders").json()
```

### Source decorator options

```py
@il.source(
    name="custom_name",
    config=MyConfig,                # Config class (extends il.Config)
    dataset="my_dataset",
    tags=["production"],
    normalizer=il.Normalizer(),     # Applied to all assets without their own
    strategy=il.MaterializationStrategy.AUTO,
)
class MySource:
    ...
```

### Instantiating a source

Call the `SourceDefinition` to create a runtime `Source`:

```py
source = MySource()                                 # Default config from env
source = MySource(io=il.FileIO("./data"))           # With IO
source = MySource(config=MyConfig(api_key="..."))   # With explicit config
source = MySource(name="renamed")                   # Override the name
source = MySource(assets=["users"])                  # Only include specific assets
source = MySource(assets={"users": "all_users"})    # Rename assets
```

### Accessing assets

Assets are available as attributes on both the definition and instance:

```py
# On the SourceDefinition (returns AssetDefinition)
MySource.users

# On the Source instance (returns Asset)
source = MySource(io=il.FileIO("./data"))
source.users.run()
source.orders.materialize()
```


## DAG

A `DAG` (Directed Acyclic Graph) orchestrates the execution of multiple assets, respecting their dependencies:

```py
source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize()
```

A DAG can be built from any combination of assets and sources:

```py
dag = il.DAG(source_a, source_b, standalone_asset)
```

`dag.materialize()` is syntactic sugar for `MultiThreadRunner`. For more control, see [Runners](runners.md).
