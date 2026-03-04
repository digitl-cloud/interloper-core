# FAQ

### Why another data framework?

Because the existing ones are either too opinionated, too bloated, or just don't get out of your way.

* ETL tools force rigid workflows.
* Orchestration frameworks overcomplicate simple jobs.
* DIY pipelines break the moment your schema changes.

Interloper sits in between:

* **Simple when you want it**: write a function, materialize an asset.
* **Powerful when you need it**: define dependencies, automatically reconcile schemas, partitioning and backfill strategies, etc.

Interloper essentially positions itself as an alternative to DLT, while being simpler and yet more powerful on several aspects. In terms of concepts and design, Interloper draws a lot of inspiration from Dagster.


### What is the difference between an AssetDefinition and an Asset?

An `AssetDefinition` is the immutable blueprint created by the `@asset` decorator. It describes *what* an asset does. An `Asset` is a runtime instance created by calling the definition -- it carries runtime configuration like IO, config, and partition info.

```py
@il.asset
def my_asset():       # This is an AssetDefinition
    return "hello"

instance = my_asset()  # This is an Asset
```


### What is the difference between `run()` and `materialize()`?

`run()` executes the asset function and returns the result without writing to IO. `materialize()` does the same but also writes the result to all configured IO backends.


### How does dependency resolution work?

Within a source, if an asset's parameter name matches a sibling asset's name, the dependency is resolved automatically. For cross-source dependencies or name mismatches, use `deps` or the `requires` parameter. See [Upstream Assets](features/upstream-assets.md).


### Can I use Interloper without a DAG?

Yes. You can run or materialize individual assets directly:

```py
result = my_asset().run()
my_asset(io=il.FileIO("./data")).materialize()
```

A DAG is only needed when you have multiple assets with dependencies.


### What IO backends are available?

Built-in: `MemoryIO` (default), `FileIO`. Via extension packages: `PostgresIO`, `MySQLIO`, `SqliteIO` (interloper-sql), `BigQueryIO` (interloper-google-cloud). You can also create custom IO backends by extending `il.IO`.


### How does configuration work?

`il.Config` extends Pydantic Settings. Fields are resolved from environment variables automatically. See [Configuration](features/config.md).
