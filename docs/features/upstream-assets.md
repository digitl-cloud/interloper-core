# Upstream Assets

Interloper supports asset dependencies -- an asset can consume data produced by another asset.

## Automatic dependency resolution

Within a source, dependencies are resolved **automatically** by matching parameter names to sibling asset names:

```py
@il.source
class MySource:
    @il.asset
    def users(self):
        return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    @il.asset
    def user_count(self, users):  # 'users' matches the asset above
        return [{"count": len(users)}]
```

When the DAG executes, `users` runs first, its result is written to IO, then read back and passed as the `users` parameter to `user_count`.

```py
source = MySource(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize()
```

## Explicit dependency mapping

When a parameter name doesn't match the upstream asset name, use `requires` in the `@asset` decorator to declare the mapping:

```py
from interloper.assets.keys import AssetDefinitionKey

@il.source
class MySource:
    @il.asset
    def raw_data(self):
        return [{"value": 42}]

    @il.asset(requires={"data": AssetDefinitionKey("MySource:raw_data")})
    def processed(self, data):  # 'data' doesn't match 'raw_data'
        return [{"result": data[0]["value"] * 2}]
```

## Runtime dependency overrides

You can also override dependency mappings at instantiation time using `deps`:

```py
asset_instance = my_asset(deps={"param_name": "source_name:asset_name"})
```

## Cross-source dependencies

Assets from different sources can depend on each other as long as they are in the same DAG:

```py
@il.source
class SourceA:
    @il.asset
    def data(self):
        return [{"id": 1}]

@il.source
class SourceB:
    @il.asset
    def report(self, data):  # Won't auto-resolve across sources
        return [{"count": len(data)}]

source_a = SourceA(io=il.FileIO("./data"))
source_b = SourceB(io=il.FileIO("./data"))

# Use deps to wire cross-source dependencies
report_asset = source_b.report
report_asset.deps = {"data": "SourceA:data"}

dag = il.DAG(source_a, source_b)
dag.materialize()
```

## Dependency resolution order

The DAG resolves dependencies in this order:

1. **Explicit mapping** via `asset.deps`
2. **Same-source implicit** -- parameter name matches a sibling asset name
3. **Renamed asset alias** -- parameter name matches the original name of a renamed asset
4. **Standalone implicit** -- parameter name matches a standalone asset name
