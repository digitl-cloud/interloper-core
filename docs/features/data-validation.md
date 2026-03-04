# Data Validation

Interloper provides several layers of data validation.

## Schema validation

When a `schema` is defined on an asset and a `normalizer` is configured, the normalized data is validated against the schema:

```py
from pydantic import BaseModel
import interloper as il

class UserSchema(BaseModel):
    id: int
    name: str

@il.asset(schema=UserSchema, normalizer=il.Normalizer())
def users():
    return [{"id": 1, "name": "Alice"}]
```

If the data doesn't match the schema, a `SchemaError` is raised.

## Materialization strategies

The `MaterializationStrategy` controls **how strictly** schemas are enforced. See [Schema & Normalizer](schema.md) for details.

| Strategy | Schema required | Behavior |
|----------|----------------|----------|
| `AUTO` | No | Infers schema if missing, validates if present |
| `STRICT` | Yes | Fails on any mismatch |
| `RECONCILE` | Yes | Aligns columns and coerces types |


## Schema inference

With `Normalizer(infer=True)` and no explicit schema, a Pydantic model is automatically inferred from the data:

```py
@il.asset(normalizer=il.Normalizer(infer=True))
def users():
    return [{"id": 1, "name": "Alice"}]
    # Inferred schema: id: int, name: str
```

## Partition validation

The DAG validates partition dependencies at construction time:

- A non-partitioned asset **cannot** depend on a partitioned asset
- Circular dependencies are detected and raise a `CircularDependencyError`
- Missing dependencies raise a `DependencyNotFoundError`

```py
# This will raise a DAGError at construction time:
@il.source
class Invalid:
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def partitioned(self, context: il.ExecutionContext):
        return [{"date": str(context.partition_date)}]

    @il.asset  # non-partitioned depending on partitioned
    def summary(self, partitioned):
        return [{"count": len(partitioned)}]
```
