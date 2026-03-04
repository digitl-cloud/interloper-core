# Schema & Normalizer

## Normalizer

The `Normalizer` transforms arbitrary asset return types into `list[dict]` and applies optional transformations.

### Supported input types

The normalizer accepts: `dict`, `list[dict]`, `BaseModel`, `list[BaseModel]`, `Generator`, and `None`.

### Usage

```py
import interloper as il

@il.asset(normalizer=il.Normalizer())
def my_asset():
    return [{"UserName": "alice", "Address": {"City": "NYC"}}]
```

After normalization, the data becomes:

```py
[{"user_name": "alice", "address_city": "NYC"}]
```

### Options

```py
il.Normalizer(
    normalize_columns=True,     # Convert column names to snake_case
    flatten_max_level=0,        # 0=disabled, None=unlimited, N=N levels deep
    flatten_separator="_",      # Separator for flattened keys
    fill_missing=True,          # Fill missing keys with None across rows
    infer=True,                 # Infer Pydantic schema from data
)
```

The normalizer can be set at the **source level** (applies to all assets) or at the **asset level** (overrides source-level):

```py
@il.source(normalizer=il.Normalizer())
class MySource:
    @il.asset  # inherits source normalizer
    def asset_a(self):
        ...

    @il.asset(normalizer=il.Normalizer(flatten_max_level=None))  # overrides
    def asset_b(self):
        ...
```


## Schema

Schemas are defined using Pydantic `BaseModel` classes and passed to the `@asset` decorator:

```py
from pydantic import BaseModel

class UserSchema(BaseModel):
    id: int
    name: str
    email: str | None = None

@il.asset(schema=UserSchema, normalizer=il.Normalizer())
def users():
    return [{"id": 1, "name": "Alice", "email": "alice@example.com"}]
```


## Materialization strategy

The `MaterializationStrategy` controls how schemas are enforced. Set it on the `@asset` or `@source` decorator:

### AUTO (default)

Infers a schema if none is provided. If a schema is set, validates data against it without coercion.

```py
@il.asset(normalizer=il.Normalizer(), strategy=il.MaterializationStrategy.AUTO)
def my_asset():
    ...
```

### STRICT

Requires a schema. Validates data and **fails** on any mismatch (extra fields, missing required fields, wrong types).

```py
@il.asset(
    schema=UserSchema,
    normalizer=il.Normalizer(),
    strategy=il.MaterializationStrategy.STRICT,
)
def my_asset():
    ...
```

### RECONCILE

Requires a schema. Aligns columns to the schema (drops extras, adds missing with `None`) and coerces values to the schema's types via Pydantic.

```py
@il.asset(
    schema=UserSchema,
    normalizer=il.Normalizer(),
    strategy=il.MaterializationStrategy.RECONCILE,
)
def my_asset():
    ...
```


## Schema inference

When `normalizer.infer=True` and no schema is provided, the normalizer automatically infers a Pydantic model from the data. This inferred schema is then attached to the asset for downstream use.

```py
@il.asset(normalizer=il.Normalizer(infer=True))
def my_asset():
    return [{"id": 1, "name": "Alice"}]
    # Schema is inferred as: class InferredSchema(BaseModel): id: int; name: str
```
