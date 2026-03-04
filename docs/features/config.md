# Configuration

## Config class

`il.Config` extends [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/), providing environment variable resolution, type validation, and default values.

```py
import interloper as il

class MyConfig(il.Config):
    api_key: str
    base_url: str = "https://api.example.com"
    timeout: int = 30
```

## Environment variable resolution

Config fields are automatically resolved from environment variables:

```sh
export API_KEY="my-secret-key"
export BASE_URL="https://custom.api.com"
```

```py
config = MyConfig()  # Resolves from environment
print(config.api_key)   # "my-secret-key"
print(config.base_url)  # "https://custom.api.com"
```

## Using with sources

Attach a config class to a source with the `config` parameter:

```py
@il.source(config=MyConfig)
class MySource:
    def __init__(self, config: MyConfig):
        self.client = httpx.Client(
            base_url=config.base_url,
            headers={"Authorization": f"Bearer {config.api_key}"},
            timeout=config.timeout,
        )

    @il.asset
    def data(self):
        return self.client.get("/data").json()
```

When the source is instantiated without an explicit config, it is resolved from the environment:

```py
# Auto-resolves config from environment variables
source = MySource(io=il.FileIO("./data"))

# Or provide config explicitly
source = MySource(
    config=MyConfig(api_key="explicit-key"),
    io=il.FileIO("./data"),
)
```

## Using with assets

Individual assets can also have their own config:

```py
class AssetConfig(il.Config):
    limit: int = 100

@il.asset(config=AssetConfig)
def my_asset(config: AssetConfig):
    return fetch_data(limit=config.limit)
```

## Config in the DAG

When a `SourceDefinition` or `AssetDefinition` is passed directly to a `DAG`, the config is resolved automatically from the environment:

```py
# These are equivalent:
dag = il.DAG(MySource)                    # Config resolved from env
dag = il.DAG(MySource(config=MyConfig())) # Config provided explicitly
```
