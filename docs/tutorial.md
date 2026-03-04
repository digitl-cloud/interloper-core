# Tutorial

## Your first asset

At the most basic level, an `asset` is simply a function that returns data.
Any kind of data, whether it's a string, a list of dictionaries, a Pandas dataframe, etc.

Let's create our first asset that pulls today's Berlin weather forecast data from [Open Meteo](https://open-meteo.com/):

```py
import datetime as dt
import httpx
import interloper as il

@il.asset
def forecast():
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": dt.date.today().isoformat(),
        "end_date": dt.date.today().isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = httpx.get(url, params=params)
    data = response.json()["hourly"]
    return data
```

We can execute our asset by instantiating it and calling `run()`:

```py
result = forecast().run()
```

!!! note

    The `@il.asset` decorator returns an `AssetDefinition`. Calling it (e.g. `forecast()`) creates a runtime `Asset` instance. This separation between definition and instance is a core concept in Interloper.


## Adding context

Assets that need access to partition information or a logger can request an `ExecutionContext`:

```py
@il.asset
def forecast(context: il.ExecutionContext):
    context.logger.info("Fetching forecast data...")
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.5244,
        "longitude": 13.4105,
        "start_date": dt.date.today().isoformat(),
        "end_date": dt.date.today().isoformat(),
        "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
    }
    response = httpx.get(url, params=params)
    data = response.json()["hourly"]
    return data
```


## Materialization

Interloper starts to make sense when we introduce the concept of **materialization**.

An asset can be **materialized**, meaning that based on an `IO` configuration, its result is **written to** a destination.

Interloper ships with built-in IO backends and additional ones via extension packages.

Let's materialize our asset using `FileIO`, which pickles the data on the filesystem:

```py
forecast_asset = forecast(io=il.FileIO("./data"))
forecast_asset.materialize()
```

The materialization handles the execution of the asset and saves the data as a pickle file under `./data/forecast/data.pkl`.


## Defining a source

Pulling data from a data source isn't typically limited to a single asset. We might fetch data from several API endpoints while reusing common configuration or sharing an HTTP client.

Interloper allows you to define `sources`, which group related assets together. A source is defined as a **class** decorated with `@il.source`:

```py
@il.source
class OpenMeteo:
    def __init__(self):
        self.client = httpx.Client(
            params={"latitude": 52.5244, "longitude": 13.4105},
        )

    @il.asset
    def forecast(self):
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation", "wind_speed_10m"],
        }
        response = self.client.get(url, params=params)
        return response.json()["hourly"]

    @il.asset
    def air_quality(self):
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        params = {
            "start_date": dt.date.today().isoformat(),
            "end_date": dt.date.today().isoformat(),
            "hourly": ["pm10", "pm2_5", "dust", "uv_index"],
        }
        response = self.client.get(url, params=params)
        return response.json()["hourly"]
```

Instantiate the source and access individual assets:

```py
source = OpenMeteo(io=il.FileIO("./data"))
source.forecast.run()
source.air_quality.materialize()
```


## DAG

When you want to materialize multiple assets together, respecting their dependencies, use a `DAG`:

```py
source = OpenMeteo(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize()
```

`dag.materialize()` is syntactic sugar that uses a `MultiThreadRunner` internally. For more control, use a runner directly:

```py
with il.SerialRunner() as runner:
    result = runner.run(dag)

print(result.status)       # ExecutionStatus.COMPLETED
print(result.elapsed)      # Total time in seconds
```


## Configuration

Sources often need external configuration like API keys or account IDs. Interloper uses `Config`, which extends [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) for environment-based resolution:

```py
class OpenMeteoConfig(il.Config):
    latitude: float = 52.5244
    longitude: float = 13.4105

@il.source(config=OpenMeteoConfig)
class OpenMeteo:
    def __init__(self, config: OpenMeteoConfig):
        self.client = httpx.Client(
            params={"latitude": config.latitude, "longitude": config.longitude},
        )

    @il.asset
    def forecast(self):
        ...
```

The config is automatically resolved from environment variables. You can also provide it explicitly:

```py
source = OpenMeteo(config=OpenMeteoConfig(latitude=48.8566, longitude=2.3522))
```


## Partitioning

Many data pipelines need to process data by date. Interloper supports time-based partitioning:

```py
@il.source
class OpenMeteo:
    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def forecast(self, context: il.ExecutionContext):
        date = context.partition_date
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        params = {
            "start_date": date.isoformat(),
            "end_date": date.isoformat(),
            "hourly": ["temperature_2m"],
        }
        response = httpx.get(url, params=params)
        return response.json()["hourly"]
```

Materialize for a specific date:

```py
source = OpenMeteo(io=il.FileIO("./data"))
dag = il.DAG(source)
dag.materialize(partition_or_window=il.TimePartition(dt.date.today()))
```


## Backfilling

To process a range of dates, use a backfiller:

```py
backfiller = il.SerialBackfiller(runner=il.SerialRunner())
result = backfiller.backfill(
    dag=dag,
    partition_or_window=il.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 7),
    ),
)
```

This iterates over each date in the window and runs the DAG for each partition.


## Next steps

- [Assets & Sources](features/basic.md) -- Detailed definition patterns
- [IO](features/io.md) -- All available IO backends
- [Runners](features/runners.md) -- Execution strategies
- [Schema & Normalizer](features/schema.md) -- Data normalization and validation
- [Partitioning](features/partitioning.md) -- Time-based partitioning
- [Backfilling](features/backfilling.md) -- Historical data processing
- [Configuration](features/config.md) -- Environment-based config
- [Events](features/events.md) -- Lifecycle event system
- [CLI](features/cli.md) -- Command-line interface
