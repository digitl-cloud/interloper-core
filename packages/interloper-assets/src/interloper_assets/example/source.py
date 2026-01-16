import logging
from collections.abc import Sequence

import interloper as il
import pandas as pd
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class ExampleSchema(BaseModel):
    hello: str


class ExampleConfig(BaseSettings):
    hello: str = "world"

    model_config = SettingsConfigDict(env_prefix="example_")


partitioning = il.TimePartitionConfig(column="date", allow_window=True)


@il.source(
    config=ExampleConfig,
)
def example_source(config: ExampleConfig) -> Sequence[il.AssetDefinition]:
    def do() -> None:
        import random
        import time

        time.sleep(random.uniform(1, 3))

    @il.asset(
        schema=ExampleSchema,
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def a(
        context: il.ExecutionContext,
    ) -> pd.DataFrame:
        print(f"Hello from A {config.hello}")
        do()
        return pd.DataFrame([{"hello": config.hello}])

    @il.asset(
        schema=ExampleSchema,
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def b(
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        print(f"Hello from B {config.hello}")
        do()
        return pd.DataFrame([{"hello": config.hello}])

    @il.asset(
        schema=ExampleSchema,
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def c(
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        print(f"Hello from C {config.hello}")
        do()
        return pd.DataFrame([{"hello": config.hello}])

    @il.asset(
        schema=ExampleSchema,
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def d(
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        print(f"Hello from D {config.hello}")
        do()
        return pd.DataFrame([{"hello": config.hello}])

    @il.asset(
        schema=ExampleSchema,
        partitioning=il.TimePartitionConfig(column="date", allow_window=True),
    )
    def e(
        context: il.ExecutionContext,
        b: str,
        c: str,
        d: str,
    ) -> pd.DataFrame:
        print(f"Hello from E {config.hello}")
        do()
        return pd.DataFrame([{"hello": config.hello}])

    return (a, b, c, d, e)


#    ↗ b ↘
#  a → c → e
#    ↘ d ↗
