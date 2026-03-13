import datetime as dt
import logging

import interloper as il
import pandas as pd
from pydantic_settings import SettingsConfigDict

logger = logging.getLogger(__name__)


class DemoSchema(il.AssetSchema):
    date: dt.date
    hello: str


class DemoConfig(il.Config):
    hello: str = "world"
    random_failure_probability: float = 0.0

    model_config = SettingsConfigDict(env_prefix="Demo_")


partitioning = il.TimePartitionConfig(column="date", allow_window=False)


@il.source(
    config=DemoConfig,
    tags=["Testing"],
)
class DemoSource:
    """Demo source. Defines a small DAG (a -> b,c,d -> e) with time partitioning."""

    #    ↗ b ↘
    #  a → c → e
    #    ↘ d ↗

    config: DemoConfig

    def do(self, context: il.ExecutionContext, name: str) -> pd.DataFrame:
        import random
        import time

        context.logger.info(f"Hello {self.config.hello} from {name}")

        time.sleep(random.uniform(0.5, 1.5))
        if random.random() < self.config.random_failure_probability:
            raise RuntimeError("Random failure in demo source")

        return pd.DataFrame([{"date": context.partition_date, "hello": self.config.hello}])

    @il.asset(
        schema=DemoSchema,
        partitioning=partitioning,
        tags=["Report"],
    )
    def a(
        self,
        context: il.ExecutionContext,
    ) -> pd.DataFrame:
        """Root asset. Returns a single row with the configured greeting."""
        return self.do(context, "A")

    @il.asset(
        schema=DemoSchema,
        partitioning=partitioning,
        tags=["Report"],
    )
    def b(
        self,
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> b -> e)."""
        return self.do(context, "B")

    @il.asset(
        schema=DemoSchema,
        partitioning=partitioning,
        tags=["Report"],
    )
    def c(
        self,
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> c -> e)."""
        return self.do(context, "C")

    @il.asset(
        schema=DemoSchema,
        partitioning=partitioning,
        tags=["Report"],
    )
    def d(
        self,
        context: il.ExecutionContext,
        a: str,
    ) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> d -> e)."""
        return self.do(context, "D")

    @il.asset(
        schema=DemoSchema,
        partitioning=partitioning,
        tags=["Report"],
    )
    def e(
        self,
        context: il.ExecutionContext,
        b: str,
        c: str,
        d: str,
    ) -> pd.DataFrame:
        """Depends on B, C, and D. Sink asset of the example DAG."""
        return self.do(context, "E")
