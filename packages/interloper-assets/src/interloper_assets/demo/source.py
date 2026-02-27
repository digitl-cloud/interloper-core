import logging

import interloper as il
import pandas as pd
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

logger = logging.getLogger(__name__)


class DemoSchema(BaseModel):
    hello: str


class DemoConfig(il.Config):
    hello: str = "world"

    model_config = SettingsConfigDict(env_prefix="Demo_")


partitioning = il.TimePartitionConfig(column="date", allow_window=False)


@il.source(
    config=DemoConfig,
    tags=["Testing"],
)
class DemoSource:
    """Demo source. Defines a small DAG (a -> b,c,d -> e) with time partitioning."""

    config: DemoConfig

    def do(self) -> None:
        import random
        import time

        time.sleep(random.uniform(0.5, 1.5))
        if random.random() < 0.15:
            raise RuntimeError("Random failure in demo source")

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
        print(f"Hello from A {self.config.hello}")
        self.do()
        return pd.DataFrame([{"hello": self.config.hello}])

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
        print(f"Hello from B {self.config.hello}")
        self.do()
        return pd.DataFrame([{"hello": self.config.hello}])

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
        print(f"Hello from C {self.config.hello}")
        self.do()
        return pd.DataFrame([{"hello": self.config.hello}])

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
        print(f"Hello from D {self.config.hello}")
        self.do()
        return pd.DataFrame([{"hello": self.config.hello}])

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
        print(f"Hello from E {self.config.hello}")
        self.do()
        return pd.DataFrame([{"hello": self.config.hello}])


#    ↗ b ↘
#  a → c → e
#    ↘ d ↗
