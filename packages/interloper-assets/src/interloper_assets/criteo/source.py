import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class Criteo:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign effectiveness across multiple channels and attribution models including conversion rates and return
        on ad spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Marketing campaign performance including click-through rates, conversion rates, cost per acquisition, and
        return on ad spend.
        """

        raise NotImplementedError
