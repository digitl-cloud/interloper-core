import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class BingAds:
    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Advertising campaign insights including clicks, conversions, impressions, spend, and ad attributes."""

        raise NotImplementedError
