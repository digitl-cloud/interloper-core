import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class PinterestAds:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance on Pinterest including conversions, click-through rates, engagement rates, costs per
        click, impressions, and spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance on Pinterest including click-through conversions, engagement rates, costs per click,
        impressions, and spend.
        """

        raise NotImplementedError
