import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class FacebookAds:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance insights for advertising campaigns including clicks, impressions, spend, and engagement rates."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance and effectiveness of Facebook advertising campaigns including clicks, impressions, spend, and
        engagement actions.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_age_gender(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by age and gender demographics including clicks, impressions, spend, and engagement rates."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_country(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance across different countries and regions including clicks, impressions, spend, and engagement
        rates.
        """

        raise NotImplementedError
