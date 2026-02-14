import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class TiktokAds:
    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance in TikTok campaigns including clicks, conversion value, conversion rate, impressions, and
        spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_country(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by country including clicks, conversion value, conversion rate, cost per conversion,
        impressions, and spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_age_gender(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by age and gender demographics including clicks, conversions, conversion rate, impressions,
        and spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_platform(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by platform including clicks, conversion value, conversion rate, cost per conversion,
        impressions, and spend.
        """

        raise NotImplementedError
