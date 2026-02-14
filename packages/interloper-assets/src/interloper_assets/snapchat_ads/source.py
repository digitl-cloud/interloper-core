import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class SnapchatAds:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance including attachment quartiles, view time, conversion rates, impressions, and spend."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Individual ad performance including attachment views, conversions, custom events, impressions, saves, and
        spend.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_by_country(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by country including view time, conversion events, impressions, and spend."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads_geo(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance by geographical data including attachment views, conversions, impressions, and spend."""

        raise NotImplementedError
