import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class SearchAds360:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance on Search Ads 360 including clicks,
        impressions, average cost, and click-through rate.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def daily_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Daily campaign performance on Search Ads 360 including clicks, impressions, cost, and conversions."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def monthly_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Monthly campaign performance on Search Ads 360 including clicks, impressions, cost, and conversions."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def customer_clients(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Customer client performance on Search Ads 360 including clicks, impressions, cost, and conversions."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def customer_clients_metadata(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Customer client metadata including customer ID, client level, manager information, and account details."""

        raise NotImplementedError
