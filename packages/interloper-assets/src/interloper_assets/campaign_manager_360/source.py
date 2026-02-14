import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class CampaignManager360:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance and effectiveness including impressions, clicks, CTR, conversions, and cost."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance within Campaign Manager 360 including impressions, clicks, conversions, and cost."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def reach(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Reach and engagement metrics focusing on unique user impressions and reach frequency."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def custom_audiences(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Audience data for Campaign Manager 360."""

        raise NotImplementedError
