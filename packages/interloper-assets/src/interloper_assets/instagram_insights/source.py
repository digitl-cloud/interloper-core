import interloper as il
import pandas as pd


@il.source(tags=["Social"])
class InstagramInsights:
    @il.asset(tags=["Report"])
    def insights(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Instagram account performance including follower count, impressions, reach, and engagement actions."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def insights_by_age_gender(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Demographic breakdowns of Instagram account insights by age and gender."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def insights_by_country(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Geographic breakdowns of Instagram account insights by country."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def insights_by_city(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Geographic breakdowns of Instagram account insights by city."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def engagement(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Instagram engagement metrics."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def media(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Instagram media performance report."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def profiles(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Instagram profile data."""

        raise NotImplementedError
