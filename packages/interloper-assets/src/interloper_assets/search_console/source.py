import interloper as il
import pandas as pd


@il.source(tags=["Search"])
class SearchConsole:
    @il.asset(tags=["Report"])
    def site(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Website performance in search results including clicks, impressions, CTR, position, and search queries."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def site_by_country_device(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Website performance in search results by country and device including clicks, impressions, CTR, and
        position.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def site_by_country_page(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Website performance in search results by country and page
        including clicks, impressions, CTR, and position.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def search_appearance(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Website appearance in Google Search results including clicks, impressions, CTR, and position."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def page(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Page-level performance in search results including clicks, impressions, CTR, position, and search queries."""

        raise NotImplementedError
