import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class DisplayVideo360:
    @il.asset(tags=["Report"])
    def line_items(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Advertising campaign performance and costs including clicks, impressions, and media costs."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def line_items_by_country(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Advertising campaign performance and costs segmented by country including clicks, impressions, and media
        costs.
        """

        raise NotImplementedError
