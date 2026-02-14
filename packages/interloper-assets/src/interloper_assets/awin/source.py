import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class Awin:
    @il.asset(tags=["Report"])
    def advertiser(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Advertiser performance within the Awin network including clicks, impressions, conversions, and revenue."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def advertiser_by_publishers(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Advertiser performance across publishers within the Awin network including clicks, impressions,
        confirmations, and revenue.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def advertiser_transactions(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Transactional activities including clicks, commissions, sales, transaction value, commission rates, and
        conversion data.
        """

        raise NotImplementedError
