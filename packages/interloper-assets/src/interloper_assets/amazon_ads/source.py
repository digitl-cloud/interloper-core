import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class AmazonAds:
    @il.asset(tags=["Report"])
    def products_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance for advertised products including clicks, impressions, sales, purchases, and attributed
        sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def products_advertised_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products including clicks, impressions, sales, purchases, and attributed sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def products_search_terms(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of search terms in advertising campaigns including click-through rate, clicks, cost,
        impressions, purchases, and sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def products_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertising campaigns based on targeting criteria including click-through rate, clicks, cost,
        impressions, purchases, and sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def products_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products including sales, purchases, SKU, and units sold."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def products_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Impact of gross and invalid traffic on advertising campaigns including clicks, impressions, and traffic
        quality indicators.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Display advertising campaign performance including clicks, impressions, purchases, sales, viewability, and
        attributed sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_advertised_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products within display ad campaigns including clicks, impressions, purchases, and
        sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of promoted products within display ad campaigns including conversions, sales, purchases, and
        attributed sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad campaign performance based on targeting criteria including clicks, impressions, purchases, and sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Traffic quality for display campaigns including total clicks, impressions, and traffic quality indicators."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def display_ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of ad groups within display campaigns including clicks, impressions, purchases, sales, and
        attributed sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Brand promotion campaign performance including impressions, clicks, conversions, and attributed sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Individual ad performance within brand promotion campaigns including impressions, clicks, conversions, and
        attributed sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_search_terms(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Search term performance in brand campaigns including clicks, impressions, purchases, and sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance based on targeting criteria in brand campaigns including clicks, impressions, purchases, and
        sales.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Products purchased through brand campaigns including number of purchases, sales, and attributed sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Traffic quality for brand campaigns including total clicks, impressions, and traffic quality indicators."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_placements(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance by ad placement including clicks, impressions, purchases, and sales."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def brands_ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad group performance within brand campaigns including impressions, clicks, conversions, and attributed
        sales.
        """

        raise NotImplementedError
