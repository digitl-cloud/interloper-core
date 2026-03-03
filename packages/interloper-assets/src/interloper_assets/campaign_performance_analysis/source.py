import interloper as il
import pandas as pd

from interloper_assets.amazon_ads.source import AmazonAds


@il.source(tags=["Analytics"])
class CampaignPerformanceAnalysis:
    """Cross-source campaign matching and advanced performance analytics."""

    @il.asset(
        tags=["Transform"],
        requires={
            "amazon_ads_campaigns": AmazonAds.products_campaigns.definition_key,
        },
    )
    def matcher(
        self,
        context: il.ExecutionContext,
        amazon_ads_campaigns: pd.DataFrame,
    ) -> pd.DataFrame:
        """Matches campaigns from different advertising sources under the same bucket."""

        raise NotImplementedError

    @il.asset(
        tags=["Analytics"],
    )
    def performance_analysis(
        self,
        context: il.ExecutionContext,
        matcher: pd.DataFrame,
    ) -> pd.DataFrame:
        """Advanced analytics of campaign performance across all matched campaigns."""

        raise NotImplementedError
