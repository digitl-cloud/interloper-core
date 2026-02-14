import interloper as il
import pandas as pd


@il.source(tags=["Social"])
class FacebookInsights:
    @il.asset(tags=["Report"])
    def page(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Engagement and reach metrics for a Facebook page including reactions, impressions, engagements, and video
        views.
        """

        raise NotImplementedError
