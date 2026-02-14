import interloper as il
import pandas as pd


@il.source(tags=["Social"])
class LinkedinOrganic:
    @il.asset(tags=["Report"])
    def page(self, context: il.ExecutionContext) -> pd.DataFrame:
        """LinkedIn organic page statistics."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def posts(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance metrics for LinkedIn posts."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def post(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance metrics for individual LinkedIn posts."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def followers(self, context: il.ExecutionContext) -> pd.DataFrame:
        """LinkedIn follower counts by various dimensions."""

        raise NotImplementedError
