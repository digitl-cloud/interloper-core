import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class Teads:
    @il.asset(tags=["Report"])
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance including billable events, delivered budget, CPC, CPM, and viewability."""

        raise NotImplementedError
