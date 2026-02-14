import interloper as il
import pandas as pd


@il.source(tags=["Advertising"])
class AmazonSellingPartner:
    @il.asset(tags=["Report"])
    def vendor_sales_retail_manufacturing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Retail performance of manufactured products including ordered units, revenue, cost of goods sold, and profit
        margins.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_sales_retail_sourcing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Retail performance of sourced products including shipped units, revenue, cost of goods sold, and profit
        margins.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_sales_business_manufacturing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Sales performance and manufacturing-related metrics
        including revenue, units sold, and manufacturing costs.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_sales_business_sourcing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of sourced products including shipped units, revenue, cost of goods sold, and profit margins."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_inventory_retail_manufacturing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Inventory management and performance including inventory levels, stock movements, and inventory costs."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_inventory_retail_sourcing(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Sourcing aspect of inventory management including inventory cost, units, lead time, and stock levels."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_forecasting_retail(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Forecasted demand for products including mean forecast units, 70th percentile forecast, and forecast
        accuracy.
        """

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Product visibility and engagement on the Amazon marketplace including glance views."""

        raise NotImplementedError

    @il.asset(tags=["Report"])
    def vendor_net_pure_product_margin(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Net pure product margin insights for products."""

        raise NotImplementedError
