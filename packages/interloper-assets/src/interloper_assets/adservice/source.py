import datetime as dt
import logging

import httpx
import interloper as il
import pandas as pd
from pydantic_settings import SettingsConfigDict

from interloper_assets.adservice import schemas

logger = logging.getLogger(__name__)


class AdserviceConfig(il.Config):
    api_key: str

    model_config = SettingsConfigDict(env_prefix="adservice_")


@il.source(
    config=AdserviceConfig,
    tags=["Advertising"],
)
class Adservice:
    """Adservice advertising platform integration."""

    def __init__(self, config: AdserviceConfig) -> None:
        base_url = "https://api.adservice.com/v2/client"
        auth = httpx.BasicAuth(username="api", password=config.api_key)
        self.client = il.RESTClient(base_url, auth)
        self.base_url = base_url

    def get_report(
        self,
        start_date: dt.date,
        end_date: dt.date,
        report_type: str,
        group_by: str | None = None,
        end_group: str | None = None,
        sales_amount: int | None = None,
    ) -> dict:
        response = self.client.get(
            url=f"{self.base_url}/{report_type}",
            params={
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "sales_amount": sales_amount,
                "group_by": group_by,
                "end_group": end_group,
            },
        )
        response.raise_for_status()
        return response.json()

    @il.asset(
        schema=schemas.Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance statistics with metrics like impressions, clicks, and conversions."""

        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics",
            group_by="stamp,camp_id",
            end_group="stamp",
            sales_amount=1,
        )
        data = response["data"]["rows"]
        return pd.DataFrame(data)

    @il.asset(
        schema=schemas.ConversionsReport,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def conversions(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Conversion events and attribution data."""
        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="conversions",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        schema=schemas.ConversionsByTimeOfDay,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def conversions_time_of_day(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Conversion events broken down by time of day."""

        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/conversions/timeofday",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        schema=schemas.CampaignsByCity,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_city(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance segmented by city."""

        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="city",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        schema=schemas.CampaignsByBrowser,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_browser(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance segmented by browser."""

        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="browser",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        schema=schemas.CampaignsByDeviceType,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_device_type(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance segmented by device type."""

        response = self.get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="device_type",
        )
        data = response["data"]
        return pd.DataFrame(data)
