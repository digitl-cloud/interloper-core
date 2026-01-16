import datetime as dt
import logging
from collections.abc import Sequence

import httpx
import interloper as il
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict

from interloper_assets.adservice.schemas.campaigns import Campaigns

logger = logging.getLogger(__name__)


class AdserviceConfig(BaseSettings):
    api_key: str

    model_config = SettingsConfigDict(env_prefix="adservice_")


@il.source(
    config=AdserviceConfig,
)
def adservice(config: AdserviceConfig) -> Sequence[il.AssetDefinition]:
    base_url = "https://api.adservice.com/v2/client"
    auth = httpx.BasicAuth(username="api", password=config.api_key)
    client = il.RESTClient(base_url, auth)

    def get_report(
        start_date: dt.date,
        end_date: dt.date,
        report_type: str,
        group_by: str | None = None,
        end_group: str | None = None,
        sales_amount: int | None = None,
    ) -> dict:
        response = client.get(
            url=f"{base_url}/{report_type}",
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
        schema=Campaigns,
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
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
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def conversions(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="conversions",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def conversions_time_of_day(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/conversions/timeofday",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_city(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="city",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_browser(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="browser",
        )
        data = response["data"]
        return pd.DataFrame(data)

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def campaigns_by_device_type(context: il.ExecutionContext) -> pd.DataFrame:
        response = get_report(
            start_date=context.partition_date,
            end_date=context.partition_date,
            report_type="statistics/devicedetails",
            group_by="device_type",
        )
        data = response["data"]
        return pd.DataFrame(data)

    return (
        campaigns,
        conversions,
        conversions_time_of_day,
        campaigns_by_city,
        campaigns_by_browser,
        campaigns_by_device_type,
    )
