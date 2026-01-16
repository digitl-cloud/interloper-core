import datetime as dt

import interloper as il
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict

from interloper_assets.adup import constants
from interloper_assets.adup.schemas.ads import Ads


class AdupConfig(BaseSettings):
    client_id: str
    client_secret: str

    model_config = SettingsConfigDict(env_prefix="adup_")


@il.source(
    config=AdupConfig,
)
def adup(config: AdupConfig) -> tuple[il.AssetDefinition, ...]:
    auth = il.OAuth2ClientCredentialsAuth(constants.BASE_URL, config.client_id, config.client_secret)
    client = il.RESTClient(constants.BASE_URL, auth)

    def get_report(report_type: str, start_date: dt.date, end_date: dt.date) -> dict:
        response = client.post(
            "/reports/v202101/report",
            json={
                "report_name": report_type,
                "report_type": report_type,
                "select": constants.FIELDS[report_type],
                "conditions": [],
                "download_format": "JSON",
                "date_range_type": "CUSTOM_DATE",
                "date_range": {
                    "min": start_date.isoformat(),
                    "max": end_date.isoformat(),
                },
            },
        )
        response.raise_for_status()

        return response.json()

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
    )
    def account(context: il.ExecutionContext) -> pd.DataFrame:
        response = client.get("/advertisers/me")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame([data])

    @il.asset(
        schema=Ads,
        partitioning=il.TimePartitionConfig(column="Date", allow_window=True),
    )
    def ads(context: il.ExecutionContext) -> pd.DataFrame:
        (start_date, end_date) = context.partition_date_window
        response = get_report("AD_PERFORMANCE_REPORT", start_date, end_date)
        data = response["rows"]
        return pd.DataFrame(data)

    return (account, ads)
