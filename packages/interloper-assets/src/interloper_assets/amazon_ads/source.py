import datetime as dt
import gzip
import json
from enum import Enum
from functools import partial

import httpx
import interloper as il
import pandas as pd
from interloper_pandas import DataFrameNormalizer
from pydantic_settings import SettingsConfigDict
from tenacity import (
    RetryCallState,
    Retrying,
    retry,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_incrementing,
)

from interloper_assets.amazon_ads import constants, schemas


class AmazonAdsAPILocation(Enum):
    NORTH_AMERIC = "NA"
    EUROPE = "EU"
    FAR_EAST = "FE"

    @property
    def api_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://advertising-api-eu.amazon.com",
            AmazonAdsAPILocation.FAR_EAST: "https://advertising-api-fe.amazon.com",
            AmazonAdsAPILocation.NORTH_AMERIC: "https://advertising-api.amazon.com",
        }[self]

    @property
    def auth_url(self) -> str:
        return {
            AmazonAdsAPILocation.EUROPE: "https://api.amazon.co.uk",
            AmazonAdsAPILocation.FAR_EAST: "https://api.amazon.co.jp",
            AmazonAdsAPILocation.NORTH_AMERIC: "https://api.amazon.com",
        }[self]


class AmazonAdsConfig(il.Config):
    location: str
    profile_id: str
    client_id: str
    client_secret: str
    refresh_token: str

    model_config = SettingsConfigDict(env_prefix="amazon_ads_")


@il.source(
    config=AmazonAdsConfig,
    tags=["Advertising"],
    normalizer=DataFrameNormalizer(flatten_max_level=1),
)
class AmazonAds:
    config: AmazonAdsConfig
    location: AmazonAdsAPILocation
    client: il.RESTClient

    def __init__(self, config: AmazonAdsConfig) -> None:
        self.location = AmazonAdsAPILocation(config.location)
        self.client = il.RESTClient(
            self.location.api_url,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=self.location.auth_url,
                token_endpoint="/auth/o2/token",
                client_id=config.client_id,
                client_secret=config.client_secret,
                refresh_token=config.refresh_token,
            ),
        )
        self.client.base_url = self.location.api_url
        self.client.headers.update({"Amazon-Advertising-API-ClientId": config.client_id})

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def request_report(
        self,
        report_type_id: str,
        profile_id: str,
        ad_product: str,
        group_by: list[str],
        columns: list[str],
        start_date: dt.date,
        end_date: dt.date,
    ) -> dict:
        response = self.client.post(
            "/reporting/reports",
            headers={
                "Amazon-Advertising-API-Scope": profile_id,
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
            json={
                "name": f"{report_type_id} | {start_date.isoformat()} - {end_date.isoformat()}",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "configuration": {
                    "reportTypeId": report_type_id,
                    "adProduct": ad_product,
                    "groupBy": group_by,
                    "columns": columns,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
        )
        response.raise_for_status()
        return response.json()

    def get_report_status(self, profile_id: str, report_id: str) -> dict:
        response = self.client.get(
            f"/reporting/reports/{report_id}",
            headers={
                "Amazon-Advertising-API-Scope": profile_id,
                "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            },
        )
        response.raise_for_status()
        return response.json()

    def download_report(self, url: str) -> list[dict]:
        # Use httpx.get directly to not use the client's auth
        response = httpx.get(url, timeout=None)
        response.raise_for_status()

        gzip_content = response.content
        json_content = gzip.decompress(gzip_content)

        return json.loads(json_content)

    def _log_retry(self, context: il.ExecutionContext, retry_state: RetryCallState) -> None:
        if retry_state.outcome is None:
            raise RuntimeError("log called before outcome was set")

        if retry_state.next_action is None:
            raise RuntimeError("log called before next_action was set")

        if retry_state.outcome.failed:
            ex = retry_state.outcome.exception()
            context.logger.debug(f"Retrying in {retry_state.next_action.sleep}. Raised {ex.__class__.__name__}: {ex}.")
            if isinstance(ex, httpx.HTTPStatusError) and ex.response.status_code == 429:
                context.logger.debug(f"Retry-After response header: {ex.response.headers.get('Retry-After')}")
        else:
            context.logger.debug(
                f"Retrying in {retry_state.next_action.sleep}. Returned {retry_state.outcome.result()}"
            )

    @retry(stop=stop_after_attempt(5))
    def _clear_auth(self) -> None:
        auth = self.client.auth
        if isinstance(auth, il.OAuth2RefreshTokenAuth):
            # Clear the token to force a new token to be acquired
            auth.clear_token()

    def request_and_download_report(
        self,
        context: il.ExecutionContext,
        report_type_id: str,
        profile_id: str,
        ad_product: str,
        group_by: list[str],
        columns: list[str],
        start_date: dt.date,
        end_date: dt.date,
        max_request_delay: int | dt.timedelta = dt.timedelta(hours=1),
        max_wait_delay: int | dt.timedelta = dt.timedelta(hours=2),
        max_download_delay: int | dt.timedelta = dt.timedelta(minutes=10),
    ) -> list[dict]:
        ConfiguredRetrying = partial(
            Retrying,
            wait=wait_exponential(multiplier=10, max=60),
            before=lambda _: self._clear_auth(),
            before_sleep=partial(self._log_retry, context),
            reraise=True,
        )

        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_request_delay),
            wait=wait_incrementing(start=60, increment=dt.timedelta(minutes=5), max=dt.timedelta(minutes=15)),
        ):
            with attempt:
                context.logger.info(
                    f"Requesting {report_type_id} report for profile {profile_id} "
                    f"(attempt {attempt.retry_state.attempt_number})"
                )
                data = self.request_report(
                    report_type_id, profile_id, ad_product, group_by, columns, start_date, end_date
                )

        report_id = data["reportId"]
        context.logger.info(f"Report id: {report_id} for profile {profile_id}")

        # Wait for report
        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_wait_delay),
            retry=retry_if_result(lambda status: status != "COMPLETED")
            | retry_if_exception(lambda e: issubclass(e.__class__, httpx.HTTPError)),
        ):
            with attempt:
                context.logger.info(f"Waiting for report {report_id} (profile {profile_id}) ")
                response = self.get_report_status(profile_id, report_id)

            if attempt.retry_state.outcome and not attempt.retry_state.outcome.failed:
                attempt.retry_state.set_result(response["status"])

        report_url = response["url"]
        context.logger.info(f"Report URL: {report_url} for profile {profile_id}")

        for attempt in ConfiguredRetrying(
            stop=stop_after_delay(max_download_delay),
        ):
            with attempt:
                context.logger.info(f"Downloading report (attempt {attempt.retry_state.attempt_number})")
                report = self.download_report(report_url)

        return report

    # ------------------------------------------------------------------
    # ASSETS
    # ------------------------------------------------------------------

    @il.asset(
        schema=schemas.Profiles,
        tags=["Entity"],
    )
    def profiles(self) -> pd.DataFrame:
        response = self.client.get("/v2/profiles")
        response.raise_for_status()
        return pd.DataFrame(response.json())

    @il.asset(
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def products_advertised_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products including clicks, impressions, sales, purchases, and attributed sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.PRODUCTS_ADVERTISED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def products_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance for advertised products including clicks, impressions, sales, purchases, and attributed
        sales.
        """
        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spCampaigns",
            group_by=["campaign", "adGroup"],
            columns=constants.PRODUCTS_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def products_search_terms(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of search terms in advertising campaigns including click-through rate, clicks, cost,
        impressions, purchases, and sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spSearchTerm",
            group_by=["searchTerm"],
            columns=constants.PRODUCTS_SEARCH_TERM_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def products_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertising campaigns based on targeting criteria including click-through rate, clicks, cost,
        impressions, purchases, and sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spTargeting",
            group_by=["targeting"],
            columns=constants.PRODUCTS_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def products_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products including sales, purchases, SKU, and units sold."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spPurchasedProduct",
            group_by=["asin"],
            columns=constants.PRODUCTS_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def products_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Impact of gross and invalid traffic on advertising campaigns including clicks, impressions, and traffic
        quality indicators.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_PRODUCTS",
            report_type_id="spGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.PRODUCTS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Display advertising campaign performance including clicks, impressions, purchases, sales, viewability, and
        attributed sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdCampaigns",
            group_by=["campaign"],
            columns=constants.DISPLAY_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_advertised_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of advertised products within display ad campaigns including clicks, impressions, purchases, and
        sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdvertisedProduct",
            group_by=["advertiser"],
            columns=constants.DISPLAY_ADVERTISED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of promoted products within display ad campaigns including conversions, sales, purchases, and
        attributed sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdPurchasedProduct",
            group_by=["asin"],
            columns=constants.DISPLAY_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad campaign performance based on targeting criteria including clicks, impressions, purchases, and sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdTargeting",
            group_by=["targeting"],
            columns=constants.DISPLAY_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Traffic quality for display campaigns including total clicks, impressions, and traffic quality indicators."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.DISPLAY_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def display_ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Performance of ad groups within display campaigns including clicks, impressions, purchases, sales, and
        attributed sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_DISPLAY",
            report_type_id="sdAdGroup",
            group_by=["adGroup"],
            columns=constants.DISPLAY_AD_GROUP_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Brand promotion campaign performance including impressions, clicks, conversions, and attributed sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaigns",
            group_by=["campaign"],
            columns=constants.BRANDS_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_ads(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Individual ad performance within brand promotion campaigns including impressions, clicks, conversions, and
        attributed sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAds",
            group_by=["ads"],
            columns=constants.BRANDS_AD_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_search_terms(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Search term performance in brand campaigns including clicks, impressions, purchases, and sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbSearchTerm",
            group_by=["searchTerm"],
            columns=constants.BRANDS_SEARCH_TERM_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad performance based on targeting criteria in brand campaigns including clicks, impressions, purchases, and
        sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbTargeting",
            group_by=["targeting"],
            columns=constants.BRANDS_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_purchased_products(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Products purchased through brand campaigns including number of purchases, sales, and attributed sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbPurchasedProduct",
            group_by=["purchasedAsin"],
            columns=constants.BRANDS_PURCHASED_PRODUCT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_gross_and_invalid_traffic(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Traffic quality for brand campaigns including total clicks, impressions, and traffic quality indicators."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbGrossAndInvalids",
            group_by=["campaign"],
            columns=constants.BRANDS_GROSS_AND_INVALID_TRAFFIC_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_placements(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Campaign performance by ad placement including clicks, impressions, purchases, and sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbCampaignPlacement",
            group_by=["campaignPlacement"],
            columns=constants.BRANDS_PLACEMENT_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def brands_ad_groups(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Ad group performance within brand campaigns including impressions, clicks, conversions, and attributed
        sales.
        """

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_BRANDS",
            report_type_id="sbAdGroup",
            group_by=["adGroup"],
            columns=constants.BRANDS_AD_GROUP_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def television_campaigns(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Television campaign performance including clicks, impressions, purchases, and sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stCampaigns",
            group_by=["campaign"],
            columns=constants.TELEVISION_CAMPAIGN_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)

    @il.asset(tags=["Report"])
    def television_targeting(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Television campaign performance by targeting including clicks, impressions, purchases, and sales."""

        data = self.request_and_download_report(
            context,
            profile_id=self.config.profile_id,
            ad_product="SPONSORED_TELEVISION",
            report_type_id="stTargeting",
            group_by=["targeting"],
            columns=constants.TELEVISION_TARGETING_METRICS,
            start_date=context.partition_date,
            end_date=context.partition_date,
        )
        return pd.DataFrame(data)
