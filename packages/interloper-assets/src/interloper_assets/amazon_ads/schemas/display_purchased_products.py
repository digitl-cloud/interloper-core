import datetime

from pydantic import BaseModel, Field


class DisplayPurchasedProducts(BaseModel):
    """
    The Display Purchased Products report provides insights into the performance of promoted products within
    ad campaigns on Amazon Ads. It includes key metrics such as conversions, sales, and units sold.
    """

    ad_group_id: int | None = Field(..., description="The ID of the ad group")
    ad_group_name: str | None = Field(..., description="The name of the ad group")
    asin_brand_halo: str | None = Field(..., description="The ASIN with brand halo")
    campaign_budget_currency_code: str | None = Field(..., description="The currency code of the campaign budget")
    campaign_id: int | None = Field(..., description="The ID of the campaign")
    campaign_name: str | None = Field(..., description="The name of the campaign")
    conversions_brand_halo: float | None = Field(..., description="The number of conversions with brand halo")
    conversions_brand_halo_clicks: float | None = Field(
        ..., description="The number of conversions with brand halo clicks"
    )
    date: datetime.date | None = Field(..., description="The date of the record")
    promoted_asin: str | None = Field(..., description="The ASIN of the promoted product")
    promoted_sku: str | None = Field(..., description="The SKU of the promoted product")
    sales_brand_halo: float | None = Field(..., description="The sales with brand halo")
    sales_brand_halo_clicks: float | None = Field(..., description="The sales with brand halo clicks")
    units_sold_brand_halo: float | None = Field(..., description="The number of units sold with brand halo")
    units_sold_brand_halo_clicks: float | None = Field(
        ..., description="The number of units sold with brand halo clicks"
    )
