from pydantic import BaseModel, Field


class CampaignsByBrowser(BaseModel):
    """
    The Campaigns by Browser report provides insights into campaign performance based on the user's browser usage.
    It includes key metrics such as conversion rates, the number of conversions, and user demographics such as
    city, country, device model, device type, and operating system.
    """

    agent_id: int = Field(description="The ID of the agent")
    browser: str = Field(description="The browser used by the user")
    camp_id: int = Field(description="The ID of the campaign")
    camp_title: str = Field(description="The title of the campaign")
    city: str = Field(description="The city where the user is located")
    conversion_pct: int = Field(description="The percentage of conversions")
    conversions: int = Field(description="The number of conversions")
    country: str = Field(description="The country where the user is located")
    device_model: str = Field(description="The model of the device used by the user")
    device_type: str = Field(description="The type of device used by the user")
    os: str = Field(description="The operating system used by the user")
