from pydantic import BaseModel, Field


class CampaignsByCity(BaseModel):
    """
    The Campaigns by City report provides insights into campaign performance segmented by cities.
    It includes key metrics such as conversion rates, the number of conversions, and user demographics
    such as browser usage, device type, operating system, and geographical coordinates (latitude and longitude).
    """

    agent_id: int = Field(description="The ID of the agent")
    browser: str = Field(description="The browser used")
    camp_id: int = Field(description="The ID of the campaign")
    camp_title: str = Field(description="The title of the campaign")
    city: str = Field(description="The city")
    conversion_pct: int = Field(description="The conversion percentage")
    conversions: int = Field(description="The number of conversions")
    coordinates_lat: float = Field(description="The latitude coordinates")
    coordinates_lng: float = Field(description="The longitude coordinates")
    country: str = Field(description="The country")
    device_model: str = Field(description="The device model")
    device_type: str = Field(description="The device type")
    os: str = Field(description="The operating system")
