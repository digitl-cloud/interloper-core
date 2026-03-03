from pydantic import BaseModel, Field


class Profiles(BaseModel):
    profileId: int = Field(..., description="The Amazon Ads profile ID")
    countryCode: str = Field(..., description="The account's country code")
    currencyCode: str = Field(..., description="The account's currency code")
    timezone: str = Field(..., description="The account's timezone")
    accountInfo: dict = Field(..., description="Account information, including marketplace string ID and id")
