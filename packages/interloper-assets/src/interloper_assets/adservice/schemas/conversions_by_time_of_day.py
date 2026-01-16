from pydantic import BaseModel, Field


class ConversionsByTimeOfDay(BaseModel):
    """
    The Conversions by Time of Day report provides insights into conversion patterns based on the day of the week
    and hour of the day. It includes key metrics such as the number of conversions and revenue generated within each
    time segment.
    """

    conversions: int = Field(description="Number of conversions")
    day: str = Field(description="Day of the week")
    hour: int = Field(description="Hour of the day")
    revenue: float = Field(description="Revenue generated")
