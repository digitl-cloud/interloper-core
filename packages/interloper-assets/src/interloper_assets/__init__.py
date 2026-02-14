import interloper as il

from interloper_assets.adservice.source import Adservice, AdserviceConfig
from interloper_assets.adup.source import Adup, AdupConfig
from interloper_assets.amazon_ads.source import AmazonAds
from interloper_assets.amazon_selling_partner.source import AmazonSellingPartner
from interloper_assets.awin.source import Awin
from interloper_assets.bing_ads.source import BingAds
from interloper_assets.campaign_manager_360.source import CampaignManager360
from interloper_assets.campaign_performance_analysis.source import CampaignPerformanceAnalysis
from interloper_assets.criteo.source import Criteo
from interloper_assets.demo.source import DemoConfig, DemoSource
from interloper_assets.display_video_360.source import DisplayVideo360
from interloper_assets.facebook_ads.source import FacebookAds
from interloper_assets.facebook_insights.source import FacebookInsights
from interloper_assets.instagram_insights.source import InstagramInsights
from interloper_assets.linkedin_ads.source import LinkedinAds
from interloper_assets.linkedin_organic.source import LinkedinOrganic
from interloper_assets.pinterest_ads.source import PinterestAds
from interloper_assets.search_ads_360.source import SearchAds360
from interloper_assets.search_console.source import SearchConsole
from interloper_assets.snapchat_ads.source import SnapchatAds
from interloper_assets.teads.source import Teads
from interloper_assets.thetradedesk.source import TheTradeDesk
from interloper_assets.tiktok_ads.source import TiktokAds

SOURCE_REGISTRY: dict[str, tuple[il.SourceDefinition, type[il.Config] | None]] = {
    DemoSource.name: (DemoSource, DemoConfig),
    Adup.name: (Adup, AdupConfig),
    Adservice.name: (Adservice, AdserviceConfig),
    AmazonAds.name: (AmazonAds, None),
    AmazonSellingPartner.name: (AmazonSellingPartner, None),
    Awin.name: (Awin, None),
    BingAds.name: (BingAds, None),
    CampaignManager360.name: (CampaignManager360, None),
    CampaignPerformanceAnalysis.name: (CampaignPerformanceAnalysis, None),
    Criteo.name: (Criteo, None),
    DisplayVideo360.name: (DisplayVideo360, None),
    FacebookAds.name: (FacebookAds, None),
    FacebookInsights.name: (FacebookInsights, None),
    InstagramInsights.name: (InstagramInsights, None),
    LinkedinAds.name: (LinkedinAds, None),
    LinkedinOrganic.name: (LinkedinOrganic, None),
    PinterestAds.name: (PinterestAds, None),
    SearchAds360.name: (SearchAds360, None),
    SearchConsole.name: (SearchConsole, None),
    SnapchatAds.name: (SnapchatAds, None),
    Teads.name: (Teads, None),
    TheTradeDesk.name: (TheTradeDesk, None),
    TiktokAds.name: (TiktokAds, None),
}


def get_source_and_config(id: str) -> tuple[il.SourceDefinition, type[il.Config] | None]:
    """Get a source definition and its config type by source type ID.

    Args:
        id: Source type identifier (e.g. FacebookAds.name, DemoSource.name)

    Returns:
        Tuple of (SourceDefinition, ConfigType or None)

    Raises:
        ValueError: If the source ID is not found in the registry
    """
    if id not in SOURCE_REGISTRY:
        raise ValueError(f"Unknown source ID: {id}")
    return SOURCE_REGISTRY[id]


def get_all_sources() -> dict[str, tuple[il.SourceDefinition, type[il.Config] | None]]:
    """Get all registered sources.

    Returns:
        Dictionary mapping source type IDs to (SourceDefinition, ConfigType) tuples
    """
    return dict(SOURCE_REGISTRY)


__all__ = [
    "SOURCE_REGISTRY",
    "get_all_sources",
    "get_source_and_config",
]
