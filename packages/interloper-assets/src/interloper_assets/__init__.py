import interloper as il
from interloper.destination.base import Destination
from interloper.errors import ConfigError, SourceError
from interloper_google_cloud import BigQueryDestination
from interloper_sql import MySQLDestination, PostgresDestination

from interloper_assets.adservice.source import Adservice, AdserviceConfig
from interloper_assets.adup.source import Adup, AdupConfig
from interloper_assets.amazon_ads.source import AmazonAds, AmazonAdsConfig
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
    DemoSource.key: (DemoSource, DemoConfig),
    Adup.key: (Adup, AdupConfig),
    Adservice.key: (Adservice, AdserviceConfig),
    AmazonAds.key: (AmazonAds, AmazonAdsConfig),
    AmazonSellingPartner.key: (AmazonSellingPartner, None),
    Awin.key: (Awin, None),
    BingAds.key: (BingAds, None),
    CampaignManager360.key: (CampaignManager360, None),
    CampaignPerformanceAnalysis.key: (CampaignPerformanceAnalysis, None),
    Criteo.key: (Criteo, None),
    DisplayVideo360.key: (DisplayVideo360, None),
    FacebookAds.key: (FacebookAds, None),
    FacebookInsights.key: (FacebookInsights, None),
    InstagramInsights.key: (InstagramInsights, None),
    LinkedinAds.key: (LinkedinAds, None),
    LinkedinOrganic.key: (LinkedinOrganic, None),
    PinterestAds.key: (PinterestAds, None),
    SearchAds360.key: (SearchAds360, None),
    SearchConsole.key: (SearchConsole, None),
    SnapchatAds.key: (SnapchatAds, None),
    Teads.key: (Teads, None),
    TheTradeDesk.key: (TheTradeDesk, None),
    TiktokAds.key: (TiktokAds, None),
}


def get_source_and_config(id: str) -> tuple[il.SourceDefinition, type[il.Config] | None]:
    """Get a source definition and its config type by source type ID.

    Args:
        id: Source type identifier (e.g. FacebookAds.key, DemoSource.key)

    Returns:
        Tuple of (SourceDefinition, ConfigType or None)

    Raises:
        ValueError: If the source ID is not found in the registry
    """
    if id not in SOURCE_REGISTRY:
        raise SourceError(f"Unknown source ID: {id}")
    return SOURCE_REGISTRY[id]


def get_all_sources() -> dict[str, tuple[il.SourceDefinition, type[il.Config] | None]]:
    """Get all registered sources.

    Returns:
        Dictionary mapping source type IDs to (SourceDefinition, ConfigType) tuples
    """
    return dict(SOURCE_REGISTRY)


DESTINATION_REGISTRY: dict[str, type[Destination]] = {
    "PostgreSQL": PostgresDestination,
    "MySQL":      MySQLDestination,
    "BigQuery":   BigQueryDestination,
}


def get_destination(key: str) -> type[Destination]:
    """Get a Destination class by key.

    The Destination class carries its own config as model fields, so no
    separate config class lookup is needed.

    Args:
        key: Destination type identifier (e.g. "PostgreSQL", "BigQuery")

    Returns:
        The Destination class registered under *key*.

    Raises:
        ConfigError: If the key is not found in the registry.
    """
    if key not in DESTINATION_REGISTRY:
        raise ConfigError(f"Unknown destination key: {key}")
    return DESTINATION_REGISTRY[key]


def get_all_destinations() -> dict[str, type[Destination]]:
    """Get all registered destination backends.

    Returns:
        Dictionary mapping destination keys to Destination classes.
    """
    return dict(DESTINATION_REGISTRY)


__all__ = [
    "DESTINATION_REGISTRY",
    "SOURCE_REGISTRY",
    "get_all_destinations",
    "get_all_sources",
    "get_destination",
    "get_source_and_config",
]
