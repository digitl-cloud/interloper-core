import interloper as il
from interloper.errors import ConfigError, SourceError
from interloper.io.base import IO
from interloper_google_cloud import BigQueryIO
from interloper_sql import MySQLIO, PostgresIO

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


IO_REGISTRY: dict[str, type[IO]] = {
    "PostgreSQL": PostgresIO,
    "MySQL":      MySQLIO,
    "BigQuery":   BigQueryIO,
}


def get_io(key: str) -> type[IO]:
    """Get an IO class by destination key.

    The IO class carries its own config as model fields, so no separate
    config class lookup is needed.

    Args:
        key: Destination type identifier (e.g. "PostgreSQL", "BigQuery")

    Returns:
        The IO class registered under *key*.

    Raises:
        ConfigError: If the key is not found in the registry.
    """
    if key not in IO_REGISTRY:
        raise ConfigError(f"Unknown IO key: {key}")
    return IO_REGISTRY[key]


def get_all_ios() -> dict[str, type[IO]]:
    """Get all registered IO backends.

    Returns:
        Dictionary mapping destination keys to IO classes.
    """
    return dict(IO_REGISTRY)


__all__ = [
    "IO_REGISTRY",
    "SOURCE_REGISTRY",
    "get_all_ios",
    "get_all_sources",
    "get_io",
    "get_source_and_config",
]
