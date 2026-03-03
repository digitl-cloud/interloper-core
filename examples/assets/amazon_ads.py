import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import AmazonAds
from interloper_assets.amazon_ads.source import AmazonAdsConfig

load_dotenv()

il.subscribe(print)

partition = il.TimePartition(dt.date(2024, 1, 1))

amazon_ads = AmazonAds(config=AmazonAdsConfig())
print(amazon_ads.profiles.run())

# dag = il.DAG(amazon_ads)
# dag.materialize(partition_or_window=partition)
