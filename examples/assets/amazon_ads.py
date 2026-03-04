import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import AmazonAds, AmazonAdsConfig

load_dotenv()

il.subscribe(print)

partition = il.TimePartition(dt.date(2026, 1, 1))

config = AmazonAdsConfig(profile_id="2302801156455552")
amazon_ads = AmazonAds(config=config, strategy=il.MaterializationStrategy.AUTO)
data = amazon_ads.profiles.run()
# data = amazon_ads.products_advertised_products.run(partition_or_window=partition)
print(data)
