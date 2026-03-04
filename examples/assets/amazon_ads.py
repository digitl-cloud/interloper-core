import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import AmazonAds, AmazonAdsConfig

load_dotenv()

il.subscribe(print)

partition = il.TimePartition(dt.date(2024, 1, 1))

amazon_ads = AmazonAds(config=AmazonAdsConfig(profile_id="2302801156455552"))
print(amazon_ads.products_advertised_products.run(partition_or_window=partition))

# dag = il.DAG(amazon_ads)
# dag.materialize(partition_or_window=partition)
