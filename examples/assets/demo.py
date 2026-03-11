import datetime as dt

import interloper as il
from interloper_assets.demo.source import DemoSource
from interloper_google_cloud import BigQueryDestination
from interloper_pandas import DataFrameAdapter

il.subscribe(print)

destination = BigQueryDestination(
    project="dc-int-connectors-prd",
    default_dataset="interloper",
    adapter=DataFrameAdapter(),
)

demo = DemoSource(destination=destination)

# print(demo.a.partition_row_counts())

dag = il.DAG(demo)

with il.MultiThreadRunner() as runner:
    result = runner.run(dag, il.TimePartition(dt.date(2025, 1, 1)))

print(result)
