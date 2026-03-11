import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import Adservice

load_dotenv()

il.subscribe(print)

destination = il.FileDestination("data/")
source = Adservice(destination=destination)
dag = il.DAG(source)
partition = il.TimePartition(dt.date(2024, 1, 1))
result = dag.materialize(partition_or_window=partition)
print(result)
