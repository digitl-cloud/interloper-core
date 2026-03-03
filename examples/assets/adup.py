import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import Adup

load_dotenv()

il.subscribe(print)

partition = il.TimePartition(dt.date(2024, 1, 1))

adup = Adup()
dag = il.DAG(adup)
dag.materialize(partition_or_window=partition)

# print(adup.account.run())
