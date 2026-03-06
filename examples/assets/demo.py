import datetime as dt

import interloper as il
from interloper_assets.demo.source import DemoSource

il.subscribe(print)

demo = DemoSource()
dag = il.DAG(demo)

with il.MultiThreadRunner() as runner:
    result = runner.run(dag, il.TimePartition(dt.date(2025, 1, 1)))

print(result)
