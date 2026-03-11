import interloper as il
from interloper_assets.demo.source import DemoSource

destination = il.FileDestination("data/")
partitioning = il.TimePartitionConfig(column="date")

demo1 = DemoSource(key="demo1")
demo2 = DemoSource(key="demo2")
dag = il.DAG(demo1, demo2)

# Test with:
# uv run interloper run examples/cli/script.py --date 2026-01-01
# uv run interloper backfill examples/cli/script.py --start-date 2026-01-01 --end-date 2026-01-07
