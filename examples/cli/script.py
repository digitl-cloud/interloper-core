import interloper as il
from interloper_assets.demo.source import DemoSource

io = il.FileIO("data/")
partitioning = il.TimePartitionConfig(column="date")

demo = DemoSource()
dag = il.DAG(demo)
