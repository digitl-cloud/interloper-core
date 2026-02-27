import interloper as il
from interloper_assets.demo.source import DemoSource

io = il.FileIO("data/")
partitioning = il.TimePartitionConfig(column="date")

demo1 = DemoSource(name="demo1")
demo2 = DemoSource(name="demo2")
dag = il.DAG(demo1, demo2)
