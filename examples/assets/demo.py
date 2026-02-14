import interloper as il
from interloper_assets.demo.source import DemoSource

demo = DemoSource()
result = il.DAG(demo).materialize()
