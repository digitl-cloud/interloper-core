import interloper as il
from interloper_assets.demo.source import DemoSource


def on_event(event: il.Event) -> None:
    print(event)


il.subscribe(on_event)

demo = DemoSource()
result = il.DAG(demo).materialize()
