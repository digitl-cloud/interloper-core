import interloper as il
from interloper_assets import demo_source


def on_event(event: il.Event) -> None:
    print(event.to_json())


il.subscribe(on_event)

io = il.FileIO("data/")
source1 = demo_source(io=io)
dag = il.DAG(source1)
result = dag.materialize()
print(result)
