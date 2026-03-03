from typing import Any

import interloper as il
from interloper_google_cloud import BigQueryIO

il.subscribe(print)

io = BigQueryIO(
    project="dc-int-connectors-prd",
    default_dataset="interloper",
)


@il.asset(io=io)
def a() -> list[dict[str, Any]]:
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]


@il.asset(io=io)
def b(a: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"id": 3, "name": "Charlie"},
        {"id": 4, "name": "David"},
    ]


dag = il.DAG(a, b)
dag.materialize()
