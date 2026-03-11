from typing import Any

import interloper as il
from interloper_google_cloud import BigQueryDestination
from interloper_pandas import DataFrameAdapter

il.subscribe(print)

destination = BigQueryDestination(
    project="dc-int-connectors-prd",
    adapter=DataFrameAdapter(),
    default_dataset="interloper",
)


@il.asset(destination=destination)
def a() -> list[dict[str, Any]]:
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]


@il.asset(destination=destination)
def b(a: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"id": 3, "name": "Charlie"},
        {"id": 4, "name": "David"},
    ]


dag = il.DAG(a, b)
dag.materialize()
