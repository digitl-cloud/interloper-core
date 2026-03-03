from typing import Any

import interloper as il
from interloper_sql import PostgresIO

il.subscribe(print)

io = PostgresIO(
    host="localhost",
    database="interloper",
    user="postgres",
    password="postgres",
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
