"""Example script demonstrating DBOS runner with recovery."""

import datetime as dt
from pprint import pp

import interloper as il
from dbos import DBOS, DBOSConfig
from interloper_assets import example_source
from interloper_dbos import DBOSRunner

if __name__ == "__main__":
    config: DBOSConfig = {
        "name": "interloper",
        "system_database_url": "postgresql://postgres:postgres@localhost:54322/postgres",
    }
    DBOS(config=config)

    def on_event(event: il.Event) -> None:
        print(event)

    # Launching DBOS should happen after the runner class is instantiated
    DBOS.launch()

    partition = il.TimePartition(value=dt.date(2025, 1, 1))
    dag = il.DAG(example_source())

    with DBOSRunner(on_event=on_event) as runner:
        result = runner.run(dag, partition)

    assert runner.handle

    print("--------------------------------")
    print(runner.handle.workflow_id)
    print(runner.handle.get_status().status)
    print(result)
    print("--------------------------------")

    steps = runner.list_failed_steps()
    pp(steps)
