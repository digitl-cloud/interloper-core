"""Example script demonstrating Docker backfiller."""

import datetime as dt
import os

import dotenv
import interloper as il
from interloper_assets import demo_source
from interloper_docker.runner import DockerRunner

dotenv.load_dotenv()


def on_event(event: il.Event) -> None:
    print(event)


io = il.FileIO("/tmp/data")
source = demo_source(io=io)
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

with DockerRunner(
    image="interloper",
    on_event=on_event,
    volumes=[f"{os.path.abspath('./data')}:/tmp/data:rw"],
) as runner:
    result = runner.run(dag, partition)
print(result)
