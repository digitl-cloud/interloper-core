"""Example script demonstrating Docker backfiller with Docker runner (Docker in Docker)."""

import datetime as dt
import os

import dotenv
import interloper as il
from interloper_assets import adup
from interloper_docker.backfiller import DockerBackfiller
from interloper_docker.runner import DockerRunner

dotenv.load_dotenv()


def on_event(event: il.Event) -> None:
    print(event)


data_path = os.path.abspath("./data")
partitioning = il.TimePartitionConfig(column="date")
io = il.FileIO("/tmp/data")
dag = il.DAG(adup(io=io))
window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))

# NOTE: In DinD, child containers created via the Docker socket see volumes from the host's perspective,
# not the parent container's filesystem.
runner = DockerRunner(
    image="interloper",
    volumes=[f"{data_path}:/tmp/data:rw"],
)

with DockerBackfiller(
    image="interloper",
    # dind=True,
    # runner=runner,
    on_event=on_event,
) as backfiller:
    result = backfiller.backfill(dag, window)
print(result)
