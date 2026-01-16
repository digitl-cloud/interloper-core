"""Example script demonstrating Docker backfiller."""

import datetime as dt

import dotenv
import interloper as il
from interloper_assets import adup
from interloper_k8s.runner import KubernetesRunner

dotenv.load_dotenv()


def on_event(event: il.Event) -> None:
    print(event)


source = adup()
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

with KubernetesRunner(
    image="interloper",
    image_pull_policy="Never",  # when running locally
    on_event=on_event,
    namespace="interloper",
) as runner:
    result = runner.run(dag, partition)
print(result)
