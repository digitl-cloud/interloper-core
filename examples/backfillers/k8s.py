"""Example script demonstrating Kubernetes backfiller."""

import datetime as dt

import dotenv
import interloper as il
from interloper_assets import adup
from interloper_k8s.backfiller import KubernetesBackfiller

dotenv.load_dotenv()


def on_event(event: il.Event) -> None:
    print(event)


dag = il.DAG(adup())
window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))

with KubernetesBackfiller(
    image="interloper",
    image_pull_policy="Never",  # when running locally
    namespace="interloper",
    on_event=on_event,
) as backfiller:
    result = backfiller.backfill(dag, window)
print(result)
