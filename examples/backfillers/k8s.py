"""Example script demonstrating Kubernetes backfiller."""

import datetime as dt

import dotenv
import interloper as il
from interloper_assets import Adup
from interloper_k8s.backfiller import KubernetesBackfiller

dotenv.load_dotenv()


dag = il.DAG(Adup())
window = il.TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 2))

with KubernetesBackfiller(
    on_event=print,
    image="interloper",
    image_pull_policy="Never",  # when running locally
    namespace="interloper",
) as backfiller:
    result = backfiller.backfill(dag, window)
print(result)
