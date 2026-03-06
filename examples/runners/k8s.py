"""Example script demonstrating Docker backfiller."""

import datetime as dt

import dotenv
import interloper as il
from interloper_assets import Adup
from interloper_k8s.runner import KubernetesRunner

dotenv.load_dotenv()


source = Adup()
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

with KubernetesRunner(
    on_event=print,
    image="interloper",
    image_pull_policy="Never",  # when running locally
    namespace="interloper",
) as runner:
    result = runner.run(dag, partition)
print(result)
