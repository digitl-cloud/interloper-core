"""Kubernetes-based runner that runs each asset in its own Job.

Each submitted asset is executed inside a Kubernetes Job. To allow an asset
to resolve its upstream dependencies from IO without recomputing them, we pass
to the container a mini-DAG consisting of the target asset plus all its
upstream ancestors. The container runs the Interloper CLI with an inline
config, similar to the `DockerRunner`.
"""

from __future__ import annotations

import threading
import time
from typing import Any, cast

from interloper.assets.base import Asset
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.errors import PartitionError, RunnerError
from interloper.events.base import EventBus, EventType, parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.utils.text import to_slug_case
from kubernetes import client, config
from kubernetes.client import V1Job
from pydantic import Field, PrivateAttr

# Lifecycle events managed by the runner itself — must not be re-emitted from
# pod logs to avoid duplicate state updates.
_LIFECYCLE_EVENTS = frozenset(
    {
        EventType.ASSET_STARTED,
        EventType.ASSET_COMPLETED,
        EventType.ASSET_FAILED,
        EventType.ASSET_CANCELED,
        EventType.RUN_STARTED,
        EventType.RUN_COMPLETED,
        EventType.RUN_FAILED,
    }
)


class KubernetesRunner(Runner[str]):
    """Execute assets as individual Kubernetes Jobs.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    ``materializable=False`` prior to execution to avoid recomputation while
    still enabling IO-based dependency resolution.
    """

    image: str
    namespace: str = "default"
    max_jobs: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    service_account_name: str | None = None
    image_pull_policy: str | None = None
    image_pull_secrets: list[str] = Field(default_factory=list)
    resources: dict[str, dict[str, str]] | None = None
    node_selector: dict[str, str] | None = None
    tolerations: list[dict[str, Any]] = Field(default_factory=list)
    poll_interval: float = 1.0
    ttl_seconds_after_finished: int = 300
    fail_fast: bool = False
    reraise: bool = False

    _batch_v1: client.BatchV1Api | None = PrivateAttr(default=None)
    _core_v1: client.CoreV1Api | None = PrivateAttr(default=None)
    _log_threads: dict[str, threading.Thread] = PrivateAttr(default_factory=dict)
    _stop_log_streaming: threading.Event = PrivateAttr(default_factory=threading.Event)

    def _on_start(self) -> None:
        """Initialize Kubernetes client."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()
        self._stop_log_streaming.clear()

    def _on_end(self) -> None:
        """Signal all log streaming threads to stop."""
        self._stop_log_streaming.set()
        for thread in self._log_threads.values():
            thread.join(timeout=2.0)
        self._log_threads.clear()

    @property
    def _capacity(self) -> int:
        return self.max_jobs

    def _build_command(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        run_id: str,
    ) -> list[str]:
        """Build the command to execute in the container."""
        cfg = Config(dag=dag)

        cmd = [
            "interloper",
            "run",
            "--format",
            "inline",
            f"--run-id={run_id}",
            cfg.to_json(),
        ]

        if isinstance(partition_or_window, TimePartition):
            cmd.extend(["--date", partition_or_window.value.strftime("%Y-%m-%d")])
        elif isinstance(partition_or_window, TimePartitionWindow):
            cmd.extend(
                [
                    "--start-date",
                    partition_or_window.start.strftime("%Y-%m-%d"),
                    "--end-date",
                    partition_or_window.end.strftime("%Y-%m-%d"),
                ]
            )
        else:
            raise PartitionError("Unsupported partition or window type")
        return cmd

    def _build_env(self) -> list[client.V1EnvVar]:
        """Build the environment variables for the container."""
        return [client.V1EnvVar(name=k, value=v) for k, v in self.env_vars.items()]

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build the resource requirements for the container."""
        if not self.resources:
            return None
        return client.V1ResourceRequirements(
            requests=self.resources.get("requests"),
            limits=self.resources.get("limits"),
        )

    def _build_job_name(self, asset: Asset) -> str:
        """Build the name for the Kubernetes job."""
        return f"interloper-run-{self.state.run_id[:8]}-{to_slug_case(asset.qualified_key)}"[:63]

    def _build_tolerations(self) -> list[client.V1Toleration]:
        """Build tolerations for pod scheduling."""
        return [
            client.V1Toleration(
                key=t.get("key"),
                operator=t.get("operator", "Equal"),
                value=t.get("value"),
                effect=t.get("effect"),
            )
            for t in self.tolerations
        ]

    def _start_log_streaming(self, job_name: str) -> None:
        """Start a background thread to stream pod logs and parse events from a K8s Job."""
        if self._core_v1 is None:
            return

        core_v1 = self._core_v1

        def stream_logs() -> None:
            # Wait for pod to be scheduled and running
            pod_name: str | None = None
            while not self._stop_log_streaming.is_set():
                try:
                    pods = core_v1.list_namespaced_pod(
                        namespace=self.namespace,
                        label_selector=f"job-name={job_name}",
                    )
                    if pods.items:
                        pod = pods.items[0]
                        if pod.status and pod.status.phase in ("Running", "Succeeded", "Failed"):
                            pod_name = pod.metadata.name
                            break
                except Exception:
                    pass
                time.sleep(0.5)

            if pod_name is None or self._stop_log_streaming.is_set():
                return

            try:
                log_stream = core_v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=self.namespace,
                    follow=True,
                    _preload_content=False,
                    container="interloper",
                )
                for line_bytes in log_stream:
                    if self._stop_log_streaming.is_set():
                        break
                    try:
                        line = line_bytes.decode("utf-8", errors="ignore")
                        event = parse_event_from_log_line(line)
                        if event is not None and event.type not in _LIFECYCLE_EVENTS:
                            EventBus.get_instance().emit(event)
                    except Exception:
                        pass
            except Exception:
                pass

        thread = threading.Thread(target=stream_logs, daemon=True)
        thread.start()
        self._log_threads[job_name] = thread

    def _stop_job_log_streaming(self, job_name: str) -> None:
        """Stop and clean up the log streaming thread for a job."""
        thread = self._log_threads.pop(job_name, None)
        if thread is not None:
            thread.join(timeout=1.0)

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> str:
        """Submit execution of an asset and return the job name for completion tracking.

        IMPORTANT: this method is not calling the ``_execute_asset`` method of the base class.
        Therefore, the state has to be updated manually here and in ``_wait_any`` below.

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The job name (string) for the asset execution
        """
        # Build a mini-DAG: target asset + its parents (non-materializable)
        mini_dag = self.state.dag.mini_dag(asset.qualified_key)

        cmd = self._build_command(mini_dag, partition_or_window, self.state.run_id)
        job_name = self._build_job_name(asset)
        env = self._build_env()
        # Enable log-based event streaming from child process logs.
        env.append(client.V1EnvVar(name="INTERLOPER_EVENTS_TO_STDERR", value="true"))
        resources = self._build_resources()
        tolerations = self._build_tolerations()

        # Build container spec
        container = client.V1Container(
            name="interloper",
            image=self.image,
            image_pull_policy=self.image_pull_policy,
            command=cmd[:1],
            args=cmd[1:],
            env=env if env else None,
            resources=resources,
        )

        # Build pod spec
        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self.service_account_name,
            node_selector=self.node_selector if self.node_selector else None,
            tolerations=tolerations if tolerations else None,
            image_pull_secrets=[client.V1LocalObjectReference(name=s) for s in self.image_pull_secrets]
            if self.image_pull_secrets
            else None,
        )

        # Build job spec
        job_spec = client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "interloper.asset_key": asset.qualified_key,
                        "interloper.run_id": self.state.run_id,
                    }
                ),
                spec=pod_spec,
            ),
            backoff_limit=0,
            ttl_seconds_after_finished=self.ttl_seconds_after_finished,
        )

        # Build job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self.namespace,
                labels={
                    "interloper.asset_key": asset.qualified_key,
                    "interloper.run_id": self.state.run_id,
                },
                annotations={
                    "interloper.asset_key": asset.qualified_key,
                },
            ),
            spec=job_spec,
        )

        self.state.mark_asset_running(asset)

        # Create the job in Kubernetes
        assert self._batch_v1 is not None
        self._batch_v1.create_namespaced_job(namespace=self.namespace, body=job)

        # Start streaming logs for event collection
        self._start_log_streaming(job_name)

        return job_name

    def _wait_any(self, handles: list[str]) -> str:
        """Wait for any job to finish by polling.

        IMPORTANT: the ``_execute_asset`` method of the base class is not called by ``_submit_asset``.
        Therefore, the state has to be updated manually here and in ``_submit_asset`` above.

        Args:
            handles: List of job names to wait for

        Returns:
            The job name that finished
        """
        assert self._batch_v1 is not None
        assert self._core_v1 is not None

        while True:
            for job_name in handles:
                # Refresh job status
                updated_job = cast(
                    V1Job,
                    self._batch_v1.read_namespaced_job_status(name=job_name, namespace=self.namespace),
                )

                assert updated_job.status is not None
                status = updated_job.status
                is_complete = status.succeeded is not None and status.succeeded > 0
                is_failed = status.failed is not None and status.failed > 0

                if is_complete or is_failed:
                    self._stop_job_log_streaming(job_name)

                    # Map back to asset
                    assert updated_job.metadata is not None and updated_job.metadata.annotations is not None
                    asset_key = updated_job.metadata.annotations.get("interloper.asset_key")
                    if asset_key is None or asset_key not in self.state.dag.asset_map:
                        raise RunnerError("Failed to map job to asset")
                    asset = self.state.dag.asset_map[asset_key]

                    if is_complete:
                        self.state.mark_asset_completed(asset)
                    else:
                        error_msg = f"Job {job_name} failed"

                        # Try to get pod logs for debugging
                        try:
                            pods = self._core_v1.list_namespaced_pod(
                                namespace=self.namespace,
                                label_selector=f"job-name={job_name}",
                            )
                            if pods.items:
                                pod = pods.items[0]
                                logs = self._core_v1.read_namespaced_pod_log(
                                    name=pod.metadata.name,
                                    namespace=self.namespace,
                                )
                                if logs:
                                    print("=============== START OF ASSET JOB LOGS ================")
                                    print(logs)
                                    print("================ END OF ASSET JOB LOGS =================")
                        except Exception:
                            pass

                        self.state.mark_asset_failed(asset, error_msg)

                        if self.reraise or self.fail_fast:
                            raise RunnerError(error_msg)

                    return job_name

            time.sleep(self.poll_interval)

    def _cancel_all(self, handles: list[str]) -> None:
        """Cancel all running jobs."""
        assert self._batch_v1 is not None

        for job_name in handles:
            self._stop_job_log_streaming(job_name)

            job: V1Job | None = None
            try:
                # Get job to retrieve asset key from annotations
                job = cast(
                    V1Job,
                    self._batch_v1.read_namespaced_job(name=job_name, namespace=self.namespace),
                )
                self._batch_v1.delete_namespaced_job(
                    name=job_name,
                    namespace=self.namespace,
                    body=client.V1DeleteOptions(propagation_policy="Background"),
                )
            except Exception:
                pass
            finally:
                if job is not None:
                    try:
                        assert job.metadata is not None and job.metadata.annotations is not None
                        asset_key = job.metadata.annotations.get("interloper.asset_key")
                        if asset_key and asset_key in self.state.dag.asset_map:
                            asset = self.state.dag.asset_map[asset_key]
                            self.state.mark_asset_canceled(asset)
                    except Exception:
                        pass
