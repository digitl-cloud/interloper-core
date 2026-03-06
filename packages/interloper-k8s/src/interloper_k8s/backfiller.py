"""Kubernetes Backfiller implementation for Interloper.

This backfiller starts Kubernetes Jobs and invokes the Interloper CLI inside them
using an inline JSON config. Each partition/window runs as a separate Job, with
asset scheduling delegated to the configured runner in the inline config.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from time import sleep
from typing import Any, cast

from interloper.backfillers.base import Backfiller
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.errors import PartitionError, RunnerError
from interloper.events.base import Event, EventBus, parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.serialization.backfiller import BackfillerInstanceSpec
from kubernetes import client, config, watch
from kubernetes.client import V1Job


class KubernetesBackfiller(Backfiller[str]):
    """Run Interloper DAG partitions as individual Kubernetes Jobs.

    Each partition/window is executed in its own Job. The image must contain
    the `interloper` package (CLI available on PATH).
    """

    def __init__(
        self,
        image: str,
        namespace: str = "default",
        max_jobs: int = 4,
        env_vars: dict[str, str] | None = None,
        service_account: str | None = None,
        image_pull_policy: str | None = None,
        image_pull_secrets: list[str] | None = None,
        resources: dict[str, dict[str, str]] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[dict[str, Any]] | None = None,
        ttl_seconds_after_finished: int = 300,
        runner: Runner | None = None,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        """Initialize the KubernetesBackfiller.

        Args:
            image: Container image to use for job execution.
            namespace: Kubernetes namespace to create jobs in.
            max_jobs: Maximum number of concurrent jobs.
            env_vars: Environment variables to set in the container.
            service_account: Service account name to use for the job.
            image_pull_policy: Image pull policy ("Always", "IfNotPresent", or "Never").
            image_pull_secrets: List of image pull secret names.
            resources: Resource requests/limits dict with 'requests' and 'limits' keys.
            node_selector: Node selector labels for pod scheduling.
            tolerations: List of toleration dicts for pod scheduling.
            ttl_seconds_after_finished: TTL for completed jobs cleanup.
            runner: Runner to use for running assets inside the container.
            on_event: Optional event handler for lifecycle events.
        """
        super().__init__(runner=runner, on_event=on_event)

        # Force the runner to re-raise exceptions to propagate container exit codes.
        self.runner._reraise = True

        self._image = image
        self._namespace = namespace
        self._max_jobs = max_jobs
        self._env_vars = env_vars or {}
        self._service_account = service_account
        self._image_pull_policy = image_pull_policy
        self._image_pull_secrets = image_pull_secrets or []
        self._resources = resources
        self._node_selector = node_selector
        self._tolerations = tolerations or []
        self._ttl_seconds_after_finished = ttl_seconds_after_finished

        self._batch_v1: client.BatchV1Api | None = None
        self._core_v1: client.CoreV1Api | None = None

        # Track log streaming threads for cleanup
        self._log_threads: dict[str, threading.Thread] = {}
        self._stop_log_streaming = threading.Event()

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent jobs."""
        return self._max_jobs

    def _on_start(self) -> None:
        """Initialize Kubernetes client."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            try:
                config.load_kube_config()
            except Exception as e:
                raise RunnerError(f"Failed to load Kubernetes config: {e}") from e

        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()
        self._stop_log_streaming.clear()

    def _on_end(self) -> None:
        """Clean up log streaming threads."""
        # Signal all log streaming threads to stop
        self._stop_log_streaming.set()

        # Wait for threads to finish
        for thread in self._log_threads.values():
            thread.join(timeout=2.0)
        self._log_threads.clear()

    def _build_command(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        backfill_id: str,
    ) -> list[str]:
        """Build the CLI command for a partition.

        Args:
            dag: The DAG to execute
            partition_or_window: The partition or window
            backfill_id: The backfill ID

        Returns:
            Command list for the container
        """
        cfg = Config(dag=dag, runner=self.runner)

        cmd = [
            "interloper",
            "run",
            "--format=inline",
            f"--backfill-id={backfill_id}",
            cfg.to_json(),
        ]

        if partition_or_window is None:
            return cmd

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
        env_vars = [client.V1EnvVar(name=k, value=v) for k, v in self._env_vars.items()]
        # Enable log-based event streaming
        env_vars.append(client.V1EnvVar(name="INTERLOPER_EVENTS_TO_STDERR", value="true"))
        return env_vars

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build the resource requirements for the container."""
        if not self._resources:
            return None
        return client.V1ResourceRequirements(
            requests=self._resources.get("requests"),
            limits=self._resources.get("limits"),
        )

    def _build_tolerations(self) -> list[client.V1Toleration]:
        """Build tolerations for pod scheduling."""
        return [
            client.V1Toleration(
                key=t.get("key"),
                operator=t.get("operator", "Equal"),
                value=t.get("value"),
                effect=t.get("effect"),
            )
            for t in self._tolerations
        ]

    def _build_job_name(self, partition_or_window: Partition | PartitionWindow | None) -> str:
        """Build the name for the Kubernetes job."""
        name = f"interloper-backfill-{self.state.backfill_id[:8]}"
        if partition_or_window is not None:
            name = f"{name}-{partition_or_window.id}"
        return name[:63].replace(":", "-").replace("_", "-").lower()

    def _build_labels(self, partition_or_window: Partition | PartitionWindow | None) -> dict[str, str]:
        """Build the labels for the Kubernetes job."""
        return {
            "interloper.backfill_id": self.state.backfill_id[:8],
        }

    def _build_annotations(self, partition_or_window: Partition | PartitionWindow | None) -> dict[str, str]:
        """Build the annotations for the Kubernetes job."""
        annotations = {}
        if partition_or_window is not None:
            annotations["interloper.partition"] = partition_or_window.id
        return annotations

    def _start_log_streaming(self, job_name: str) -> None:
        """Start a background thread to stream logs and parse events from a job's pod.

        Args:
            job_name: The Kubernetes job name to stream logs from
        """
        assert self._core_v1 is not None
        # Capture reference for use in closure
        core_v1 = self._core_v1

        def stream_logs() -> None:
            try:
                # Wait for pod to be created and running
                pod_name: str | None = None
                while not self._stop_log_streaming.is_set():
                    try:
                        pods = core_v1.list_namespaced_pod(
                            namespace=self._namespace,
                            label_selector=f"job-name={job_name}",
                        )
                        if pods.items:
                            pod = pods.items[0]
                            if pod.metadata and pod.metadata.name:
                                pod_name = pod.metadata.name
                                # Check if pod is ready for log streaming
                                if pod.status and pod.status.phase in ("Running", "Succeeded", "Failed"):
                                    break
                    except Exception:
                        pass
                    sleep(0.5)

                if pod_name is None or self._stop_log_streaming.is_set():
                    return

                # Stream logs from the pod
                w = watch.Watch()
                try:
                    for line in w.stream(
                        core_v1.read_namespaced_pod_log,
                        name=pod_name,
                        namespace=self._namespace,
                        follow=True,
                    ):
                        if self._stop_log_streaming.is_set():
                            break

                        try:
                            # watch.Watch.stream() returns strings for log streaming
                            if isinstance(line, str):
                                event = parse_event_from_log_line(line)
                                if event is not None:
                                    EventBus.get_instance().emit(event)
                        except Exception:
                            # Ignore parsing errors, continue streaming
                            pass
                except Exception:
                    # Pod may have been removed or completed
                    pass
                finally:
                    w.stop()
            except Exception:
                # Job or pod may have been removed
                pass

        thread = threading.Thread(target=stream_logs, daemon=True)
        thread.start()
        self._log_threads[job_name] = thread

    def _stop_job_log_streaming(self, job_name: str) -> None:
        """Stop and clean up the log streaming thread for a job.

        Args:
            job_name: The Kubernetes job name to stop streaming for
        """
        thread = self._log_threads.pop(job_name, None)
        if thread is not None:
            # Thread will stop on next iteration due to stop flag or pod completion
            thread.join(timeout=1.0)

    def _submit_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> str:
        """Submit execution of a run as a Kubernetes Job.

        Args:
            dag: The DAG to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The job name for tracking
        """
        cmd = self._build_command(dag, partition_or_window, self.state.backfill_id)
        job_name = self._build_job_name(partition_or_window)
        env = self._build_env()
        resources = self._build_resources()
        tolerations = self._build_tolerations()
        labels = self._build_labels(partition_or_window)
        annotations = self._build_annotations(partition_or_window)

        container = client.V1Container(
            name="interloper",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            command=cmd[:1],
            args=cmd[1:],
            env=env if env else None,
            resources=resources,
        )

        pod_spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            service_account_name=self._service_account,
            node_selector=self._node_selector if self._node_selector else None,
            tolerations=tolerations if tolerations else None,
            image_pull_secrets=[client.V1LocalObjectReference(name=s) for s in self._image_pull_secrets]
            if self._image_pull_secrets
            else None,
        )

        job_spec = client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels=labels,
                    annotations=annotations,
                ),
                spec=pod_spec,
            ),
            backoff_limit=0,
            ttl_seconds_after_finished=self._ttl_seconds_after_finished,
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels=labels,
                annotations=annotations,
            ),
            spec=job_spec,
        )

        self.state.mark_run_running(partition_or_window)

        assert self._batch_v1 is not None
        self._batch_v1.create_namespaced_job(namespace=self._namespace, body=job)

        # Start log streaming for event collection
        self._start_log_streaming(job_name)

        return job_name

    def _wait_any(self, handles: list[str]) -> str:
        """Wait for any job to finish by polling.

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
                    self._batch_v1.read_namespaced_job_status(name=job_name, namespace=self._namespace),
                )

                assert updated_job.status is not None
                status = updated_job.status
                is_complete = status.succeeded is not None and status.succeeded > 0
                is_failed = status.failed is not None and status.failed > 0

                if is_complete or is_failed:
                    # Stop log streaming for this job
                    self._stop_job_log_streaming(job_name)

                    # Get partition ID from annotations
                    assert updated_job.metadata is not None and updated_job.metadata.annotations is not None
                    partition_id = updated_job.metadata.annotations.get("interloper.partition")
                    partition: Partition | PartitionWindow | None = None

                    # Find the matching partition from state
                    if partition_id is not None:
                        for p in self.state.partitions:
                            if p is not None and p.id == partition_id:
                                partition = p
                                break
                        else:
                            raise PartitionError(f"Partition {partition} not found in state")

                    if is_complete:
                        # TODO: This is not the true RunResult, we need to get it from the container?
                        #       Missing the asset_executions.
                        result = RunResult(partition, ExecutionStatus.COMPLETED)
                        self.state.mark_run_completed(partition, result)
                    else:
                        error_msg = f"Job {job_name} failed"
                        self.state.mark_run_failed(partition, error_msg)

                        # Try to get pod logs for debugging
                        try:
                            pods = self._core_v1.list_namespaced_pod(
                                namespace=self._namespace,
                                label_selector=f"job-name={job_name}",
                            )
                            if pods.items:
                                pod = pods.items[0]
                                assert pod.metadata is not None and pod.metadata.name is not None
                                logs = self._core_v1.read_namespaced_pod_log(
                                    name=pod.metadata.name,
                                    namespace=self._namespace,
                                )
                                if logs:
                                    print("=============== START OF RUN JOB LOGS ==================")
                                    print(logs)
                                    print("================ END OF RUN JOB LOGS ===================")
                        except Exception:
                            pass

                    return job_name

            sleep(1.0)

    def _cancel_all(self, handles: list[str]) -> None:
        """Cancel all running jobs.

        Args:
            handles: List of job names to cancel
        """
        assert self._batch_v1 is not None

        for job_name in handles:
            # Stop log streaming for this job
            self._stop_job_log_streaming(job_name)

            job: V1Job | None = None
            try:
                # Get job to retrieve partition from annotations
                job = cast(
                    V1Job,
                    self._batch_v1.read_namespaced_job(name=job_name, namespace=self._namespace),
                )
                self._batch_v1.delete_namespaced_job(
                    name=job_name,
                    namespace=self._namespace,
                    body=client.V1DeleteOptions(propagation_policy="Background"),
                )
            except Exception:
                pass
            finally:
                if job is not None:
                    try:
                        assert job.metadata is not None and job.metadata.annotations is not None
                        partition_str = job.metadata.annotations.get("interloper.partition", "")

                        # Find the matching partition from state
                        for p in self.state.partitions:
                            if str(p) == partition_str or (p is None and partition_str == ""):
                                self.state.mark_run_cancelled(p)
                                break
                    except Exception:
                        pass

    def to_spec(self) -> BackfillerInstanceSpec:
        """Convert to serializable spec."""
        return BackfillerInstanceSpec(
            path=self.path,
            init=dict(
                image=self._image,
                namespace=self._namespace,
                max_jobs=self._max_jobs,
                env_vars=self._env_vars,
                service_account=self._service_account,
                image_pull_policy=self._image_pull_policy,
                image_pull_secrets=self._image_pull_secrets,
                resources=self._resources,
                node_selector=self._node_selector,
                tolerations=self._tolerations,
                ttl_seconds_after_finished=self._ttl_seconds_after_finished,
            ),
        )
