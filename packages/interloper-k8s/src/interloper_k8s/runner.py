"""Kubernetes-based runner that runs each asset in its own Job.

Each submitted asset is executed inside a Kubernetes Job. To allow an asset
to resolve its upstream dependencies from IO without recomputing them, we pass
to the container a mini-DAG consisting of the target asset plus all its
upstream ancestors. The container runs the Interloper CLI with an inline
config, similar to the `DockerRunner`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast

from interloper.assets.base import Asset
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.errors import PartitionError, RunnerError
from interloper.events.base import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.runner import RunnerSpec
from kubernetes import client, config
from kubernetes.client import V1Job


class KubernetesRunner(Runner[str]):
    """Execute assets as individual Kubernetes Jobs.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    `materializable=False` prior to execution to avoid recomputation while
    still enabling IO-based dependency resolution.
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
        poll_interval: float = 1.0,
        ttl_seconds_after_finished: int = 300,
        fail_fast: bool = False,
        reraise: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        """Initialize the KubernetesRunner.

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
            poll_interval: Interval in seconds between job status polls.
            ttl_seconds_after_finished: TTL for completed jobs cleanup.
            fail_fast: Stop execution on first failure.
            reraise: Re-raise exceptions.
            on_event: Optional event handler for lifecycle events.
        """
        super().__init__(fail_fast=fail_fast, reraise=reraise, on_event=on_event)
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
        self._poll_interval = poll_interval
        self._ttl_seconds_after_finished = ttl_seconds_after_finished

        self._batch_v1: client.BatchV1Api | None = None
        self._core_v1: client.CoreV1Api | None = None

    def _on_start(self) -> None:
        """Initialize Kubernetes client."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()

    @property
    def _capacity(self) -> int:
        return self._max_jobs

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
        return [client.V1EnvVar(name=k, value=v) for k, v in self._env_vars.items()]

    def _build_resources(self) -> client.V1ResourceRequirements | None:
        """Build the resource requirements for the container."""
        if not self._resources:
            return None
        return client.V1ResourceRequirements(
            requests=self._resources.get("requests"),
            limits=self._resources.get("limits"),
        )

    def _build_job_name(self, asset: Asset) -> str:
        """Build the name for the Kubernetes job."""
        # K8s names must be lowercase, alphanumeric, and can contain hyphens
        safe_key = asset.instance_key.replace(".", "-").replace("_", "-").lower()
        return f"interloper-{self.state.run_id[:8]}-{safe_key}"[:63]

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

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> str:
        """Submit execution of an asset and return the job name for completion tracking.

        IMPORTANT: this method is not calling the `_execute_asset` method of the base class.
        Therefore, the state has to be updated manually here and in `_wait_any` below.

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The job name (string) for the asset execution
        """
        # Build a mini-DAG: target asset + its parents (non-materializable)
        mini_dag = self.state.dag.mini_dag(asset.instance_key)

        cmd = self._build_command(mini_dag, partition_or_window, self.state.run_id)
        job_name = self._build_job_name(asset)
        env = self._build_env()
        resources = self._build_resources()
        tolerations = self._build_tolerations()

        # Build container spec
        container = client.V1Container(
            name="interloper",
            image=self._image,
            image_pull_policy=self._image_pull_policy,
            command=cmd[:1],
            args=cmd[1:],
            env=env if env else None,
            resources=resources,
        )

        # Build pod spec
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

        # Build job spec
        job_spec = client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "interloper.asset_key": asset.instance_key.replace(".", "-").lower(),
                        "interloper.run_id": self.state.run_id[:8],
                    }
                ),
                spec=pod_spec,
            ),
            backoff_limit=0,
            ttl_seconds_after_finished=self._ttl_seconds_after_finished,
        )

        # Build job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=self._namespace,
                labels={
                    "interloper.asset_key": asset.instance_key.replace(".", "-").lower(),
                    "interloper.run_id": self.state.run_id[:8],
                },
                annotations={
                    "interloper.asset_key": asset.instance_key,
                },
            ),
            spec=job_spec,
        )

        self.state.mark_asset_running(asset)

        # Create the job in Kubernetes
        assert self._batch_v1 is not None
        self._batch_v1.create_namespaced_job(namespace=self._namespace, body=job)

        return job_name

    def _wait_any(self, handles: list[str]) -> str:
        """Wait for any job to finish by polling.

        IMPORTANT: the `_execute_asset` method of the base class is not called by `_submit_asset`.
        Therefore, the state has to be updated manually here and in `_submit_asset` above.

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
                                namespace=self._namespace,
                                label_selector=f"job-name={job_name}",
                            )
                            if pods.items:
                                pod = pods.items[0]
                                logs = self._core_v1.read_namespaced_pod_log(
                                    name=pod.metadata.name,
                                    namespace=self._namespace,
                                )
                                if logs:
                                    print("=============== START OF ASSET JOB LOGS ================")
                                    print(logs)
                                    print("================ END OF ASSET JOB LOGS =================")
                        except Exception:
                            pass

                        self.state.mark_asset_failed(asset, error_msg)

                        if self._reraise or self._fail_fast:
                            raise RunnerError(error_msg)

                    return job_name

            time.sleep(self._poll_interval)

    def _cancel_all(self, handles: list[str]) -> None:
        """Cancel all running jobs."""
        assert self._batch_v1 is not None

        for job_name in handles:
            job: V1Job | None = None
            try:
                # Get job to retrieve asset key from annotations
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
                        asset_key = job.metadata.annotations.get("interloper.asset_key")
                        if asset_key and asset_key in self.state.dag.asset_map:
                            asset = self.state.dag.asset_map[asset_key]
                            self.state.mark_asset_cancelled(asset)
                    except Exception:
                        pass

    def to_spec(self) -> RunnerSpec:
        return RunnerSpec(
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
                poll_interval=self._poll_interval,
                ttl_seconds_after_finished=self._ttl_seconds_after_finished,
                fail_fast=self._fail_fast,
                reraise=self._reraise,
            ),
        )
