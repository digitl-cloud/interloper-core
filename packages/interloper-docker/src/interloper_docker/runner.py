"""Docker-based runner that runs each asset in its own container.

Each submitted asset is executed inside a fresh container. To allow an asset
to resolve its upstream dependencies from IO without recomputing them, we pass
to the container a mini-DAG consisting of the target asset plus all its
upstream ancestors. The container runs the Interloper CLI with an inline
config, similar to the `DockerBackfiller`.
"""

from __future__ import annotations

import threading
from time import sleep
from typing import Any

import docker
from docker.models.containers import Container
from interloper.assets.base import Asset
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.errors import PartitionError, RunnerError
from interloper.events.base import EventBus, EventType, parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.utils.text import to_slug_case
from pydantic import Field, PrivateAttr

# Lifecycle events managed by the runner itself — must not be re-emitted from
# container logs to avoid duplicate state updates.
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


class DockerRunner(Runner[Container]):
    """Execute assets as individual Docker containers.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    `materializable=False` prior to execution to avoid recomputation while
    still enabling IO-based dependency resolution.
    """

    image: str
    max_containers: int = 4
    env_vars: dict[str, str] = Field(default_factory=dict)
    volumes: dict[str, dict[str, str]] | list[str] = Field(default_factory=dict)
    fail_fast: bool = False
    reraise: bool = False

    _docker: Any = PrivateAttr()
    _log_threads: dict[str, threading.Thread] = PrivateAttr(default_factory=dict)
    _stop_log_streaming: threading.Event = PrivateAttr(default_factory=threading.Event)

    def model_post_init(self, __context: Any, /) -> None:
        """Initialize Docker client after model initialization."""
        super().model_post_init(__context)
        self._docker = docker.from_env()

    def _on_start(self) -> None:
        self._stop_log_streaming.clear()

    def _on_end(self) -> None:
        # Signal all log streaming threads to stop
        self._stop_log_streaming.set()

        # Wait for threads to finish
        for thread in self._log_threads.values():
            thread.join(timeout=2.0)
        self._log_threads.clear()

    @property
    def _capacity(self) -> int:
        return self.max_containers

    def _build_command(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        run_id: str,
    ) -> list[str]:
        """Build the CLI command for asset execution in a container.

        Args:
            dag: The DAG to execute.
            partition_or_window: The partition or window.
            run_id: The run ID.

        Returns:
            Command list for the container.
        """
        config = Config(dag=dag)

        cmd = [
            "interloper",
            "run",
            "--format",
            "inline",
            f"--run-id={run_id}",
            config.to_json(),
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

    def _build_env(self) -> dict[str, str]:
        """Build the environment variables for the container."""
        env = dict(self.env_vars)
        # Enable log-based event streaming
        env["INTERLOPER_EVENTS_TO_STDERR"] = "true"
        return env

    def _build_volumes(self) -> dict[str, dict[str, str]]:
        """Build the volume mounts for the container."""
        volumes: dict[str, dict[str, str]] = {}
        if isinstance(self.volumes, dict):
            volumes.update(self.volumes)
        elif isinstance(self.volumes, list):
            for volume in self.volumes:
                volumes[volume.split(":")[0]] = {"bind": volume.split(":")[1], "mode": "rw"}
        return volumes

    def _build_name(self, asset: Asset) -> str:
        """Build the name for the container."""
        return f"interloper_run_{self.state.run_id[:8]}_{to_slug_case(asset.qualified_key)}"

    def _start_log_streaming(self, container: Container) -> None:
        """Start a background thread to stream logs and parse events from a container.

        Args:
            container: The Docker container to stream logs from
        """

        def stream_logs() -> None:
            try:
                for log_line in container.logs(stream=True, follow=True, stdout=False, stderr=True):
                    if self._stop_log_streaming.is_set():
                        break

                    try:
                        line = log_line.decode("utf-8", errors="ignore")
                        event = parse_event_from_log_line(line)
                        if event is not None and event.type not in _LIFECYCLE_EVENTS:
                            EventBus.get_instance().emit(event)
                    except Exception:
                        pass
            except Exception:
                # Container may have been removed or stopped
                pass

        thread = threading.Thread(target=stream_logs, daemon=True)
        thread.start()
        if container.id is not None:
            self._log_threads[container.id] = thread

    def _stop_container_log_streaming(self, container: Container) -> None:
        """Stop and clean up the log streaming thread for a container.

        Args:
            container: The Docker container to stop streaming for
        """
        if container.id is None:
            return
        thread = self._log_threads.pop(container.id, None)
        if thread is not None:
            thread.join(timeout=1.0)

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Container:
        """Submit execution of an asset and return the container object for completion tracking.

        IMPORTANT: this method is not calling the `_execute_asset` method of the base class.
        Therefore, the state has to be updated manually here and in `_wait_any` below.

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The container object for the asset execution
        """
        # Build a mini-DAG: target asset + its parents (non-materializable)
        mini_dag = self.state.dag.mini_dag(asset.qualified_key)

        cmd = self._build_command(mini_dag, partition_or_window, self.state.run_id)
        name = self._build_name(asset)
        env = self._build_env()
        volumes = self._build_volumes()

        self.state.mark_asset_running(asset)

        container = self._docker.containers.run(
            image=self.image,
            name=name,
            command=cmd,
            environment=env,
            volumes=volumes if volumes else None,
            labels={"interloper.asset_key": asset.qualified_key},
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )

        self._start_log_streaming(container)

        return container

    def _wait_any(self, handles: list[Container]) -> Container:
        """Wait for any container to finish by polling.

        IMPORTANT: the `_execute_asset` method of the base class is not called by `_submit_asset`.
        Therefore, the state has to be updated manually here and in `_submit_asset` above.

        Args:
            handles: List of container objects to wait for

        Returns:
            The container object that finished
        """

        while True:
            for container in handles:
                container.reload()

                if container.status in ("exited", "dead"):
                    self._stop_container_log_streaming(container)

                    result = container.wait()
                    status_code = result.get("StatusCode", 1)

                    # Map back to asset
                    asset: Asset | None = None
                    asset_key = container.labels.get("interloper.asset_key")
                    if asset_key and asset_key in self.state.dag.asset_map:
                        asset = self.state.dag.asset_map[asset_key]
                    if asset is None:
                        raise RunnerError("Failed to map container to asset")

                    if status_code == 0:
                        self.state.mark_asset_completed(asset)
                    else:
                        self.state.mark_asset_failed(asset, f"Container {container.id} exited with code {status_code}")

                        try:
                            logs = container.logs(stdout=True, stderr=True)
                            if logs:
                                print("=============== START OF ASSET CONTAINER LOGS ================")
                                print(logs.decode("utf-8", errors="ignore"))
                                print("================ END OF ASSET CONTAINER LOGS =================")

                        except Exception:
                            pass

                        if self.reraise or self.fail_fast:
                            raise RunnerError(f"Container {container.id} exited with code {status_code}")

                    # Remove the container after processing
                    try:
                        container.remove()
                    except Exception as e:
                        print(f"Error removing container {container.id}: {e}")
                        pass

                    return container

            sleep(1.0)

    def _cancel_all(self, handles: list[Container]) -> None:
        for container in handles:
            self._stop_container_log_streaming(container)

            try:
                container.stop(timeout=2)
            except Exception:
                try:
                    container.kill()
                except Exception:
                    pass
            finally:
                asset_key = container.labels.get("interloper.asset_key")
                asset = self.state.dag.asset_map[asset_key]
                self.state.mark_asset_canceled(asset)
