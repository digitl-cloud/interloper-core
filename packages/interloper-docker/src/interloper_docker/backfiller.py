"""Docker Backfiller implementation for Interloper.

This backfiller starts a Docker container and invokes the Interloper CLI inside it
using an inline JSON config. It runs the entire DAG in the container, delegating
asset scheduling to the configured backfiller in the inline config (typically in_process).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from time import sleep

import docker
from docker.errors import NotFound
from docker.models.containers import Container
from interloper.backfillers.base import Backfiller
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.events.base import Event, emit, parse_event_from_log_line
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.runners.results import ExecutionStatus, RunResult
from interloper.serialization.backfiller import BackfillerSpec


class DockerBackfiller(Backfiller[Container]):
    """Run an Interloper DAG inside a Docker container via the Interloper CLI.

    The image must contain the `interloper` package (CLI available on PATH).
    """

    def __init__(
        self,
        image: str,
        env_vars: dict[str, str] | None = None,
        max_containers: int = 1,
        runner: Runner | None = None,
        volumes: dict[str, dict[str, str]] | list[str] | None = None,
        dind: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        """Initialize the DockerBackfiller.

        Args:
            image: Docker image to use
            env_vars: Environment variables to pass to the container
            max_containers: Maximum number of concurrent containers (default 1)
            runner: Runner to use for running assets
            dind: If True, mount the Docker socket to enable Docker-in-Docker
        """
        super().__init__(runner=runner, on_event=on_event)

        # Force the runner to re-raise exceptions to make sure the container's exit code is propagated.
        self.runner._reraise = True

        self._image = image
        self._env_vars = env_vars or {}
        self._max_containers = max_containers
        self._volumes = volumes or {}
        self._dind = dind
        self._docker = docker.from_env()

        # Track log streaming threads for cleanup
        self._log_threads: dict[str, threading.Thread] = {}
        self._stop_log_streaming = threading.Event()

    @property
    def _capacity(self) -> int:
        """Maximum number of concurrent containers."""
        return self._max_containers

    def _on_start(self) -> None:
        self._stop_log_streaming.clear()

    def _on_end(self) -> None:
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

        Returns:
            Command list for the container
        """
        config = Config(dag=dag, runner=self.runner)

        cmd = [
            "interloper",
            "run",
            "--format=inline",
            f"--backfill-id={backfill_id}",
            config.to_json(),
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
            raise ValueError("Unsupported partition or window type")
        return cmd

    def _build_env(self) -> dict[str, str]:
        """Build the environment variables for the container."""
        env = dict(self._env_vars)
        # Enable log-based event streaming
        env["INTERLOPER_EVENTS_TO_STDERR"] = "true"
        return env

    def _build_volumes(self) -> dict[str, dict[str, str]]:
        """Build the volume mounts for the container."""
        volumes = {}
        if isinstance(self._volumes, dict):
            volumes.update(self._volumes)
        elif isinstance(self._volumes, list):
            for volume in self._volumes:
                volumes[volume.split(":")[0]] = {"bind": volume.split(":")[1], "mode": "rw"}
        if self._dind:
            volumes["/var/run/docker.sock"] = {"bind": "/var/run/docker.sock", "mode": "rw"}
        return volumes

    def _build_name(self, partition_or_window: Partition | PartitionWindow | None) -> str:
        """Build the name for the container."""
        name = f"interloper_backfill_{self.state.backfill_id[:8]}"
        if partition_or_window is not None:
            name += f"-{partition_or_window}"
        return name.replace(":", "-").replace("_", "-").lower()

    def _start_log_streaming(self, container: Container) -> None:
        """Start a background thread to stream logs and parse events from a container.

        Args:
            container: The Docker container to stream logs from
        """

        def stream_logs() -> None:
            try:
                # Stream logs from the container (both stdout and stderr)
                for log_line in container.logs(stream=True, follow=True, stdout=True, stderr=True):
                    if self._stop_log_streaming.is_set():
                        break

                    try:
                        line = log_line.decode("utf-8", errors="ignore")
                        event = parse_event_from_log_line(line)
                        if event is not None:
                            emit(event)
                    except Exception:
                        # Ignore parsing errors, continue streaming
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
            # Thread will stop on next iteration due to container exit
            thread.join(timeout=1.0)

    def _submit_run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Container:
        """Submit execution of a run in a Docker container.

        Args:
            dag: The DAG to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The container as the handle
        """
        cmd = self._build_command(dag, partition_or_window, self.state.backfill_id)
        env = self._build_env()
        volumes = self._build_volumes()
        name = self._build_name(partition_or_window)

        self.state.mark_run_running(partition_or_window)

        container = self._docker.containers.run(
            image=self._image,
            name=name,
            command=cmd,
            environment=env,
            volumes=volumes if volumes else None,
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )
        # Store partition in container object for _wait_any
        setattr(container, "_interloper_partition", partition_or_window)

        # Start log streaming for event collection
        self._start_log_streaming(container)

        return container

    def _wait_any(self, handles: list[Container]) -> Container:
        """Wait for any container to complete by polling.

        Args:
            handles: List of container objects to wait for

        Returns:
            The container that completed
        """
        while True:
            for container in handles:
                container.reload()

                if container.status in ("exited", "dead"):
                    # Stop log streaming for this container
                    self._stop_container_log_streaming(container)

                    result = container.wait()
                    status_code = result.get("StatusCode", 1)

                    # Get partition from container object
                    partition = getattr(container, "_interloper_partition", None)

                    if status_code == 0:
                        # TODO: This is not the true RunResult, we need to get it from the container?
                        #       Missing the asset_executions.
                        result = RunResult(partition, ExecutionStatus.COMPLETED)
                        self.state.mark_run_completed(partition, result)
                    else:
                        self.state.mark_run_failed(partition, f"Container exited with code {status_code}")

                        try:
                            logs = container.logs(stdout=True, stderr=True)
                            if logs:
                                print("=============== START OF RUN CONTAINER LOGS ==================")
                                print(logs.decode("utf-8", errors="ignore"))
                                print("================ END OF RUN CONTAINER LOGS ===================")
                        except Exception:
                            pass

                    # Remove the container after processing
                    try:
                        container.remove()
                    except Exception as e:
                        print(f"Error removing container {container.id}: {e}")
                        pass

                    return container

            sleep(1.0)

    def _cancel_all(self, handles: list[Container]) -> None:
        """Best-effort cancellation of outstanding containers.

        Args:
            handles: List of container objects to cancel
        """
        for container in handles:
            partition = getattr(container, "_interloper_partition", None)

            # Stop log streaming for this container
            self._stop_container_log_streaming(container)

            try:
                container.stop(timeout=5)
            except NotFound:
                # Container already removed, mark as cancelled
                if partition is not None:
                    self.state.mark_run_cancelled(partition)
            except Exception:
                try:
                    container.kill()
                except NotFound:
                    # Container already removed
                    self.state.mark_run_cancelled(partition)
                except Exception:
                    pass
            else:
                # Only mark as cancelled if we successfully stopped/killed
                if partition is not None:
                    self.state.mark_run_cancelled(partition)

    def to_spec(self) -> BackfillerSpec:
        """Convert to serializable spec."""
        return BackfillerSpec(
            path=self.path,
            init=dict(
                image=self._image,
                env_vars=self._env_vars,
                volumes=self._volumes,
                max_containers=self._max_containers,
                dind=self._dind,
            ),
        )
