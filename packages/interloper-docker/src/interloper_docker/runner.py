"""Docker-based runner that runs each asset in its own container.

Each submitted asset is executed inside a fresh container. To allow an asset
to resolve its upstream dependencies from IO without recomputing them, we pass
to the container a mini-DAG consisting of the target asset plus all its
upstream ancestors. The container runs the Interloper CLI with an inline
config, similar to the `DockerBackfiller`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import docker
from docker.models.containers import Container
from interloper.assets.base import Asset
from interloper.cli.config import Config
from interloper.dag.base import DAG
from interloper.events.base import Event
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.base import Runner
from interloper.serialization.runner import RunnerSpec


class DockerRunner(Runner):
    """Execute assets as individual Docker containers.

    For each asset, constructs a mini-DAG comprising the asset and all its
    upstream ancestors. The mini-DAG is sent to the container via inline JSON.
    Inside the container, all non-target assets are marked as
    `materializable=False` prior to execution to avoid recomputation while
    still enabling IO-based dependency resolution.
    """

    def __init__(
        self,
        image: str,
        max_containers: int = 4,
        env_vars: dict[str, str] | None = None,
        volumes: dict[str, dict[str, str]] | list[str] | None = None,
        fail_fast: bool = False,
        reraise: bool = False,
        on_event: Callable[[Event], None] | None = None,
    ) -> None:
        super().__init__(fail_fast=fail_fast, reraise=reraise, on_event=on_event)
        self._image = image
        self._max_containers = max_containers
        self._env_vars = env_vars or {}
        self._volumes = volumes or {}
        self._docker = docker.from_env()

    @property
    def _capacity(self) -> int:
        return self._max_containers

    def _build_command(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None,
        run_id: str,
    ) -> list[str]:
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
            raise ValueError("Unsupported partition or window type")
        return cmd

    def _build_env(self) -> dict[str, str]:
        """Build the environment variables for the container."""
        return dict(self._env_vars)

    def _build_volumes(self) -> dict[str, dict[str, str]]:
        """Build the volume mounts for the container."""
        volumes = {}
        if isinstance(self._volumes, dict):
            volumes.update(self._volumes)
        elif isinstance(self._volumes, list):
            for volume in self._volumes:
                volumes[volume.split(":")[0]] = {"bind": volume.split(":")[1], "mode": "rw"}
        return volumes

    def _build_name(self, asset: Asset) -> str:
        """Build the name for the container."""
        name = f"interloper_run_{self.state.run_id[:8]}-{asset.key}"
        return name.replace(":", "-").replace("_", "-").lower()

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
        mini_dag = self.state.dag.mini_dag(asset.key)

        cmd = self._build_command(mini_dag, partition_or_window, self.state.run_id)
        name = self._build_name(asset)
        env = self._build_env()
        volumes = self._build_volumes()

        self.state.mark_asset_running(asset)

        container = self._docker.containers.run(
            image=self._image,
            name=name,
            command=cmd,
            environment=env,
            volumes=volumes if volumes else None,
            labels={"interloper.asset_key": asset.key},
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )

        return container

    def _wait_any(self, containers: list[Container]) -> Any:
        """Wait for any container to finish by polling.

        IMPORTANT: the `_execute_asset` method of the base class is not called by `_submit_asset`.
        Therefore, the state has to be updated manually here and in `_submit_asset` above.

        Args:
            containers: List of container objects to wait for

        Returns:
            The container object that finished
        """

        while True:
            for container in containers:
                container.reload()

                if container.status in ("exited", "dead"):
                    result = container.wait()
                    status_code = result.get("StatusCode", 1)

                    # Map back to asset
                    asset: Asset | None = None
                    asset_key = container.labels.get("interloper.asset_key")
                    if asset_key and asset_key in self.state.dag.asset_map:
                        asset = self.state.dag.asset_map[asset_key]
                    if asset is None:
                        raise RuntimeError("Failed to map container to asset")

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

                        if self._reraise or self._fail_fast:
                            raise RuntimeError(f"Container {container.id} exited with code {status_code}")

                    # Remove the container after processing
                    try:
                        container.remove()
                    except Exception as e:
                        print(f"Error removing container {container.id}: {e}")
                        pass

                    return container

    def _cancel_all(self, containers: list[Container]) -> None:
        for container in containers:
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
                self.state.mark_asset_cancelled(asset)

    def to_spec(self) -> RunnerSpec:
        return RunnerSpec(
            path=self.path,
            init=dict(
                image=self._image,
                max_containers=self._max_containers,
                env_vars=self._env_vars,
                volumes=self._volumes,
                fail_fast=self._fail_fast,
                reraise=self._reraise,
            ),
        )
