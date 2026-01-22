"""DBOS runner for durable workflow execution."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dbos import DBOS, DBOSConfiguredInstance, Queue, SetWorkflowID, WorkflowHandle
from dbos._sys_db import StepInfo
from interloper import Event, RunResult
from interloper.assets.base import Asset
from interloper.dag.base import DAG
from interloper.partitioning.base import Partition, PartitionWindow
from interloper.runners.base import Runner
from interloper.runners.results import ExecutionStatus
from interloper.serialization.asset import AssetSpec
from interloper.serialization.dag import DAGSpec
from interloper.serialization.runner import RunnerSpec


@DBOS.dbos_class()
class DBOSRunner(Runner[str], DBOSConfiguredInstance):
    """DBOS-based runner for durable workflow execution."""

    def __init__(
        self,
        concurrency: int = 10,
        on_event: Callable[[Event], None] | None = None,
    ):
        """Initialize the DBOS runner.

        Args:
            concurrency: The concurrency of the DBOS queue
        """
        Runner.__init__(self, fail_fast=False, reraise=True, on_event=on_event)
        DBOSConfiguredInstance.__init__(self, config_name="interloper")

        self._concurrency = concurrency
        self._queue = Queue("interloper", concurrency=self._capacity)
        self._handle = None

    @property
    def _capacity(self) -> int:
        return self._concurrency

    @property
    def handle(self) -> WorkflowHandle | None:
        return self._handle

    def run(
        self,
        dag: DAG,
        partition_or_window: Partition | PartitionWindow | None = None,
        workflow_id: str | None = None,
    ) -> RunResult:  # ty:ignore[invalid-method-override]
        """Materialize the DAG using a DBOS workflow.

        Args:
            dag: The DAG to execute
            partition_or_window: Either a Partition or PartitionWindow object
            workflow_id: Optional workflow ID

        Returns:
            RunResult: The result of the workflow
        """
        try:
            if workflow_id is None:
                self._handle = DBOS.start_workflow(self._materialize_workflow, dag.to_spec(), partition_or_window)
                return self._handle.get_result()

            with SetWorkflowID(workflow_id):
                self._handle = DBOS.start_workflow(self._materialize_workflow, dag.to_spec(), partition_or_window)
                return self._handle.get_result()

        except Exception:
            return RunResult(
                partition_or_window=partition_or_window,
                status=ExecutionStatus.FAILED,
                asset_executions=self.state.asset_executions,
                execution_time=self.state.elapsed_time or 0,
            )

    def _submit_asset(
        self,
        asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> str:
        """Produce and submit the DBOS asset workflows (`_execute_asset_workflow`) to the DBOS queue.

        Note: the function doesn't return a DBOS WorkflowHandle as handle for the asset execution.
        Instead, it returns the key of the asset so we can use strings for the `inflight` handle dictionary
        and send DBOS messages based on the asset key to update the state.

        Args:
            asset: The asset to execute
            partition_or_window: Either a Partition or PartitionWindow object

        Returns:
            The key of the asset as handle for the execution.
        """
        if DBOS.workflow_id is None:
            raise RuntimeError("Workflow ID is not set")

        self._queue.enqueue(self._execute_asset_workflow, DBOS.workflow_id, asset.to_spec(), partition_or_window)

        return asset.key

    def _wait_any(self, handles: list[str]) -> str:
        """Wait for any asset to complete and return the key of the completed asset."""

        (status, asset_key, error) = DBOS.recv()

        if status == ExecutionStatus.FAILED.value:
            raise RuntimeError(f"Asset {asset_key} failed: {error}")

        if status == ExecutionStatus.COMPLETED.value and asset_key not in handles:
            raise RuntimeError(f"Completed asset key {asset_key} not found in handles {handles}")

        return asset_key

    def _wait_all(self) -> None:
        while not self.state.is_run_complete():
            try:
                self._wait_any([asset.key for asset in self.state.running_assets])
            except Exception:
                pass

    def _cancel_all(self, handles: list[str]) -> None:
        raise NotImplementedError("Not supported for DBOS runner")

    @DBOS.workflow(name="materialize")
    def _materialize_workflow(
        self,
        dag_spec: DAGSpec,
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> RunResult:
        """DBOS workflow to materialize the DAG.

        Directly wraps the base `materialize` method to preserve the scheduling logic which calls `_submit_asset`.
        `_submit_asset` will then produce the DBOS assets as part of that workflow.
        """

        try:
            return super().run(dag_spec.reconstruct(), partition_or_window)
        except Exception:
            self._wait_all()
            raise RuntimeError(
                f"Failed to materialize workflow. Failed assets: {[asset.key for asset in self.state.failed_assets]}"
            )

    @DBOS.workflow(name="execute_asset")
    def _execute_asset_workflow(
        self,
        workflow_id: str,
        asset_spec: AssetSpec,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Any:
        """DBOS workflow to execute an asset.

        Wraps the DBOS asset step in order to send messages to the parent workflow since messages cannot be sent
        from within a step.
        """
        asset = asset_spec.reconstruct()

        try:
            result = self._execute_asset_step(asset_spec, partition_or_window)
        except Exception as e:
            DBOS.send(workflow_id, (ExecutionStatus.FAILED.value, asset.key, e))
            raise e

        DBOS.send(workflow_id, (ExecutionStatus.COMPLETED.value, asset.key, None))
        return result

    @DBOS.step(name="execute_asset")
    def _execute_asset_step(
        self,
        asset_spec: AssetSpec,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> Any:
        asset = asset_spec.reconstruct()
        return self._execute_asset(asset, partition_or_window)

    def list_steps(self) -> list[StepInfo]:
        if self._handle is None:
            raise RuntimeError("Workflow handle is not set. Materialization has not been executed.")

        steps = DBOS.list_workflow_steps(self._handle.workflow_id)
        return [step for step in steps if step["function_name"] == "execute_asset"]

    def list_failed_steps(self) -> list[StepInfo]:
        failed_steps = []
        for step in self.list_steps():
            workflow_id = step["child_workflow_id"]
            if workflow_id is None:
                continue

            workflow = DBOS.get_workflow_status(workflow_id)
            if not workflow:
                raise RuntimeError(f"Workflow {workflow_id} not found")

            if workflow.status == "ERROR":
                failed_steps.append(step)
        return failed_steps

    def get_first_failed_step(self) -> StepInfo | None:
        return next((step for step in self.list_failed_steps()), None)

    def to_spec(self) -> RunnerSpec:
        return RunnerSpec(
            path="dbos",
            init={
                "concurrency": self._concurrency,
            },
        )
