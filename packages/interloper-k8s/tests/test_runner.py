"""Tests for KubernetesRunner."""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import interloper as il
import pytest
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.serialization.runner import RunnerInstanceSpec

from interloper_k8s import KubernetesRunner

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_runner():
    """A KubernetesRunner with only required parameters."""
    return KubernetesRunner(image="my-image:latest")


@pytest.fixture
def custom_runner():
    """A KubernetesRunner with all parameters customized."""
    return KubernetesRunner(
        image="registry.example.com/interloper:v2",
        namespace="production",
        max_jobs=8,
        env_vars={"DB_HOST": "db.prod", "LOG_LEVEL": "debug"},
        service_account="interloper-sa",
        image_pull_policy="Always",
        image_pull_secrets=["regcred", "backup-cred"],
        resources={
            "requests": {"cpu": "500m", "memory": "512Mi"},
            "limits": {"cpu": "2", "memory": "2Gi"},
        },
        node_selector={"pool": "compute", "tier": "high"},
        tolerations=[
            {"key": "dedicated", "operator": "Equal", "value": "compute", "effect": "NoSchedule"},
            {"key": "gpu", "operator": "Exists", "effect": "NoSchedule"},
        ],
        poll_interval=2.5,
        ttl_seconds_after_finished=600,
        fail_fast=True,
        reraise=True,
    )


@pytest.fixture
def simple_dag(tmp_path):
    """A minimal DAG for testing command/job building."""
    io = il.FileIO(tmp_path)

    @il.asset
    def my_asset(context: il.ExecutionContext) -> list[dict]:
        return [{"v": 1}]

    return il.DAG(my_asset(io=io))


@pytest.fixture
def partitioned_dag(tmp_path):
    """A partitioned DAG for testing partition command building."""
    io = il.FileIO(tmp_path)
    part = il.TimePartitionConfig(column="date")

    @il.asset(partitioning=part)
    def my_asset(context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "v": 1}]

    return il.DAG(my_asset(io=io))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestInit:
    """Initialization with defaults and custom parameters."""

    def test_defaults(self, default_runner):
        """Default values are applied for all optional parameters."""
        r = default_runner
        assert r._image == "my-image:latest"
        assert r._namespace == "default"
        assert r._max_jobs == 4
        assert r._env_vars == {}
        assert r._service_account is None
        assert r._image_pull_policy is None
        assert r._image_pull_secrets == []
        assert r._resources is None
        assert r._node_selector is None
        assert r._tolerations == []
        assert r._poll_interval == 1.0
        assert r._ttl_seconds_after_finished == 300
        assert r._fail_fast is False
        assert r._reraise is False

    def test_custom_params(self, custom_runner):
        """All custom parameters are stored correctly."""
        r = custom_runner
        assert r._image == "registry.example.com/interloper:v2"
        assert r._namespace == "production"
        assert r._max_jobs == 8
        assert r._env_vars == {"DB_HOST": "db.prod", "LOG_LEVEL": "debug"}
        assert r._service_account == "interloper-sa"
        assert r._image_pull_policy == "Always"
        assert r._image_pull_secrets == ["regcred", "backup-cred"]
        assert r._resources == {
            "requests": {"cpu": "500m", "memory": "512Mi"},
            "limits": {"cpu": "2", "memory": "2Gi"},
        }
        assert r._node_selector == {"pool": "compute", "tier": "high"}
        assert len(r._tolerations) == 2
        assert r._poll_interval == 2.5
        assert r._ttl_seconds_after_finished == 600
        assert r._fail_fast is True
        assert r._reraise is True

    def test_k8s_clients_initially_none(self, default_runner):
        """Kubernetes API clients are not created until _on_start."""
        assert default_runner._batch_v1 is None
        assert default_runner._core_v1 is None


class TestCapacity:
    """The _capacity property reflects max_jobs."""

    def test_default_capacity(self, default_runner):
        """Default capacity is 4."""
        assert default_runner._capacity == 4

    def test_custom_capacity(self, custom_runner):
        """Custom capacity matches max_jobs."""
        assert custom_runner._capacity == 8


class TestToSpec:
    """Serialization to RunnerInstanceSpec and roundtrip reconstruction."""

    def test_to_spec_returns_runner_spec(self, default_runner):
        """to_spec returns a RunnerInstanceSpec."""
        spec = default_runner.to_spec()
        assert isinstance(spec, RunnerInstanceSpec)

    def test_to_spec_path(self, default_runner):
        """Spec path points to the KubernetesRunner class."""
        spec = default_runner.to_spec()
        assert spec.path == "interloper_k8s.runner.KubernetesRunner"

    def test_to_spec_default_init(self, default_runner):
        """Spec init dict captures all constructor kwargs."""
        spec = default_runner.to_spec()
        init = spec.init
        assert init["image"] == "my-image:latest"
        assert init["namespace"] == "default"
        assert init["max_jobs"] == 4
        assert init["env_vars"] == {}
        assert init["service_account"] is None
        assert init["image_pull_policy"] is None
        assert init["image_pull_secrets"] == []
        assert init["resources"] is None
        assert init["node_selector"] is None
        assert init["tolerations"] == []
        assert init["poll_interval"] == 1.0
        assert init["ttl_seconds_after_finished"] == 300
        assert init["fail_fast"] is False
        assert init["reraise"] is False

    def test_to_spec_custom_init(self, custom_runner):
        """Spec init dict captures all custom constructor kwargs."""
        spec = custom_runner.to_spec()
        init = spec.init
        assert init["image"] == "registry.example.com/interloper:v2"
        assert init["namespace"] == "production"
        assert init["max_jobs"] == 8
        assert init["env_vars"] == {"DB_HOST": "db.prod", "LOG_LEVEL": "debug"}
        assert init["service_account"] == "interloper-sa"
        assert init["image_pull_policy"] == "Always"
        assert init["image_pull_secrets"] == ["regcred", "backup-cred"]
        assert init["resources"]["requests"]["cpu"] == "500m"
        assert init["tolerations"][0]["key"] == "dedicated"
        assert init["poll_interval"] == 2.5
        assert init["ttl_seconds_after_finished"] == 600
        assert init["fail_fast"] is True
        assert init["reraise"] is True

    def test_to_spec_roundtrip(self, custom_runner):
        """Reconstructing from spec yields an equivalent runner."""
        spec = custom_runner.to_spec()
        reconstructed = spec.reconstruct()
        assert isinstance(reconstructed, KubernetesRunner)
        assert reconstructed._image == custom_runner._image
        assert reconstructed._namespace == custom_runner._namespace
        assert reconstructed._max_jobs == custom_runner._max_jobs
        assert reconstructed._env_vars == custom_runner._env_vars
        assert reconstructed._service_account == custom_runner._service_account
        assert reconstructed._image_pull_policy == custom_runner._image_pull_policy
        assert reconstructed._image_pull_secrets == custom_runner._image_pull_secrets
        assert reconstructed._resources == custom_runner._resources
        assert reconstructed._node_selector == custom_runner._node_selector
        assert reconstructed._tolerations == custom_runner._tolerations
        assert reconstructed._poll_interval == custom_runner._poll_interval
        assert reconstructed._ttl_seconds_after_finished == custom_runner._ttl_seconds_after_finished
        assert reconstructed._fail_fast == custom_runner._fail_fast
        assert reconstructed._reraise == custom_runner._reraise


class TestBuildEnv:
    """Environment variable list construction."""

    def test_empty_env(self, default_runner):
        """Empty env_vars produces an empty list."""
        result = default_runner._build_env()
        assert result == []

    def test_env_vars_converted(self, custom_runner):
        """Each env var becomes a V1EnvVar object."""
        result = custom_runner._build_env()
        assert len(result) == 2
        names = {e.name for e in result}
        assert names == {"DB_HOST", "LOG_LEVEL"}
        values = {e.value for e in result}
        assert values == {"db.prod", "debug"}


class TestBuildResources:
    """Resource requirements construction."""

    def test_no_resources(self, default_runner):
        """Returns None when no resources configured."""
        assert default_runner._build_resources() is None

    def test_with_resources(self, custom_runner):
        """Returns a V1ResourceRequirements with requests and limits."""
        result = custom_runner._build_resources()
        assert result is not None
        assert result.requests == {"cpu": "500m", "memory": "512Mi"}
        assert result.limits == {"cpu": "2", "memory": "2Gi"}

    def test_requests_only(self):
        """Resources with only requests, no limits."""
        r = KubernetesRunner(
            image="img",
            resources={"requests": {"cpu": "100m"}},
        )
        result = r._build_resources()
        assert result is not None
        assert result.requests == {"cpu": "100m"}
        assert result.limits is None


class TestBuildTolerations:
    """Toleration list construction."""

    def test_no_tolerations(self, default_runner):
        """Empty tolerations list produces an empty list."""
        assert default_runner._build_tolerations() == []

    def test_tolerations_converted(self, custom_runner):
        """Toleration dicts become V1Toleration objects."""
        result = custom_runner._build_tolerations()
        assert len(result) == 2

        t0 = result[0]
        assert t0.key == "dedicated"
        assert t0.operator == "Equal"
        assert t0.value == "compute"
        assert t0.effect == "NoSchedule"

        t1 = result[1]
        assert t1.key == "gpu"
        assert t1.operator == "Exists"
        assert t1.effect == "NoSchedule"

    def test_default_operator(self):
        """Operator defaults to 'Equal' when not specified."""
        r = KubernetesRunner(
            image="img",
            tolerations=[{"key": "mykey", "value": "myval", "effect": "NoSchedule"}],
        )
        result = r._build_tolerations()
        assert len(result) == 1
        assert result[0].operator == "Equal"


class TestBuildJobName:
    """Job name generation from asset instance key."""

    def test_simple_name(self, default_runner, simple_dag):
        """Constructs a valid K8s job name for a simple asset."""
        default_runner._state = MagicMock()
        default_runner._state.run_id = "abcdef12-3456-7890-abcd-ef1234567890"

        asset = simple_dag.assets[0]
        name = default_runner._build_job_name(asset)

        assert name.startswith("interloper-abcdef12-")
        # Must be lowercase, alphanumeric with hyphens
        assert name == name.lower()
        assert "." not in name
        assert "_" not in name
        assert len(name) <= 63

    def test_name_truncated_to_63(self, default_runner):
        """Job names are truncated to 63 characters (K8s limit)."""
        default_runner._state = MagicMock()
        default_runner._state.run_id = "a" * 50

        # Create a mock asset with a very long instance_key
        asset = MagicMock()
        asset.instance_key = "very.long.source.name:very_long_asset_name_that_exceeds_limits"

        name = default_runner._build_job_name(asset)
        assert len(name) <= 63

    def test_dots_and_underscores_replaced(self, default_runner):
        """Dots and underscores in instance_key are replaced with hyphens."""
        default_runner._state = MagicMock()
        default_runner._state.run_id = "run12345"

        asset = MagicMock()
        asset.instance_key = "my_source.v1:my_asset"

        name = default_runner._build_job_name(asset)
        assert "." not in name
        assert "_" not in name


class TestBuildCommand:
    """Command line generation for container execution."""

    def _setup_runner_state(self, runner, dag):
        """Set up runner state for command building tests."""
        from interloper.runners.state import RunState

        runner._state = RunState(dag)

    def test_time_partition_command(self, default_runner, partitioned_dag):
        """TimePartition produces --date flag."""
        self._setup_runner_state(default_runner, partitioned_dag)
        partition = TimePartition(dt.date(2025, 3, 15))

        cmd = default_runner._build_command(
            partitioned_dag, partition, default_runner.state.run_id
        )

        assert cmd[0] == "interloper"
        assert cmd[1] == "run"
        assert "--format" in cmd
        assert "inline" in cmd
        assert "--date" in cmd
        assert "2025-03-15" in cmd
        # run-id flag should be present
        run_id_flags = [c for c in cmd if c.startswith("--run-id=")]
        assert len(run_id_flags) == 1

    def test_time_partition_window_command(self, default_runner, partitioned_dag):
        """TimePartitionWindow produces --start-date and --end-date flags."""
        self._setup_runner_state(default_runner, partitioned_dag)
        window = TimePartitionWindow(
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 31),
        )

        cmd = default_runner._build_command(
            partitioned_dag, window, default_runner.state.run_id
        )

        assert "--start-date" in cmd
        assert "2025-01-01" in cmd
        assert "--end-date" in cmd
        assert "2025-01-31" in cmd

    def test_unsupported_partition_raises(self, default_runner, partitioned_dag):
        """Unsupported partition type raises PartitionError."""
        self._setup_runner_state(default_runner, partitioned_dag)

        with pytest.raises(il.PartitionError):
            default_runner._build_command(
                partitioned_dag, None, default_runner.state.run_id
            )

    def test_command_contains_inline_json(self, default_runner, partitioned_dag):
        """The last positional arg is the inline JSON config."""
        self._setup_runner_state(default_runner, partitioned_dag)
        partition = TimePartition(dt.date(2025, 6, 1))

        cmd = default_runner._build_command(
            partitioned_dag, partition, default_runner.state.run_id
        )

        # The inline JSON config should be the last element
        json_arg = cmd[-3]  # before --date and the date value
        # It should be a valid JSON string containing "dag"
        assert "{" in json_arg


class TestSubmitAsset:
    """Job object construction during _submit_asset."""

    def test_job_structure(self, default_runner, partitioned_dag):
        """Verify the K8s Job object structure passed to create_namespaced_job."""
        from interloper.runners.state import RunState

        default_runner._state = RunState(partitioned_dag)
        default_runner._state.start_run(TimePartition(dt.date(2025, 1, 1)))

        mock_batch = MagicMock()
        default_runner._batch_v1 = mock_batch

        asset = partitioned_dag.assets[0]
        partition = TimePartition(dt.date(2025, 1, 1))

        default_runner._submit_asset(asset, partition)

        # Verify create_namespaced_job was called
        mock_batch.create_namespaced_job.assert_called_once()
        call_kwargs = mock_batch.create_namespaced_job.call_args
        assert call_kwargs.kwargs["namespace"] == "default"

        job = call_kwargs.kwargs["body"]

        # Top-level fields
        assert job.api_version == "batch/v1"
        assert job.kind == "Job"

        # Metadata
        assert job.metadata.namespace == "default"
        assert "interloper.asset_key" in job.metadata.labels
        assert "interloper.run_id" in job.metadata.labels
        assert "interloper.asset_key" in job.metadata.annotations

        # Job spec
        assert job.spec.backoff_limit == 0
        assert job.spec.ttl_seconds_after_finished == 300

        # Pod spec
        pod_spec = job.spec.template.spec
        assert pod_spec.restart_policy == "Never"
        assert len(pod_spec.containers) == 1

        container = pod_spec.containers[0]
        assert container.name == "interloper"
        assert container.image == "my-image:latest"
        assert container.command == ["interloper"]
        assert len(container.args) > 0

    def test_job_with_custom_settings(self, custom_runner, partitioned_dag):
        """Job includes service account, resources, tolerations, pull secrets."""
        from interloper.runners.state import RunState

        custom_runner._state = RunState(partitioned_dag)
        custom_runner._state.start_run(TimePartition(dt.date(2025, 1, 1)))

        mock_batch = MagicMock()
        custom_runner._batch_v1 = mock_batch

        asset = partitioned_dag.assets[0]
        partition = TimePartition(dt.date(2025, 1, 1))

        custom_runner._submit_asset(asset, partition)

        job = mock_batch.create_namespaced_job.call_args.kwargs["body"]
        pod_spec = job.spec.template.spec

        # Service account
        assert pod_spec.service_account_name == "interloper-sa"

        # Image pull secrets
        assert pod_spec.image_pull_secrets is not None
        secret_names = [s.name for s in pod_spec.image_pull_secrets]
        assert "regcred" in secret_names
        assert "backup-cred" in secret_names

        # Node selector
        assert pod_spec.node_selector == {"pool": "compute", "tier": "high"}

        # Tolerations
        assert pod_spec.tolerations is not None
        assert len(pod_spec.tolerations) == 2

        # Container resources
        container = pod_spec.containers[0]
        assert container.resources is not None
        assert container.resources.requests["cpu"] == "500m"
        assert container.resources.limits["memory"] == "2Gi"

        # Image pull policy
        assert container.image_pull_policy == "Always"

        # Environment
        assert container.env is not None
        env_names = {e.name for e in container.env}
        assert "DB_HOST" in env_names
        assert "LOG_LEVEL" in env_names

    def test_submit_returns_job_name(self, default_runner, partitioned_dag):
        """_submit_asset returns the job name string."""
        from interloper.runners.state import RunState

        default_runner._state = RunState(partitioned_dag)
        default_runner._state.start_run(TimePartition(dt.date(2025, 1, 1)))

        mock_batch = MagicMock()
        default_runner._batch_v1 = mock_batch

        asset = partitioned_dag.assets[0]
        partition = TimePartition(dt.date(2025, 1, 1))

        result = default_runner._submit_asset(asset, partition)

        assert isinstance(result, str)
        assert result.startswith("interloper-")

    def test_submit_marks_asset_running(self, default_runner, partitioned_dag):
        """_submit_asset marks the asset as running in state."""
        from interloper.runners.state import RunState

        default_runner._state = RunState(partitioned_dag)
        default_runner._state.start_run(TimePartition(dt.date(2025, 1, 1)))

        mock_batch = MagicMock()
        default_runner._batch_v1 = mock_batch

        asset = partitioned_dag.assets[0]
        partition = TimePartition(dt.date(2025, 1, 1))

        default_runner._submit_asset(asset, partition)

        running_keys = [a.instance_key for a in default_runner.state.running_assets]
        assert asset.instance_key in running_keys


class TestOnStart:
    """Kubernetes client initialization via _on_start."""

    @patch("interloper_k8s.runner.config")
    @patch("interloper_k8s.runner.client")
    def test_on_start_incluster(self, mock_client, mock_config):
        """_on_start loads in-cluster config when available."""
        runner = KubernetesRunner(image="img")
        runner._on_start()

        mock_config.load_incluster_config.assert_called_once()
        assert runner._batch_v1 is not None
        assert runner._core_v1 is not None

    @patch("interloper_k8s.runner.config")
    @patch("interloper_k8s.runner.client")
    def test_on_start_kubeconfig_fallback(self, mock_client, mock_config):
        """_on_start falls back to kubeconfig when in-cluster fails."""
        mock_config.ConfigException = Exception
        mock_config.load_incluster_config.side_effect = Exception("not in cluster")

        runner = KubernetesRunner(image="img")
        runner._on_start()

        mock_config.load_kube_config.assert_called_once()
        assert runner._batch_v1 is not None
        assert runner._core_v1 is not None
