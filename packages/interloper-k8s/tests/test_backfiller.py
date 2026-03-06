"""Tests for KubernetesBackfiller."""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import interloper as il
import pytest
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.serialization.backfiller import BackfillerInstanceSpec

from interloper_k8s import KubernetesBackfiller

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_runner():
    """A mock runner for backfiller tests."""
    runner = il.SerialRunner(fail_fast=False, reraise=False)
    return runner


@pytest.fixture
def default_backfiller(mock_runner):
    """A KubernetesBackfiller with only required parameters."""
    return KubernetesBackfiller(image="my-image:latest", runner=mock_runner)


@pytest.fixture
def custom_backfiller(mock_runner):
    """A KubernetesBackfiller with all parameters customized."""
    return KubernetesBackfiller(
        image="registry.example.com/interloper:v2",
        namespace="staging",
        max_jobs=6,
        env_vars={"API_KEY": "secret123", "STAGE": "staging"},
        service_account="backfiller-sa",
        image_pull_policy="IfNotPresent",
        image_pull_secrets=["registry-cred"],
        resources={
            "requests": {"cpu": "1", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "4Gi"},
        },
        node_selector={"workload": "batch"},
        tolerations=[
            {"key": "batch", "operator": "Equal", "value": "true", "effect": "NoSchedule"},
        ],
        ttl_seconds_after_finished=120,
        runner=mock_runner,
    )


@pytest.fixture
def partitioned_dag(tmp_path):
    """A partitioned DAG for backfiller tests."""
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

    def test_defaults(self, default_backfiller):
        """Default values are applied for all optional parameters."""
        b = default_backfiller
        assert b._image == "my-image:latest"
        assert b._namespace == "default"
        assert b._max_jobs == 4
        assert b._env_vars == {}
        assert b._service_account is None
        assert b._image_pull_policy is None
        assert b._image_pull_secrets == []
        assert b._resources is None
        assert b._node_selector is None
        assert b._tolerations == []
        assert b._ttl_seconds_after_finished == 300

    def test_custom_params(self, custom_backfiller):
        """All custom parameters are stored correctly."""
        b = custom_backfiller
        assert b._image == "registry.example.com/interloper:v2"
        assert b._namespace == "staging"
        assert b._max_jobs == 6
        assert b._env_vars == {"API_KEY": "secret123", "STAGE": "staging"}
        assert b._service_account == "backfiller-sa"
        assert b._image_pull_policy == "IfNotPresent"
        assert b._image_pull_secrets == ["registry-cred"]
        assert b._resources["limits"]["memory"] == "4Gi"
        assert b._node_selector == {"workload": "batch"}
        assert len(b._tolerations) == 1
        assert b._ttl_seconds_after_finished == 120

    def test_runner_reraise_forced(self, mock_runner):
        """The backfiller forces the runner to re-raise exceptions."""
        b = KubernetesBackfiller(image="img", runner=mock_runner)
        assert b.runner._reraise is True

    def test_k8s_clients_initially_none(self, default_backfiller):
        """Kubernetes API clients are not created until _on_start."""
        assert default_backfiller._batch_v1 is None
        assert default_backfiller._core_v1 is None

    def test_log_threads_initially_empty(self, default_backfiller):
        """Log thread tracking is initially empty."""
        assert default_backfiller._log_threads == {}
        assert not default_backfiller._stop_log_streaming.is_set()


class TestCapacity:
    """The _capacity property reflects max_jobs."""

    def test_default_capacity(self, default_backfiller):
        """Default capacity is 4."""
        assert default_backfiller._capacity == 4

    def test_custom_capacity(self, custom_backfiller):
        """Custom capacity matches max_jobs."""
        assert custom_backfiller._capacity == 6


class TestToSpec:
    """Serialization to BackfillerInstanceSpec and roundtrip reconstruction."""

    def test_to_spec_returns_backfiller_spec(self, default_backfiller):
        """to_spec returns a BackfillerInstanceSpec."""
        spec = default_backfiller.to_spec()
        assert isinstance(spec, BackfillerInstanceSpec)

    def test_to_spec_path(self, default_backfiller):
        """Spec path points to the KubernetesBackfiller class."""
        spec = default_backfiller.to_spec()
        assert spec.path == "interloper_k8s.backfiller.KubernetesBackfiller"

    def test_to_spec_default_init(self, default_backfiller):
        """Spec init dict captures all constructor kwargs."""
        spec = default_backfiller.to_spec()
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
        assert init["ttl_seconds_after_finished"] == 300

    def test_to_spec_custom_init(self, custom_backfiller):
        """Spec init dict captures all custom constructor kwargs."""
        spec = custom_backfiller.to_spec()
        init = spec.init
        assert init["image"] == "registry.example.com/interloper:v2"
        assert init["namespace"] == "staging"
        assert init["max_jobs"] == 6
        assert init["env_vars"] == {"API_KEY": "secret123", "STAGE": "staging"}
        assert init["service_account"] == "backfiller-sa"
        assert init["image_pull_policy"] == "IfNotPresent"
        assert init["image_pull_secrets"] == ["registry-cred"]
        assert init["resources"]["limits"]["memory"] == "4Gi"
        assert init["node_selector"] == {"workload": "batch"}
        assert init["tolerations"][0]["key"] == "batch"
        assert init["ttl_seconds_after_finished"] == 120

    def test_to_spec_roundtrip(self, custom_backfiller):
        """Reconstructing from spec yields an equivalent backfiller."""
        spec = custom_backfiller.to_spec()
        reconstructed = spec.reconstruct()
        assert isinstance(reconstructed, KubernetesBackfiller)
        assert reconstructed._image == custom_backfiller._image
        assert reconstructed._namespace == custom_backfiller._namespace
        assert reconstructed._max_jobs == custom_backfiller._max_jobs
        assert reconstructed._env_vars == custom_backfiller._env_vars
        assert reconstructed._service_account == custom_backfiller._service_account
        assert reconstructed._image_pull_policy == custom_backfiller._image_pull_policy
        assert reconstructed._image_pull_secrets == custom_backfiller._image_pull_secrets
        assert reconstructed._resources == custom_backfiller._resources
        assert reconstructed._node_selector == custom_backfiller._node_selector
        assert reconstructed._tolerations == custom_backfiller._tolerations
        assert reconstructed._ttl_seconds_after_finished == custom_backfiller._ttl_seconds_after_finished

    def test_to_spec_excludes_runner(self, default_backfiller):
        """Spec init does not include the runner (it's separate)."""
        spec = default_backfiller.to_spec()
        assert "runner" not in spec.init


class TestBuildEnv:
    """Environment variable list construction."""

    def test_empty_env_still_has_events_flag(self, default_backfiller):
        """Even with no user env vars, INTERLOPER_EVENTS_TO_STDERR is set."""
        result = default_backfiller._build_env()
        names = {e.name for e in result}
        assert "INTERLOPER_EVENTS_TO_STDERR" in names

    def test_env_vars_merged_with_events_flag(self, custom_backfiller):
        """User env vars are included alongside the events flag."""
        result = custom_backfiller._build_env()
        names = {e.name for e in result}
        assert "API_KEY" in names
        assert "STAGE" in names
        assert "INTERLOPER_EVENTS_TO_STDERR" in names
        assert len(result) == 3  # 2 user + 1 system

    def test_events_flag_value(self, default_backfiller):
        """INTERLOPER_EVENTS_TO_STDERR is set to 'true'."""
        result = default_backfiller._build_env()
        events_env = next(e for e in result if e.name == "INTERLOPER_EVENTS_TO_STDERR")
        assert events_env.value == "true"


class TestBuildResources:
    """Resource requirements construction."""

    def test_no_resources(self, default_backfiller):
        """Returns None when no resources configured."""
        assert default_backfiller._build_resources() is None

    def test_with_resources(self, custom_backfiller):
        """Returns a V1ResourceRequirements with requests and limits."""
        result = custom_backfiller._build_resources()
        assert result is not None
        assert result.requests == {"cpu": "1", "memory": "1Gi"}
        assert result.limits == {"cpu": "4", "memory": "4Gi"}


class TestBuildTolerations:
    """Toleration list construction."""

    def test_no_tolerations(self, default_backfiller):
        """Empty tolerations list produces an empty list."""
        assert default_backfiller._build_tolerations() == []

    def test_tolerations_converted(self, custom_backfiller):
        """Toleration dicts become V1Toleration objects."""
        result = custom_backfiller._build_tolerations()
        assert len(result) == 1
        t = result[0]
        assert t.key == "batch"
        assert t.operator == "Equal"
        assert t.value == "true"
        assert t.effect == "NoSchedule"


class TestBuildJobName:
    """Job name generation for backfill runs."""

    def test_name_with_partition(self, default_backfiller):
        """Job name includes backfill ID prefix and partition ID."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(
            partitions=[TimePartition(dt.date(2025, 1, 1))],
        )

        partition = TimePartition(dt.date(2025, 1, 1))
        name = default_backfiller._build_job_name(partition)

        assert name.startswith("interloper-backfill-")
        assert name == name.lower()
        assert len(name) <= 63

    def test_name_without_partition(self, default_backfiller):
        """Job name works when partition is None."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        name = default_backfiller._build_job_name(None)
        assert name.startswith("interloper-backfill-")
        assert len(name) <= 63

    def test_name_truncated_to_63(self, default_backfiller):
        """Long names are truncated to K8s 63-char limit."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(
            partitions=[None],
            metadata={"backfill_id": "a" * 50},
        )

        partition = MagicMock()
        partition.id = "very-long-partition-id-that-exceeds-everything"
        name = default_backfiller._build_job_name(partition)
        assert len(name) <= 63

    def test_name_special_chars_replaced(self, default_backfiller):
        """Colons and underscores are replaced with hyphens."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(
            partitions=[None],
        )

        partition = MagicMock()
        partition.id = "2025_01:window"
        name = default_backfiller._build_job_name(partition)
        assert ":" not in name
        assert "_" not in name


class TestBuildLabelsAndAnnotations:
    """Labels and annotations construction."""

    def test_labels_contain_backfill_id(self, default_backfiller):
        """Labels include a truncated backfill_id."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        labels = default_backfiller._build_labels(None)
        assert "interloper.backfill_id" in labels
        assert labels["interloper.backfill_id"] == default_backfiller.state.backfill_id[:8]

    def test_annotations_with_partition(self, default_backfiller):
        """Annotations include the partition ID when partition is provided."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(
            partitions=[TimePartition(dt.date(2025, 3, 1))],
        )

        partition = TimePartition(dt.date(2025, 3, 1))
        annotations = default_backfiller._build_annotations(partition)
        assert "interloper.partition" in annotations

    def test_annotations_without_partition(self, default_backfiller):
        """Annotations are empty when partition is None."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        annotations = default_backfiller._build_annotations(None)
        assert annotations == {}


class TestBuildCommand:
    """Command line generation for container execution."""

    def test_time_partition_command(self, default_backfiller, partitioned_dag):
        """TimePartition produces --date flag."""
        from interloper.backfillers.state import BackfillState

        partition = TimePartition(dt.date(2025, 7, 15))
        default_backfiller._state = BackfillState(partitions=[partition])

        cmd = default_backfiller._build_command(
            partitioned_dag, partition, default_backfiller.state.backfill_id
        )

        assert cmd[0] == "interloper"
        assert cmd[1] == "run"
        assert "--format=inline" in cmd
        assert "--date" in cmd
        assert "2025-07-15" in cmd
        # backfill-id flag should be present
        backfill_flags = [c for c in cmd if c.startswith("--backfill-id=")]
        assert len(backfill_flags) == 1

    def test_time_partition_window_command(self, default_backfiller, partitioned_dag):
        """TimePartitionWindow produces --start-date and --end-date flags."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        window = TimePartitionWindow(
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 3, 31),
        )

        cmd = default_backfiller._build_command(
            partitioned_dag, window, default_backfiller.state.backfill_id
        )

        assert "--start-date" in cmd
        assert "2025-01-01" in cmd
        assert "--end-date" in cmd
        assert "2025-03-31" in cmd

    def test_none_partition_command(self, default_backfiller, partitioned_dag):
        """None partition produces command without date flags."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        cmd = default_backfiller._build_command(
            partitioned_dag, None, default_backfiller.state.backfill_id
        )

        assert "--date" not in cmd
        assert "--start-date" not in cmd
        assert "--end-date" not in cmd

    def test_unsupported_partition_raises(self, default_backfiller, partitioned_dag):
        """Unsupported partition type raises PartitionError."""
        from interloper.backfillers.state import BackfillState

        default_backfiller._state = BackfillState(partitions=[None])

        bad_partition = MagicMock()

        with pytest.raises(il.PartitionError):
            default_backfiller._build_command(
                partitioned_dag, bad_partition, default_backfiller.state.backfill_id
            )

    def test_command_includes_runner_in_config(self, default_backfiller, partitioned_dag):
        """The inline JSON config includes the runner spec."""
        from interloper.backfillers.state import BackfillState

        partition = TimePartition(dt.date(2025, 1, 1))
        default_backfiller._state = BackfillState(partitions=[partition])

        cmd = default_backfiller._build_command(
            partitioned_dag, partition, default_backfiller.state.backfill_id
        )

        # Find the inline JSON arg (the one containing '{')
        json_args = [c for c in cmd if "{" in c]
        assert len(json_args) == 1
        assert "runner" in json_args[0]


class TestSubmitRun:
    """Job object construction during _submit_run."""

    def test_job_structure(self, default_backfiller, partitioned_dag):
        """Verify the K8s Job object structure passed to create_namespaced_job."""
        from interloper.backfillers.state import BackfillState

        partition = TimePartition(dt.date(2025, 1, 1))
        default_backfiller._state = BackfillState(partitions=[partition])
        default_backfiller._state.start_backfill()

        mock_batch = MagicMock()
        mock_core = MagicMock()
        default_backfiller._batch_v1 = mock_batch
        default_backfiller._core_v1 = mock_core

        default_backfiller._submit_run(partitioned_dag, partition)

        mock_batch.create_namespaced_job.assert_called_once()
        call_kwargs = mock_batch.create_namespaced_job.call_args
        assert call_kwargs.kwargs["namespace"] == "default"

        job = call_kwargs.kwargs["body"]

        # Top-level fields
        assert job.api_version == "batch/v1"
        assert job.kind == "Job"

        # Metadata
        assert job.metadata.namespace == "default"
        assert "interloper.backfill_id" in job.metadata.labels

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

    def test_submit_returns_job_name(self, default_backfiller, partitioned_dag):
        """_submit_run returns the job name string."""
        from interloper.backfillers.state import BackfillState

        partition = TimePartition(dt.date(2025, 1, 1))
        default_backfiller._state = BackfillState(partitions=[partition])
        default_backfiller._state.start_backfill()

        mock_batch = MagicMock()
        mock_core = MagicMock()
        default_backfiller._batch_v1 = mock_batch
        default_backfiller._core_v1 = mock_core

        result = default_backfiller._submit_run(partitioned_dag, partition)

        assert isinstance(result, str)
        assert result.startswith("interloper-backfill-")

    def test_submit_starts_log_streaming(self, default_backfiller, partitioned_dag):
        """_submit_run starts a log streaming thread for the job."""
        from interloper.backfillers.state import BackfillState

        partition = TimePartition(dt.date(2025, 1, 1))
        default_backfiller._state = BackfillState(partitions=[partition])
        default_backfiller._state.start_backfill()

        mock_batch = MagicMock()
        mock_core = MagicMock()
        default_backfiller._batch_v1 = mock_batch
        default_backfiller._core_v1 = mock_core

        job_name = default_backfiller._submit_run(partitioned_dag, partition)

        # A log streaming thread should have been created
        assert job_name in default_backfiller._log_threads

        # Clean up the thread
        default_backfiller._stop_log_streaming.set()
        default_backfiller._log_threads[job_name].join(timeout=2.0)


class TestOnStart:
    """Kubernetes client initialization via _on_start."""

    @patch("interloper_k8s.backfiller.config")
    @patch("interloper_k8s.backfiller.client")
    def test_on_start_incluster(self, mock_client, mock_config, mock_runner):
        """_on_start loads in-cluster config when available."""
        b = KubernetesBackfiller(image="img", runner=mock_runner)
        b._on_start()

        mock_config.load_incluster_config.assert_called_once()
        assert b._batch_v1 is not None
        assert b._core_v1 is not None
        assert not b._stop_log_streaming.is_set()

    @patch("interloper_k8s.backfiller.config")
    @patch("interloper_k8s.backfiller.client")
    def test_on_start_kubeconfig_fallback(self, mock_client, mock_config, mock_runner):
        """_on_start falls back to kubeconfig when in-cluster fails."""
        mock_config.ConfigException = Exception
        mock_config.load_incluster_config.side_effect = Exception("not in cluster")

        b = KubernetesBackfiller(image="img", runner=mock_runner)
        b._on_start()

        mock_config.load_kube_config.assert_called_once()

    @patch("interloper_k8s.backfiller.config")
    @patch("interloper_k8s.backfiller.client")
    def test_on_start_both_fail_raises(self, mock_client, mock_config, mock_runner):
        """_on_start raises RunnerError when both config methods fail."""
        mock_config.ConfigException = Exception
        mock_config.load_incluster_config.side_effect = Exception("not in cluster")
        mock_config.load_kube_config.side_effect = Exception("no kubeconfig")

        b = KubernetesBackfiller(image="img", runner=mock_runner)

        with pytest.raises(il.RunnerError, match="Failed to load Kubernetes config"):
            b._on_start()


class TestOnEnd:
    """Lifecycle cleanup in _on_end."""

    def test_on_end_signals_stop(self, default_backfiller):
        """_on_end sets the stop event for log streaming."""
        default_backfiller._on_end()
        assert default_backfiller._stop_log_streaming.is_set()

    def test_on_end_clears_threads(self, default_backfiller):
        """_on_end clears all log thread references."""
        # Add a mock thread
        mock_thread = MagicMock()
        default_backfiller._log_threads["test-job"] = mock_thread

        default_backfiller._on_end()

        mock_thread.join.assert_called_once_with(timeout=2.0)
        assert default_backfiller._log_threads == {}


class TestStopJobLogStreaming:
    """Log streaming cleanup for individual jobs."""

    def test_stop_existing_job(self, default_backfiller):
        """Stops and removes the log thread for a known job."""
        mock_thread = MagicMock()
        default_backfiller._log_threads["job-1"] = mock_thread

        default_backfiller._stop_job_log_streaming("job-1")

        mock_thread.join.assert_called_once_with(timeout=1.0)
        assert "job-1" not in default_backfiller._log_threads

    def test_stop_nonexistent_job(self, default_backfiller):
        """Stopping a nonexistent job is a no-op."""
        # Should not raise
        default_backfiller._stop_job_log_streaming("no-such-job")
