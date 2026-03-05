"""Tests for DockerBackfiller."""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import interloper as il
import pytest
from interloper.cli.config import Config
from interloper.errors import PartitionError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.serialization.backfiller import BackfillerSpec

from interloper_docker import DockerBackfiller


@pytest.fixture
def mock_docker_client():
    """Patch docker.from_env and return the mock client."""
    with patch("interloper_docker.backfiller.docker.from_env") as mock_from_env:
        client = MagicMock()
        mock_from_env.return_value = client
        yield client


@pytest.fixture
def backfiller(mock_docker_client):
    """A DockerBackfiller with default settings and a mocked Docker client."""
    return DockerBackfiller(image="test-image:latest")


@pytest.fixture
def backfiller_custom(mock_docker_client):
    """A DockerBackfiller with custom settings and a mocked Docker client."""
    return DockerBackfiller(
        image="custom-image:v2",
        env_vars={"DB_HOST": "localhost", "ENV": "test"},
        max_containers=3,
        volumes={"/host/data": {"bind": "/container/data", "mode": "ro"}},
        dind=True,
    )


@pytest.fixture
def simple_dag(tmp_path):
    """A minimal DAG with a single partitioned asset for command-building tests."""
    io = il.FileIO(tmp_path)

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def my_asset(context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "v": 1}]

    return il.DAG(my_asset(io=io))


class TestDockerBackfillerInit:
    """Initialization and default parameter tests."""

    def test_default_params(self, backfiller):
        """Default values are applied correctly."""
        assert backfiller._image == "test-image:latest"
        assert backfiller._env_vars == {}
        assert backfiller._max_containers == 1
        assert backfiller._volumes == {}
        assert backfiller._dind is False

    def test_custom_params(self, backfiller_custom):
        """Custom values are stored correctly."""
        assert backfiller_custom._image == "custom-image:v2"
        assert backfiller_custom._env_vars == {"DB_HOST": "localhost", "ENV": "test"}
        assert backfiller_custom._max_containers == 3
        assert backfiller_custom._volumes == {"/host/data": {"bind": "/container/data", "mode": "ro"}}
        assert backfiller_custom._dind is True

    def test_docker_client_initialized(self, mock_docker_client, backfiller):
        """Docker client is created via docker.from_env()."""
        assert backfiller._docker is mock_docker_client

    def test_runner_reraise_forced_true(self, backfiller):
        """The inner runner's _reraise is forced to True."""
        assert backfiller.runner._reraise is True

    def test_default_runner_is_multi_thread(self, backfiller):
        """Without an explicit runner, a MultiThreadRunner is used."""
        assert isinstance(backfiller.runner, il.MultiThreadRunner)

    def test_custom_runner_accepted(self, mock_docker_client):
        """A provided runner is used instead of the default."""
        custom_runner = il.SerialRunner()
        b = DockerBackfiller(image="img", runner=custom_runner)
        assert b.runner is custom_runner
        # reraise is still forced
        assert b.runner._reraise is True

    def test_env_vars_none_becomes_empty_dict(self, mock_docker_client):
        """Passing None for env_vars results in an empty dict."""
        b = DockerBackfiller(image="img", env_vars=None)
        assert b._env_vars == {}

    def test_volumes_none_becomes_empty_dict(self, mock_docker_client):
        """Passing None for volumes results in an empty dict."""
        b = DockerBackfiller(image="img", volumes=None)
        assert b._volumes == {}

    def test_log_threads_initialized_empty(self, backfiller):
        """Log thread tracking dict starts empty."""
        assert backfiller._log_threads == {}

    def test_stop_log_streaming_event_created(self, backfiller):
        """The stop event for log streaming is initialized."""
        assert not backfiller._stop_log_streaming.is_set()


class TestDockerBackfillerCapacity:
    """Tests for the _capacity property."""

    def test_default_capacity(self, backfiller):
        """Default capacity is 1."""
        assert backfiller._capacity == 1

    def test_custom_capacity(self, backfiller_custom):
        """Custom capacity is reflected in _capacity."""
        assert backfiller_custom._capacity == 3

    def test_capacity_matches_max_containers(self, mock_docker_client):
        """Capacity equals max_containers for any value."""
        b = DockerBackfiller(image="img", max_containers=10)
        assert b._capacity == 10


class TestDockerBackfillerToSpec:
    """Tests for the to_spec() serialization roundtrip."""

    def test_to_spec_returns_backfiller_spec(self, backfiller):
        """to_spec() returns a BackfillerSpec instance."""
        spec = backfiller.to_spec()
        assert isinstance(spec, BackfillerSpec)

    def test_to_spec_default_values(self, backfiller):
        """Spec captures default init kwargs."""
        spec = backfiller.to_spec()
        assert spec.init["image"] == "test-image:latest"
        assert spec.init["env_vars"] == {}
        assert spec.init["volumes"] == {}
        assert spec.init["max_containers"] == 1
        assert spec.init["dind"] is False

    def test_to_spec_custom_values(self, backfiller_custom):
        """Spec captures custom init kwargs."""
        spec = backfiller_custom.to_spec()
        assert spec.init["image"] == "custom-image:v2"
        assert spec.init["env_vars"] == {"DB_HOST": "localhost", "ENV": "test"}
        assert spec.init["volumes"] == {"/host/data": {"bind": "/container/data", "mode": "ro"}}
        assert spec.init["max_containers"] == 3
        assert spec.init["dind"] is True

    def test_to_spec_path(self, backfiller):
        """Spec path points to the DockerBackfiller class."""
        spec = backfiller.to_spec()
        assert spec.path == "interloper_docker.backfiller.DockerBackfiller"

    def test_to_spec_roundtrip(self, mock_docker_client):
        """Reconstructing from a spec produces an equivalent backfiller."""
        original = DockerBackfiller(
            image="roundtrip-image:v1",
            env_vars={"KEY": "VALUE"},
            max_containers=5,
            volumes={"/a": {"bind": "/b", "mode": "rw"}},
            dind=True,
        )
        spec = original.to_spec()
        reconstructed = spec.reconstruct()

        assert isinstance(reconstructed, DockerBackfiller)
        assert reconstructed._image == original._image
        assert reconstructed._env_vars == original._env_vars
        assert reconstructed._max_containers == original._max_containers
        assert reconstructed._volumes == original._volumes
        assert reconstructed._dind == original._dind


class TestDockerBackfillerBuildCommand:
    """Tests for _build_command() CLI command construction."""

    def test_command_with_time_partition(self, backfiller, simple_dag):
        """TimePartition produces --date flag."""
        partition = TimePartition(dt.date(2025, 6, 15))
        cmd = backfiller._build_command(simple_dag, partition, "bf-123")

        assert cmd[0] == "interloper"
        assert cmd[1] == "run"
        assert "--format=inline" in cmd
        assert "--backfill-id=bf-123" in cmd
        assert "--date" in cmd
        assert cmd[cmd.index("--date") + 1] == "2025-06-15"

    def test_command_with_time_partition_window(self, backfiller, simple_dag):
        """TimePartitionWindow produces --start-date and --end-date flags."""
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 31))
        cmd = backfiller._build_command(simple_dag, window, "bf-456")

        assert "--start-date" in cmd
        assert cmd[cmd.index("--start-date") + 1] == "2025-01-01"
        assert "--end-date" in cmd
        assert cmd[cmd.index("--end-date") + 1] == "2025-01-31"

    def test_command_with_none_partition(self, backfiller, simple_dag):
        """None partition omits date flags."""
        cmd = backfiller._build_command(simple_dag, None, "bf-789")

        assert "--date" not in cmd
        assert "--start-date" not in cmd
        assert "--end-date" not in cmd
        assert "--format=inline" in cmd
        assert "--backfill-id=bf-789" in cmd

    def test_command_contains_inline_config_json(self, backfiller, simple_dag):
        """Command includes the serialized config JSON."""
        partition = TimePartition(dt.date(2025, 1, 1))
        cmd = backfiller._build_command(simple_dag, partition, "bf-aaa")

        config_json = Config(dag=simple_dag, runner=backfiller.runner).to_json()
        assert config_json in cmd

    def test_command_unsupported_partition_raises(self, backfiller, simple_dag):
        """Unsupported partition types raise PartitionError."""
        unsupported = MagicMock()
        unsupported.__class__ = type("CustomPartition", (), {})

        with pytest.raises(PartitionError, match="Unsupported partition or window type"):
            backfiller._build_command(simple_dag, unsupported, "bf-000")

    def test_command_backfill_id_embedded(self, backfiller, simple_dag):
        """Backfill ID is embedded in the command with the correct prefix."""
        partition = TimePartition(dt.date(2025, 3, 1))
        cmd = backfiller._build_command(simple_dag, partition, "my-backfill-id")
        assert "--backfill-id=my-backfill-id" in cmd


class TestDockerBackfillerBuildEnv:
    """Tests for _build_env() environment variable construction."""

    def test_empty_env_still_has_events_flag(self, backfiller):
        """Even with no env_vars, the events stderr flag is set."""
        env = backfiller._build_env()
        assert env == {"INTERLOPER_EVENTS_TO_STDERR": "true"}

    def test_env_vars_merged_with_events_flag(self, backfiller_custom):
        """User env vars are merged with the events flag."""
        env = backfiller_custom._build_env()
        assert env["DB_HOST"] == "localhost"
        assert env["ENV"] == "test"
        assert env["INTERLOPER_EVENTS_TO_STDERR"] == "true"

    def test_env_vars_copied(self, backfiller_custom):
        """Env vars are returned as a copy, not a reference."""
        env = backfiller_custom._build_env()
        env["NEW_KEY"] = "new_value"
        assert "NEW_KEY" not in backfiller_custom._build_env()


class TestDockerBackfillerBuildVolumes:
    """Tests for _build_volumes() volume mount construction."""

    def test_dict_volumes(self, mock_docker_client):
        """Dict volumes are passed through."""
        b = DockerBackfiller(
            image="img",
            volumes={"/host/data": {"bind": "/container/data", "mode": "ro"}},
        )
        volumes = b._build_volumes()
        assert volumes == {"/host/data": {"bind": "/container/data", "mode": "ro"}}

    def test_list_volumes(self, mock_docker_client):
        """List-style volumes are converted to dict format."""
        b = DockerBackfiller(
            image="img",
            volumes=["/host/path:/container/path", "/data:/mnt/data"],
        )
        volumes = b._build_volumes()
        assert volumes == {
            "/host/path": {"bind": "/container/path", "mode": "rw"},
            "/data": {"bind": "/mnt/data", "mode": "rw"},
        }

    def test_empty_volumes_no_dind(self, backfiller):
        """Empty volumes without dind produces empty result."""
        assert backfiller._build_volumes() == {}

    def test_dind_mounts_docker_socket(self, backfiller_custom):
        """When dind=True, Docker socket is mounted."""
        volumes = backfiller_custom._build_volumes()
        assert "/var/run/docker.sock" in volumes
        assert volumes["/var/run/docker.sock"] == {"bind": "/var/run/docker.sock", "mode": "rw"}

    def test_dind_false_no_socket(self, backfiller):
        """When dind=False, Docker socket is not mounted."""
        volumes = backfiller._build_volumes()
        assert "/var/run/docker.sock" not in volumes

    def test_dind_with_user_volumes(self, mock_docker_client):
        """DinD socket is added alongside user-provided volumes."""
        b = DockerBackfiller(
            image="img",
            volumes={"/my/vol": {"bind": "/app/vol", "mode": "rw"}},
            dind=True,
        )
        volumes = b._build_volumes()
        assert "/my/vol" in volumes
        assert "/var/run/docker.sock" in volumes


class TestDockerBackfillerBuildName:
    """Tests for _build_name() container name construction."""

    def test_name_with_partition(self, backfiller):
        """Name includes truncated backfill ID and partition string."""
        backfiller._state = MagicMock()
        backfiller._state.backfill_id = "abcdef12-3456-7890"

        partition = TimePartition(dt.date(2025, 6, 15))
        name = backfiller._build_name(partition)

        assert name.startswith("interloper-backfill-abcdef12")
        assert "_" not in name
        assert ":" not in name
        assert name == name.lower()

    def test_name_without_partition(self, backfiller):
        """Name without partition omits partition suffix."""
        backfiller._state = MagicMock()
        backfiller._state.backfill_id = "abcdef12-3456-7890"

        name = backfiller._build_name(None)
        assert name == "interloper-backfill-abcdef12"

    def test_name_sanitizes_special_chars(self, backfiller):
        """Underscores and colons are replaced with hyphens."""
        backfiller._state = MagicMock()
        backfiller._state.backfill_id = "1111_2222:3333-4444"

        name = backfiller._build_name(None)
        assert ":" not in name
        assert "_" not in name
        assert name == name.lower()


class TestDockerBackfillerLifecycle:
    """Tests for _on_start and _on_end lifecycle hooks."""

    def test_on_start_clears_stop_event(self, backfiller):
        """_on_start clears the stop log streaming event."""
        backfiller._stop_log_streaming.set()
        backfiller._on_start()
        assert not backfiller._stop_log_streaming.is_set()

    def test_on_end_sets_stop_event(self, backfiller):
        """_on_end signals log threads to stop."""
        backfiller._on_start()
        backfiller._on_end()
        assert backfiller._stop_log_streaming.is_set()

    def test_on_end_clears_log_threads(self, backfiller):
        """_on_end clears the log threads dict after joining."""
        mock_thread = MagicMock()
        backfiller._log_threads["container-1"] = mock_thread

        backfiller._on_end()

        mock_thread.join.assert_called_once_with(timeout=2.0)
        assert backfiller._log_threads == {}

    def test_on_end_joins_multiple_threads(self, backfiller):
        """_on_end joins all active log threads."""
        threads = {f"c-{i}": MagicMock() for i in range(3)}
        backfiller._log_threads = dict(threads)

        backfiller._on_end()

        for thread in threads.values():
            thread.join.assert_called_once_with(timeout=2.0)
        assert backfiller._log_threads == {}
