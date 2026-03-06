"""Tests for DockerRunner."""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import interloper as il
import pytest
from interloper.cli.config import Config
from interloper.errors import PartitionError
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.serialization.runner import RunnerInstanceSpec

from interloper_docker import DockerRunner


@pytest.fixture
def mock_docker_client():
    """Patch docker.from_env and return the mock client."""
    with patch("interloper_docker.runner.docker.from_env") as mock_from_env:
        client = MagicMock()
        mock_from_env.return_value = client
        yield client


@pytest.fixture
def runner(mock_docker_client):
    """A DockerRunner with default settings and a mocked Docker client."""
    return DockerRunner(image="test-image:latest")


@pytest.fixture
def runner_custom(mock_docker_client):
    """A DockerRunner with custom settings and a mocked Docker client."""
    return DockerRunner(
        image="custom-image:v2",
        max_containers=8,
        env_vars={"DB_HOST": "localhost", "ENV": "test"},
        volumes={"/host/data": {"bind": "/container/data", "mode": "ro"}},
        fail_fast=True,
        reraise=True,
    )


@pytest.fixture
def simple_dag(tmp_path):
    """A minimal DAG with a single asset for command-building tests."""
    io = il.FileIO(tmp_path)

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def my_asset(context: il.ExecutionContext) -> list[dict]:
        return [{"date": context.partition_date, "v": 1}]

    return il.DAG(my_asset(io=io))


class TestDockerRunnerInit:
    """Initialization and default parameter tests."""

    def test_default_params(self, runner):
        """Default values are applied correctly."""
        assert runner._image == "test-image:latest"
        assert runner._max_containers == 4
        assert runner._env_vars == {}
        assert runner._volumes == {}
        assert runner._fail_fast is False
        assert runner._reraise is False

    def test_custom_params(self, runner_custom):
        """Custom values are stored correctly."""
        assert runner_custom._image == "custom-image:v2"
        assert runner_custom._max_containers == 8
        assert runner_custom._env_vars == {"DB_HOST": "localhost", "ENV": "test"}
        assert runner_custom._volumes == {"/host/data": {"bind": "/container/data", "mode": "ro"}}
        assert runner_custom._fail_fast is True
        assert runner_custom._reraise is True

    def test_docker_client_initialized(self, mock_docker_client, runner):
        """Docker client is created via docker.from_env()."""
        assert runner._docker is mock_docker_client

    def test_env_vars_none_becomes_empty_dict(self, mock_docker_client):
        """Passing None for env_vars results in an empty dict."""
        r = DockerRunner(image="img", env_vars=None)
        assert r._env_vars == {}

    def test_volumes_none_becomes_empty_dict(self, mock_docker_client):
        """Passing None for volumes results in an empty dict."""
        r = DockerRunner(image="img", volumes=None)
        assert r._volumes == {}


class TestDockerRunnerCapacity:
    """Tests for the _capacity property."""

    def test_default_capacity(self, runner):
        """Default capacity is 4."""
        assert runner._capacity == 4

    def test_custom_capacity(self, runner_custom):
        """Custom capacity is reflected in _capacity."""
        assert runner_custom._capacity == 8

    def test_capacity_matches_max_containers(self, mock_docker_client):
        """Capacity equals max_containers for any value."""
        r = DockerRunner(image="img", max_containers=16)
        assert r._capacity == 16


class TestDockerRunnerToSpec:
    """Tests for the to_spec() serialization roundtrip."""

    def test_to_spec_returns_runner_spec(self, runner):
        """to_spec() returns a RunnerInstanceSpec instance."""
        spec = runner.to_spec()
        assert isinstance(spec, RunnerInstanceSpec)

    def test_to_spec_default_values(self, runner):
        """Spec captures default init kwargs."""
        spec = runner.to_spec()
        assert spec.init["image"] == "test-image:latest"
        assert spec.init["max_containers"] == 4
        assert spec.init["env_vars"] == {}
        assert spec.init["volumes"] == {}
        assert spec.init["fail_fast"] is False
        assert spec.init["reraise"] is False

    def test_to_spec_custom_values(self, runner_custom):
        """Spec captures custom init kwargs."""
        spec = runner_custom.to_spec()
        assert spec.init["image"] == "custom-image:v2"
        assert spec.init["max_containers"] == 8
        assert spec.init["env_vars"] == {"DB_HOST": "localhost", "ENV": "test"}
        assert spec.init["volumes"] == {"/host/data": {"bind": "/container/data", "mode": "ro"}}
        assert spec.init["fail_fast"] is True
        assert spec.init["reraise"] is True

    def test_to_spec_path(self, runner):
        """Spec path points to the DockerRunner class."""
        spec = runner.to_spec()
        assert spec.path == "interloper_docker.runner.DockerRunner"

    def test_to_spec_roundtrip(self, mock_docker_client):
        """Reconstructing from a spec produces an equivalent runner."""
        original = DockerRunner(
            image="roundtrip-image:v1",
            max_containers=6,
            env_vars={"KEY": "VALUE"},
            volumes={"/a": {"bind": "/b", "mode": "rw"}},
            fail_fast=True,
            reraise=True,
        )
        spec = original.to_spec()
        reconstructed = spec.reconstruct()

        assert isinstance(reconstructed, DockerRunner)
        assert reconstructed._image == original._image
        assert reconstructed._max_containers == original._max_containers
        assert reconstructed._env_vars == original._env_vars
        assert reconstructed._volumes == original._volumes
        assert reconstructed._fail_fast == original._fail_fast
        assert reconstructed._reraise == original._reraise


class TestDockerRunnerBuildCommand:
    """Tests for _build_command() CLI command construction."""

    def test_command_with_time_partition(self, runner, simple_dag):
        """TimePartition produces --date flag."""
        partition = TimePartition(dt.date(2025, 6, 15))
        cmd = runner._build_command(simple_dag, partition, "run-123")

        assert cmd[0] == "interloper"
        assert cmd[1] == "run"
        assert "--format" in cmd
        assert cmd[cmd.index("--format") + 1] == "inline"
        assert "--run-id=run-123" in cmd
        assert "--date" in cmd
        assert cmd[cmd.index("--date") + 1] == "2025-06-15"

    def test_command_with_time_partition_window(self, runner, simple_dag):
        """TimePartitionWindow produces --start-date and --end-date flags."""
        window = TimePartitionWindow(start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 31))
        cmd = runner._build_command(simple_dag, window, "run-456")

        assert "--start-date" in cmd
        assert cmd[cmd.index("--start-date") + 1] == "2025-01-01"
        assert "--end-date" in cmd
        assert cmd[cmd.index("--end-date") + 1] == "2025-01-31"

    def test_command_contains_inline_config_json(self, runner, simple_dag):
        """Command includes the serialized config JSON as the last element."""
        partition = TimePartition(dt.date(2025, 1, 1))
        cmd = runner._build_command(simple_dag, partition, "run-789")

        # The JSON config should be after --format inline and before --date
        config_json = Config(dag=simple_dag).to_json()
        assert config_json in cmd

    def test_command_unsupported_partition_raises(self, runner, simple_dag):
        """Unsupported partition types raise PartitionError."""
        unsupported = MagicMock()
        # Ensure it doesn't match TimePartition or TimePartitionWindow
        unsupported.__class__ = type("CustomPartition", (), {})

        with pytest.raises(PartitionError, match="Unsupported partition or window type"):
            runner._build_command(simple_dag, unsupported, "run-000")

    def test_command_none_partition_raises(self, runner, simple_dag):
        """None partition raises PartitionError (no matching isinstance branch)."""
        with pytest.raises(PartitionError, match="Unsupported partition or window type"):
            runner._build_command(simple_dag, None, "run-000")

    def test_command_run_id_embedded(self, runner, simple_dag):
        """Run ID is embedded in the command with the correct prefix."""
        partition = TimePartition(dt.date(2025, 3, 1))
        cmd = runner._build_command(simple_dag, partition, "abc-def-ghi")
        assert "--run-id=abc-def-ghi" in cmd


class TestDockerRunnerBuildEnv:
    """Tests for _build_env() environment variable construction."""

    def test_empty_env(self, runner):
        """No env_vars produces an empty dict."""
        assert runner._build_env() == {}

    def test_env_vars_copied(self, runner_custom):
        """Env vars are returned as a copy."""
        env = runner_custom._build_env()
        assert env == {"DB_HOST": "localhost", "ENV": "test"}
        # Mutating the copy should not affect the runner
        env["NEW_KEY"] = "new_value"
        assert "NEW_KEY" not in runner_custom._build_env()


class TestDockerRunnerBuildVolumes:
    """Tests for _build_volumes() volume mount construction."""

    def test_dict_volumes(self, runner_custom):
        """Dict volumes are passed through."""
        volumes = runner_custom._build_volumes()
        assert volumes == {"/host/data": {"bind": "/container/data", "mode": "ro"}}

    def test_list_volumes(self, mock_docker_client):
        """List-style volumes are converted to dict format."""
        r = DockerRunner(
            image="img",
            volumes=["/host/path:/container/path", "/data:/mnt/data"],
        )
        volumes = r._build_volumes()
        assert volumes == {
            "/host/path": {"bind": "/container/path", "mode": "rw"},
            "/data": {"bind": "/mnt/data", "mode": "rw"},
        }

    def test_empty_volumes(self, runner):
        """Empty volumes dict produces empty result."""
        assert runner._build_volumes() == {}


class TestDockerRunnerBuildName:
    """Tests for _build_name() container name construction."""

    def test_name_format(self, runner, simple_dag):
        """Container name follows the expected pattern."""
        TimePartition(dt.date(2025, 1, 1))
        # We need a state to access run_id
        runner._state = MagicMock()
        runner._state.run_id = "abcdef12-3456-7890"

        asset = simple_dag.assets[0]
        name = runner._build_name(asset)

        assert name.startswith("interloper-run-abcdef12")
        # Name should be lowercase with no underscores or colons
        assert "_" not in name
        assert ":" not in name
        assert name == name.lower()

    def test_name_sanitizes_special_chars(self, mock_docker_client):
        """Underscores and colons in asset keys are replaced with hyphens."""
        r = DockerRunner(image="img")
        r._state = MagicMock()
        r._state.run_id = "11112222-3333-4444"

        asset = MagicMock()
        asset.instance_key = "source_A:my_asset"

        name = r._build_name(asset)
        assert ":" not in name
        assert "_" not in name
        assert name == name.lower()
