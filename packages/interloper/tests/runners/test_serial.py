"""Tests for SerialRunner."""

import datetime as dt

import pytest

import interloper as il


class TestSerialRunner:
    """Tests for SerialRunner."""

    def test_initialization(self, dag):
        """Test InProcessRunner initialization."""
        runner = il.SerialRunner()
        assert isinstance(runner, il.SerialRunner)

    def test_materialize_non_partitioned(self, dag):
        """Test materialize for non-partitioned DAG."""
        runner = il.SerialRunner()
        result = runner.run(dag=dag)

        # All assets should be called exactly once, with correct upstream parameters
        for name in ["a", "b", "c", "d", "e", "f", "g"]:
            dag.asset_map[name].func.assert_called_once()

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert result.completed_assets == ["a", "b", "c", "d", "e", "f", "g"]
        assert result.failed_assets == []

    def test_materialize_partitioned(self, dag_partitioned):
        """Test materialize with partition."""
        runner = il.SerialRunner()
        result = runner.run(dag=dag_partitioned, partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert result.completed_assets == ["a", "b", "c", "d", "e", "f", "g"]
        assert result.failed_assets == []

    def test_materialize_mixed(self, dag_mixed):
        """Execute a mixed DAG with a single partition and assert all assets run once."""
        runner = il.SerialRunner()
        result = runner.run(dag=dag_mixed, partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)
        for name in ["a", "b", "c", "e"]:
            dag_mixed.asset_map[name].func.assert_called_once()

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert result.completed_assets == ["a", "b", "c", "e"]
        assert result.failed_assets == []

    def test_fail_fast_true(self, dag):
        """When fail_fast=True, execution stops on first failure."""
        # Make asset 'c' fail when executed
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.SerialRunner(fail_fast=True, reraise=False)
        result = runner.run(dag=dag)

        assert result.status == "failed"
        # 'c' must be in failed; 'g' cannot be executed since depends on downstream
        assert "c" in result.failed_assets
        # Some upstream assets likely executed before failure
        assert len(result.completed_assets) >= 1

    def test_fail_fast_false(self, dag):
        """When fail_fast=False, continue executing independent branches."""
        # Make asset 'c' fail; branch via d->f should still proceed
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.SerialRunner(fail_fast=False, reraise=False)
        result = runner.run(dag=dag)

        assert result.status == il.ExecutionStatus.FAILED
        # 'c' failed, but d and f (depending only on a) can succeed
        assert "c" in result.failed_assets
        assert "d" in result.completed_assets
        assert "f" in result.completed_assets

    def test_reraise_true(self, dag):
        """When reraise=True, exceptions are re-raised regardless of fail_fast."""
        # Make asset 'c' fail when executed
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.SerialRunner(fail_fast=False, reraise=True)

        # Should raise the exception instead of returning a result
        with pytest.raises(RuntimeError, match="boom"):
            runner.run(dag=dag)

    def test_reraise_false_fail_fast_true(self, dag):
        """When reraise=False and fail_fast=True, exceptions are not re-raised at top level."""
        # Make asset 'c' fail when executed
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.SerialRunner(fail_fast=True, reraise=False)
        result = runner.run(dag=dag)

        # Should return failed result, not raise exception
        assert result.status == "failed"
        assert "c" in result.failed_assets

    def test_reraise_false_fail_fast_false(self, dag):
        """When reraise=False and fail_fast=False, exceptions are not re-raised."""
        # Make asset 'c' fail; branch via d->f should still proceed
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.SerialRunner(fail_fast=False, reraise=False)
        result = runner.run(dag=dag)

        assert result.status == il.ExecutionStatus.FAILED
        # 'c' failed, but d and f (depending only on a) can succeed
        assert "c" in result.failed_assets
        assert "d" in result.completed_assets
        assert "f" in result.completed_assets

    def test_runner_with_identical_asset_names(self, double_source_dag):
        """Test SerialRunner with assets that have identical names but different keys."""
        runner = il.SerialRunner()
        result = runner.run(dag=double_source_dag, partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))

        assert result.status == il.ExecutionStatus.COMPLETED

        # Should have 4 assets executed (2 from each source)
        assert len(result.completed_assets) == 4

        # All assets should have been called exactly once
        for asset in double_source_dag.assets:
            asset.func.assert_called_once()

        # Verify that both sources' assets were executed
        # The assets should be tracked by their keys, not names
        executed_keys = set(result.completed_assets)
        expected_keys = {asset.key for asset in double_source_dag.assets}
        assert executed_keys == expected_keys

