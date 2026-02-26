"""Tests for MultiThreadRunner."""

import datetime as dt

import interloper as il


class TestMultiThreadRunner:
    """Tests for MultiThreadRunner."""

    def test_initialization(self, dag):
        """Test MultiThreadRunner initialization."""
        runner = il.MultiThreadRunner()
        assert isinstance(runner, il.MultiThreadRunner)

    def test_initialization_with_max_workers(self, dag):
        """Test MultiThreadRunner with max_workers."""
        runner = il.MultiThreadRunner(max_workers=4)
        assert runner._max_workers == 4

    def test_materialize_non_partitioned(self, dag):
        """Test materialize for non-partitioned DAG using complex topology."""
        runner = il.MultiThreadRunner(max_workers=4)
        result = runner.run(dag=dag)
        assert isinstance(result, il.RunResult)
        # All assets should be called exactly once
        for name in ["a", "b", "c", "d", "e", "f", "g"]:
            dag.asset_map[name].func.assert_called_once()

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.completed_assets) == 7
        assert set(result.completed_assets) == {"a", "b", "c", "d", "e", "f", "g"}
        assert result.failed_assets == []

        # Verify dependency order is respected
        executed = result.completed_assets
        # a and b have no dependencies
        # c depends on a, d depends on a
        assert executed.index("a") < executed.index("c")
        assert executed.index("a") < executed.index("d")
        # e depends on b and c, f depends on d
        assert executed.index("b") < executed.index("e")
        assert executed.index("c") < executed.index("e")
        assert executed.index("d") < executed.index("f")
        # g depends on e and f
        assert executed.index("e") < executed.index("g")
        assert executed.index("f") < executed.index("g")

    def test_materialize_partitioned(self, dag_partitioned):
        """Test materialize with partition."""
        runner = il.MultiThreadRunner(max_workers=2)
        result = runner.run(dag=dag_partitioned, partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.completed_assets) == 7
        assert set(result.completed_assets) == {"a", "b", "c", "d", "e", "f", "g"}
        assert result.failed_assets == []

        # Verify dependency order is respected
        executed = result.completed_assets
        # a and b have no dependencies
        # c depends on a, d depends on a
        assert executed.index("a") < executed.index("c")
        assert executed.index("a") < executed.index("d")
        # e depends on b and c, f depends on d
        assert executed.index("b") < executed.index("e")
        assert executed.index("c") < executed.index("e")
        assert executed.index("d") < executed.index("f")
        # g depends on e and f
        assert executed.index("e") < executed.index("g")
        assert executed.index("f") < executed.index("g")

    def test_materialize_mixed(self, dag_mixed):
        """Execute a mixed DAG with a single partition and assert all assets run once."""
        runner = il.MultiThreadRunner(max_workers=4)
        result = runner.run(dag=dag_mixed, partition_or_window=il.TimePartition(dt.date(2025, 1, 1)))
        assert isinstance(result, il.RunResult)
        for name in ["a", "b", "c", "e"]:
            dag_mixed.asset_map[name].func.assert_called_once()

        # Verify execution order respects dependencies
        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.completed_assets) == 4
        assert set(result.completed_assets) == {"a", "b", "c", "e"}
        assert result.failed_assets == []

        # Verify dependency order is respected
        executed = result.completed_assets
        # a and b have no dependencies
        # c depends on a
        assert executed.index("a") < executed.index("c")
        # e depends on b and c
        assert executed.index("b") < executed.index("e")
        assert executed.index("c") < executed.index("e")

    def test_fail_fast_true(self, dag):
        """When fail_fast=True, threads stop scheduling further work after a failure."""
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.MultiThreadRunner(max_workers=4, fail_fast=True)
        result = runner.run(dag=dag)

        assert result.status == "failed"
        assert "c" in result.failed_assets
        assert len(result.completed_assets) >= 1

    def test_fail_fast_false(self, dag):
        """When fail_fast=False, other ready assets continue executing in parallel."""
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        runner = il.MultiThreadRunner(max_workers=4, fail_fast=False)
        result = runner.run(dag=dag)

        assert result.status == il.ExecutionStatus.FAILED
        assert "c" in result.failed_assets
        assert "d" in result.completed_assets
        assert "f" in result.completed_assets

    def test_fail_fast_false_continues_with_single_worker(self, dag):
        """When fail_fast=False, scheduler continues after first failure."""
        dag.asset_map["c"].func.side_effect = RuntimeError("boom")

        # Single worker makes scheduling order deterministic:
        # if the run loop exits on first failure, d/f will never run.
        runner = il.MultiThreadRunner(max_workers=1, fail_fast=False, reraise=False)
        result = runner.run(dag=dag)

        assert result.status == il.ExecutionStatus.FAILED
        assert "c" in result.failed_assets
        assert "d" in result.completed_assets
        assert "f" in result.completed_assets

    def test_runner_with_identical_asset_names(self, double_source_dag):
        """Test MultiThreadRunner with assets that have identical names but different keys."""
        runner = il.MultiThreadRunner(max_workers=4)
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
        expected_keys = {asset.instance_key for asset in double_source_dag.assets}
        assert executed_keys == expected_keys

