"""Tests for MultiProcessRunner."""

import datetime as dt

import pytest

import interloper as il


@pytest.mark.skip()
class TestMultiProcessRunner:
    """Tests for MultiProcessRunner."""

    def test_initialization(self, file_based_dag):
        """Test MultiProcessRunner initialization."""
        runner = il.MultiProcessRunner()
        assert runner._max_workers == 4  # default
        assert runner._fail_fast is True  # default

    def test_initialization_with_max_workers(self, file_based_dag):
        """Test MultiProcessRunner initialization with custom max_workers."""
        runner = il.MultiProcessRunner(max_workers=8, fail_fast=False)
        assert runner._max_workers == 8
        assert runner._fail_fast is False

    def test_materialize_non_partitioned(self, file_based_dag, tmp_path):
        """Test materialize for non-partitioned DAG."""
        runner = il.MultiProcessRunner()
        result = runner.run(dag=file_based_dag)

        assert result.status == il.ExecutionStatus.COMPLETED
        assert len(result.completed_assets) == 3

        # Verify that the output files were created with the correct content
        data_dir = tmp_path / "data"
        import pickle

        # Check that the pickle files were created and contain the right data
        asset_a_file = data_dir / "asset_a" / "data.pkl"
        asset_b_file = data_dir / "asset_b" / "data.pkl"
        asset_c_file = data_dir / "asset_c" / "data.pkl"

        assert asset_a_file.exists()
        assert asset_b_file.exists()
        assert asset_c_file.exists()

        with asset_a_file.open("rb") as f:
            assert pickle.load(f) == "a_ran"
        with asset_b_file.open("rb") as f:
            assert pickle.load(f) == "b_ran"
        with asset_c_file.open("rb") as f:
            assert pickle.load(f) == "c_ran_with_a_ran"

    def test_fail_fast_true(self, file_based_dag, tmp_path):
        """When fail_fast=True, execution stops on first failure."""
        # Create a simple DAG with just two assets where one fails
        from .. import assets as test_assets

        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        # Use module-level assets but override their IO to use tmp_path
        asset_a_fails = test_assets.asset_a_fails()(io=il.FileIO(str(data_dir)))
        asset_b_success = test_assets.asset_b_success()(io=il.FileIO(str(data_dir)))

        dag = il.DAG(asset_a_fails, asset_b_success)

        runner = il.MultiProcessRunner(fail_fast=True)
        result = runner.run(dag=dag)

        assert result.status == "failed"
        assert "asset_a_fails" in result.failed_assets

    def test_fail_fast_false(self, file_based_dag, tmp_path):
        """When fail_fast=False, continue executing independent branches."""
        # Create a simple DAG with two independent assets where one fails
        from .. import assets as test_assets

        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)

        # Use module-level assets but override their IO to use tmp_path
        asset_a_fails = test_assets.asset_a_fails()(io=il.FileIO(str(data_dir)))
        asset_b_success = test_assets.asset_b_success()(io=il.FileIO(str(data_dir)))

        dag = il.DAG(asset_a_fails, asset_b_success)

        runner = il.MultiProcessRunner(fail_fast=False)
        result = runner.run(dag=dag)

        assert result.status == il.ExecutionStatus.COMPLETED  # The run itself is successful, even if some assets fail
        assert "asset_a_fails" in result.failed_assets
        assert "asset_b_success" in result.completed_assets

        # 'b' should have run successfully - check the pickle file
        import pickle

        asset_b_file = data_dir / "asset_b_success" / "data.pkl"
        assert asset_b_file.exists()
        with asset_b_file.open("rb") as f:
            assert pickle.load(f) == "b_success_ran"

    def test_runner_with_identical_asset_names(self, double_source_dag):
        """Test MultiProcessRunner with assets that have identical names but different keys."""
        runner = il.MultiProcessRunner(max_workers=2)
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

