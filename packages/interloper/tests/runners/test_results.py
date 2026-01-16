"""Tests for runner results."""

import interloper as il


class TestExecutionStatus:
    """Tests for ExecutionStatus enum."""

    def test_enum_values(self):
        """Test ExecutionStatus enum values."""
        assert il.ExecutionStatus.QUEUED == "queued"
        assert il.ExecutionStatus.READY == "ready"
        assert il.ExecutionStatus.RUNNING == "running"
        assert il.ExecutionStatus.COMPLETED == "completed"
        assert il.ExecutionStatus.FAILED == "failed"
        assert il.ExecutionStatus.SKIPPED == "skipped"
        assert il.ExecutionStatus.CANCELLED == "cancelled"


class TestAssetExecutionInfo:
    """Tests for AssetExecutionInfo."""

    def test_initialization(self):
        """Test AssetExecutionInfo initialization."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.QUEUED,
        )
        assert info.asset_key == "test_asset"
        assert info.status == il.ExecutionStatus.QUEUED
        assert info.start_time is None
        assert info.end_time is None
        assert info.error is None

    def test_mark_running(self):
        """Test mark_running method."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.QUEUED,
        )
        info.mark_running()
        assert info.status == il.ExecutionStatus.RUNNING
        assert info.start_time is not None

    def test_mark_completed(self):
        """Test mark_completed method."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.RUNNING,
        )
        info.mark_completed()
        assert info.status == il.ExecutionStatus.COMPLETED
        assert info.end_time is not None

    def test_mark_failed(self):
        """Test mark_failed method."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.RUNNING,
        )
        info.mark_failed("Test error")
        assert info.status == il.ExecutionStatus.FAILED
        assert info.error == "Test error"
        assert info.end_time is not None

    def test_execution_time(self):
        """Test execution_time property."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.QUEUED,
        )
        assert info.execution_time is None

        info.mark_running()
        info.mark_completed()
        assert info.execution_time is not None
        assert isinstance(info.execution_time, float)


class TestRunResult:
    """Tests for RunResult."""

    def test_initialization(self):
        """Test RunResult initialization."""
        result = il.RunResult()
        assert result.partition_or_window is None
        assert result.status == il.ExecutionStatus.COMPLETED
        assert result.asset_executions == {}
        assert result.execution_time == 0.0

    def test_completed_assets(self):
        """Test completed_assets property."""
        result = il.RunResult()
        result.asset_executions = {
            "asset1": il.AssetExecutionInfo("asset1", il.ExecutionStatus.COMPLETED),
            "asset2": il.AssetExecutionInfo("asset2", il.ExecutionStatus.FAILED),
            "asset3": il.AssetExecutionInfo("asset3", il.ExecutionStatus.COMPLETED),
        }
        assert result.completed_assets == ["asset1", "asset3"]

    def test_failed_assets(self):
        """Test failed_assets property."""
        result = il.RunResult()
        result.asset_executions = {
            "asset1": il.AssetExecutionInfo("asset1", il.ExecutionStatus.COMPLETED),
            "asset2": il.AssetExecutionInfo("asset2", il.ExecutionStatus.FAILED),
            "asset3": il.AssetExecutionInfo("asset3", il.ExecutionStatus.COMPLETED),
        }
        assert result.failed_assets == ["asset2"]
