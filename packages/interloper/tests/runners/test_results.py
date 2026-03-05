"""Tests for runner results."""

import datetime as dt

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


class TestAssetExecutionInfoMarkCancelled:
    """Tests for AssetExecutionInfo.mark_cancelled."""

    def test_mark_cancelled_sets_status_and_end_time(self):
        """Test that mark_cancelled sets status to CANCELLED and records end_time."""
        info = il.AssetExecutionInfo(
            asset_key="test_asset",
            status=il.ExecutionStatus.RUNNING,
        )
        info.mark_cancelled()
        assert info.status == il.ExecutionStatus.CANCELLED
        assert info.end_time is not None


class TestAssetExecutionInfoToDict:
    """Tests for AssetExecutionInfo.to_dict."""

    def test_to_dict_with_defaults(self):
        """Test to_dict returns correct dict when no times or error are set."""
        info = il.AssetExecutionInfo(
            asset_key="my_asset",
            status=il.ExecutionStatus.QUEUED,
        )
        d = info.to_dict()
        assert d == {
            "asset_key": "my_asset",
            "status": "queued",
            "start_time": None,
            "end_time": None,
            "execution_time": None,
            "error": None,
        }

    def test_to_dict_with_all_fields(self):
        """Test to_dict returns correct dict after running and failing."""
        info = il.AssetExecutionInfo(
            asset_key="my_asset",
            status=il.ExecutionStatus.QUEUED,
        )
        info.mark_running()
        info.mark_failed("something broke")
        d = info.to_dict()
        assert d["asset_key"] == "my_asset"
        assert d["status"] == "failed"
        assert d["start_time"] == info.start_time.isoformat()
        assert d["end_time"] == info.end_time.isoformat()
        assert isinstance(d["execution_time"], float)
        assert d["error"] == "something broke"


class TestRunResultStr:
    """Tests for RunResult.__str__."""

    def test_str_partition_none(self):
        """Test __str__ with partition_or_window=None shows partition=None."""
        result = il.RunResult()
        s = str(result)
        assert "partition=None" in s
        assert s.startswith("RunResult(")
        assert s.endswith(")")

    def test_str_with_partition(self):
        """Test __str__ with a TimePartition shows partition=..."""
        partition = il.TimePartition(value=dt.date(2025, 1, 15))
        result = il.RunResult(partition_or_window=partition)
        s = str(result)
        assert "partition=2025-01-15" in s
        assert "window=" not in s

    def test_str_with_window(self):
        """Test __str__ with a TimePartitionWindow shows window=..."""
        window = il.TimePartitionWindow(
            start=dt.date(2025, 1, 1),
            end=dt.date(2025, 1, 3),
        )
        result = il.RunResult(partition_or_window=window)
        s = str(result)
        assert "window=2025-01-01:2025-01-03" in s
        assert "partition=" not in s

    def test_str_no_failures_omits_failed_assets(self):
        """Test __str__ without failures does not include failed_assets."""
        result = il.RunResult()
        result.asset_executions = {
            "a": il.AssetExecutionInfo("a", il.ExecutionStatus.COMPLETED),
        }
        s = str(result)
        assert "failed_assets=" not in s
        assert "failed=0" in s

    def test_str_with_failures_includes_failed_assets(self):
        """Test __str__ with failures includes the list of failed asset names."""
        result = il.RunResult()
        result.asset_executions = {
            "ok": il.AssetExecutionInfo("ok", il.ExecutionStatus.COMPLETED),
            "bad1": il.AssetExecutionInfo("bad1", il.ExecutionStatus.FAILED),
            "bad2": il.AssetExecutionInfo("bad2", il.ExecutionStatus.FAILED),
        }
        s = str(result)
        assert "failed=2" in s
        assert "failed_assets=[bad1, bad2]" in s

    def test_str_with_more_than_five_failures(self):
        """Test __str__ with >5 failures shows first 5 and '+N more'."""
        result = il.RunResult()
        result.asset_executions = {
            f"fail_{i}": il.AssetExecutionInfo(f"fail_{i}", il.ExecutionStatus.FAILED)
            for i in range(7)
        }
        s = str(result)
        assert "failed=7" in s
        assert "+2 more" in s
        # Only first 5 names should appear before "+N more"
        assert "failed_assets=[" in s
