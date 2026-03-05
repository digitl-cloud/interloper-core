"""Tests for MultiProcessRunner."""

from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import interloper as il
from interloper.errors import RunnerError


class TestMultiProcessRunnerInit:
    """MultiProcessRunner constructor and defaults."""

    def test_default_init(self):
        """Default init uses 4 workers, fail_fast=True, reraise=False."""
        runner = il.MultiProcessRunner()
        assert runner._max_workers == 4
        assert runner._fail_fast is True
        assert runner._reraise is False

    def test_custom_init(self):
        """Custom init accepts max_workers, fail_fast, reraise."""
        runner = il.MultiProcessRunner(max_workers=8, fail_fast=False, reraise=True)
        assert runner._max_workers == 8
        assert runner._fail_fast is False
        assert runner._reraise is True

    def test_capacity_matches_max_workers(self):
        """_capacity property returns max_workers."""
        runner = il.MultiProcessRunner(max_workers=6)
        assert runner._capacity == 6

    def test_to_spec_roundtrip(self):
        """to_spec captures constructor args."""
        runner = il.MultiProcessRunner(max_workers=3, fail_fast=False, reraise=True)
        spec = runner.to_spec()
        assert spec.init["max_workers"] == 3
        assert spec.init["fail_fast"] is False
        assert spec.init["reraise"] is True


class TestMultiProcessRunnerLifecycle:
    """Pool lifecycle (_on_start / _on_end)."""

    def test_on_start_creates_pool(self):
        """_on_start creates a ProcessPoolExecutor."""
        runner = il.MultiProcessRunner(max_workers=2)
        assert runner._pool is None
        runner._on_start()
        assert runner._pool is not None
        runner._on_end()

    def test_on_end_shuts_down_pool(self):
        """_on_end shuts down the pool and sets it to None."""
        runner = il.MultiProcessRunner(max_workers=2)
        runner._on_start()
        assert runner._pool is not None
        runner._on_end()
        assert runner._pool is None

    def test_on_end_without_pool_is_noop(self):
        """_on_end with no pool does nothing."""
        runner = il.MultiProcessRunner()
        runner._on_end()  # Should not raise


class TestMultiProcessRunnerSubmit:
    """_submit_asset error paths."""

    def test_submit_without_pool_raises(self):
        """_submit_asset raises RunnerError when pool is None."""
        runner = il.MultiProcessRunner()
        asset = MagicMock()
        import pytest

        with pytest.raises(RunnerError, match="Pool not initialized"):
            runner._submit_asset(asset, None)


class TestMultiProcessRunnerCancel:
    """_cancel_all behaviour."""

    def test_cancel_all_cancels_futures(self):
        """_cancel_all calls cancel on each handle."""
        runner = il.MultiProcessRunner()
        f1 = MagicMock(spec=Future)
        f2 = MagicMock(spec=Future)
        runner._cancel_all([f1, f2])
        f1.cancel.assert_called_once()
        f2.cancel.assert_called_once()

    def test_cancel_all_swallows_exceptions(self):
        """_cancel_all does not propagate cancel errors."""
        runner = il.MultiProcessRunner()
        f1 = MagicMock(spec=Future)
        f1.cancel.side_effect = RuntimeError("already done")
        f2 = MagicMock(spec=Future)
        runner._cancel_all([f1, f2])
        # f2 should still be cancelled despite f1 raising
        f2.cancel.assert_called_once()


class TestMultiProcessRunnerWaitAny:
    """_wait_any behaviour with mocked futures."""

    def test_wait_any_returns_completed_future(self):
        """_wait_any returns the first completed future."""
        runner = il.MultiProcessRunner(fail_fast=False, reraise=False)
        future = MagicMock(spec=Future)
        future.result.return_value = ("asset_a", True, None)

        with patch("interloper.runners.multi_process.wait") as mock_wait:
            mock_wait.return_value = ({future}, set())
            result = runner._wait_any([future])

        assert result is future

    def test_wait_any_fail_fast_raises_on_failure(self):
        """_wait_any raises RunnerError on failure when fail_fast=True."""
        runner = il.MultiProcessRunner(fail_fast=True, reraise=False)
        failed = MagicMock(spec=Future)
        failed.result.return_value = ("asset_x", False, "boom")
        other = MagicMock(spec=Future)

        with patch("interloper.runners.multi_process.wait") as mock_wait:
            mock_wait.return_value = ({failed}, set())
            import pytest

            with pytest.raises(RunnerError, match="Asset asset_x failed: boom"):
                runner._wait_any([failed, other])

        # Other future should be cancelled (called twice: once from the
        # if-block inside try, once from the except block)
        assert other.cancel.call_count == 2

    def test_wait_any_no_fail_fast_returns_on_failure(self):
        """_wait_any returns normally on failure when fail_fast=False."""
        runner = il.MultiProcessRunner(fail_fast=False, reraise=False)
        failed = MagicMock(spec=Future)
        failed.result.return_value = ("asset_x", False, "boom")

        with patch("interloper.runners.multi_process.wait") as mock_wait:
            mock_wait.return_value = ({failed}, set())
            result = runner._wait_any([failed])

        assert result is failed
