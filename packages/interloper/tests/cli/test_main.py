"""Tests for CLI script loading."""

from __future__ import annotations

import textwrap

import pytest

from interloper.cli.main import _load_script
from interloper.dag.base import DAG


def test_load_script_returns_single_dag(tmp_path) -> None:
    """Load a script file that defines exactly one DAG."""
    script = tmp_path / "single_dag.py"
    script.write_text(
        textwrap.dedent(
            """
            import interloper as il

            @il.asset
            def a(context: il.ExecutionContext):
                return 1

            dag = il.DAG(a())
            """
        ).strip()
        + "\n"
    )

    dag = _load_script(str(script))
    assert isinstance(dag, DAG)


def test_load_script_raises_when_no_dag(tmp_path) -> None:
    """Raise when the script does not define any DAG instance."""
    script = tmp_path / "no_dag.py"
    script.write_text("x = 1\n")

    with pytest.raises(ValueError, match="No DAG objects found"):
        _load_script(str(script))


def test_load_script_raises_when_multiple_dags(tmp_path) -> None:
    """Raise when the script defines more than one DAG instance."""
    script = tmp_path / "multiple_dags.py"
    script.write_text(
        textwrap.dedent(
            """
            import interloper as il

            @il.asset
            def a(context: il.ExecutionContext):
                return 1

            @il.asset
            def b(context: il.ExecutionContext):
                return 2

            dag1 = il.DAG(a())
            dag2 = il.DAG(b())
            """
        ).strip()
        + "\n"
    )

    with pytest.raises(ValueError, match="Multiple DAG objects found"):
        _load_script(str(script))
