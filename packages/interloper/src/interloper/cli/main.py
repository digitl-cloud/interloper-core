"""CLI for Interloper."""

import argparse
import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

from interloper import SerialBackfiller
from interloper.cli.config import Config
from interloper.cli.display import RichView
from interloper.dag.base import DAG
from interloper.errors import ScriptLoadError
from interloper.events.base import enable_event_forwarding, flush, subscribe
from interloper.partitioning.time import TimePartition, TimePartitionWindow
from interloper.runners.multi_thread import MultiThreadRunner
from interloper.runners.serial import SerialRunner
from interloper.serialization.config import ConfigInstanceSpec
from interloper.utils.imports import require_import


def _load_script(path: str) -> DAG:
    """Load and return the single DAG defined in a Python script.

    Returns:
        The DAG defined in the script.

    Raises:
        ScriptLoadError: If the file is missing, unloadable, or does not
            contain exactly one DAG.
    """
    script_path = Path(path).expanduser().resolve()
    if not script_path.exists():
        raise ScriptLoadError(f"Script file not found: {script_path}")
    if not script_path.is_file():
        raise ScriptLoadError(f"Script path is not a file: {script_path}")

    module_name = f"interloper_user_script_{abs(hash(str(script_path)))}"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ScriptLoadError(f"Unable to load script module from path: {script_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        # Avoid leaking user script modules into global import state.
        sys.modules.pop(module_name, None)

    dags = [obj for obj in vars(module).values() if isinstance(obj, DAG)]
    if not dags:
        raise ScriptLoadError(f"No DAG objects found in script {script_path}")
    if len(dags) > 1:
        raise ScriptLoadError(f"Multiple DAG objects found in script {script_path}")

    return dags[0]


@require_import("yaml", "`pyyaml` package not found (please install interloper's `cli` extra)")
def _config_from_yaml(path: str) -> Config:
    """Load the config from a YAML file.

    Returns:
        The parsed Config.
    """
    import yaml

    with open(path) as f:
        spec = ConfigInstanceSpec.model_validate(yaml.safe_load(f))
        return spec.reconstruct()


def _config_from_json(path: str) -> Config:
    """Load the config from a JSON file.

    Returns:
        The parsed Config.
    """
    with open(path) as f:
        spec = ConfigInstanceSpec.model_validate(json.load(f))
        return spec.reconstruct()


def _config_from_inline_json(json_data: str) -> Config:
    """Load a config from a JSON string.

    Useful for containerized workflows.

    Example:
    ```bash
    interloper run --format inline --date 2025-01-01 \
        '{"backfiller": {"type": "in_process"}, "io": {"file": {"path": "interloper.FileIO", "init": {"base_path": "data"}}}, "dag": {"assets": [{"type": "source", "path": "interloper_assets.adservice"}]}}'
    ```

    Returns:
        The parsed Config.
    """  # noqa: E501
    return Config.from_dict(json.loads(json_data))


def _config_from_script(path: str) -> Config:
    """Load a Config from a Python script containing a DAG.

    Returns:
        The Config built from the script's DAG.
    """
    return Config(
        backfiller=SerialBackfiller(),
        dag=_load_script(path),
    )


def _config_from_args(args: argparse.Namespace) -> Config:
    """Build a Config from parsed CLI arguments.

    Returns:
        The Config corresponding to the chosen format.

    Raises:
        ScriptLoadError: If the format is invalid.
    """
    if args.format == "script":
        return _config_from_script(args.file)
    elif args.format == "inline":
        return _config_from_inline_json(args.file)
    elif args.format == "yaml":
        return _config_from_yaml(args.file)
    elif args.format == "json":
        return _config_from_json(args.file)
    else:
        raise ScriptLoadError(f"Invalid format: {args.format}")


def _partition_or_window_from_args(args: argparse.Namespace) -> TimePartition | TimePartitionWindow | None:
    """Extract the partition or window from parsed CLI arguments.

    Returns:
        The partition, window, or None if no date arguments are set.
    """
    if args.date is not None:
        return TimePartition(args.date)
    elif args.start_date is not None and args.end_date is not None:
        return TimePartitionWindow(args.start_date, args.end_date)
    return None


def _backfill(
    config: Config,
    partition_or_window: TimePartition | TimePartitionWindow | None = None,
    windowed: bool = False,
    backfill_id: str | None = None,
) -> None:
    """Execute a backfill using the configured backfiller and runner."""
    backfiller = config.backfiller or SerialBackfiller()
    runner = config.runner or MultiThreadRunner()
    dag = config.dag

    backfiller.runner = runner

    metadata: dict[str, str] = {}
    if backfill_id:
        metadata["backfill_id"] = backfill_id
    backfiller.backfill(dag, partition_or_window, windowed=windowed, metadata=metadata or None)


def _run(
    config: Config,
    partition_or_window: TimePartition | TimePartitionWindow | None = None,
    run_id: str | None = None,
    backfill_id: str | None = None,
) -> None:
    """Execute a single run using the configured runner."""
    if config.backfiller is not None:
        print("Warning: backfiller is configured, but will be ignored when using the `run` command.")

    runner = config.runner or SerialRunner()
    dag = config.dag

    metadata: dict[str, str] = {}
    if run_id:
        metadata["run_id"] = run_id
    if backfill_id:
        metadata["backfill_id"] = backfill_id
    runner.run(dag, partition_or_window, metadata=metadata or None)


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common arguments shared by backfill and run commands."""
    parser.add_argument("file", type=str, help="Python script containing a DAG definition or YAML spec file")
    parser.add_argument(
        "--format",
        choices=["script", "yaml", "json", "inline"],
        default="script",
        help="Type of file to interpret: script (Python), yaml, or json",
    )
    parser.add_argument("--date", type=dt.date.fromisoformat, help="Single date to materialize")
    parser.add_argument("--start-date", type=dt.date.fromisoformat, help="Start date for window materialization")
    parser.add_argument("--end-date", type=dt.date.fromisoformat, help="End date for window materialization")


def _validate_date_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Validate date-related arguments."""
    has_start = args.start_date is not None
    has_end = args.end_date is not None

    if args.date is not None and (has_start or has_end):
        parser.error("Cannot use --date together with --start-date/--end-date")

    if (has_start and not has_end) or (has_end and not has_start):
        parser.error("Must provide both --start-date and --end-date for window materialization")


def main() -> None:
    """The main entrypoint for the CLI."""
    # TODO: TEMP TO BE REMOVED
    from dotenv import load_dotenv

    load_dotenv()

    # Explicitly opt in to default forwarding for CLI-driven execution.
    enable_event_forwarding()

    parser = argparse.ArgumentParser(description="Interloper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Backfill command
    backfill_parser = subparsers.add_parser("backfill")
    _add_common_arguments(backfill_parser)
    backfill_parser.add_argument("--windowed", action="store_true", help="Materialize the window as a single run")
    backfill_parser.add_argument("--backfill-id", type=str, help="ID for the backfill")

    # Run command
    run_parser = subparsers.add_parser("run")
    _add_common_arguments(run_parser)
    run_parser.add_argument("--run-id", type=str, help="ID for the run")
    run_parser.add_argument("--backfill-id", type=str, help="ID for the backfill")

    args = parser.parse_args()
    _validate_date_arguments(args, parser)

    config = _config_from_args(args)
    partition_or_window = _partition_or_window_from_args(args)

    # Rich terminal visualization
    view = RichView(dag=config.dag, partition_or_window=partition_or_window)
    subscribe(view.handle_event)
    view.start()

    try:
        if args.command == "backfill":
            if args.windowed and args.date is not None:
                parser.error("Cannot use --windowed together with --date")
            _backfill(config, partition_or_window, windowed=args.windowed, backfill_id=args.backfill_id)

        elif args.command == "run":
            _run(config, partition_or_window, run_id=args.run_id, backfill_id=args.backfill_id)
    finally:
        flush(timeout=5.0)
        view.stop()


if __name__ == "__main__":
    main()
