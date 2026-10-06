"""Thin CLI: ``ga4-sparkify run --config job.yaml [--set a.b=c ...]``."""

from __future__ import annotations

import argparse
import sys
from typing import Any

import yaml
from loguru import logger
from pyspark.sql import SparkSession

from .config import JobConfig, parse_config
from .io.registry import get_reader, get_writer
from .jobs import JOBS
from .session import build_local_session


def run_from_config(config: JobConfig, spark: SparkSession | None = None) -> None:
    """Build the reader, writer and job from ``config`` and run it.

    If ``spark`` is not supplied, a session is obtained as follows:
    a ``spark:`` block in the config builds a local session (adding the backends'
    connector jars); otherwise the active session is used via ``getOrCreate()``
    -- the managed-runtime path.
    """
    reader = get_reader(config.source.type, config.source.params, config.run_date)
    writer = get_writer(config.sink.type, config.sink.params, config.run_date)

    if spark is None:
        if config.spark is not None:
            packages = config.spark.packages + reader.jars + writer.jars
            spark = build_local_session(
                app_name=f"ga4-sparkify-{config.job}",
                master=config.spark.master or "local[*]",
                packages=packages,
                conf=config.spark.conf,
            )
        else:
            spark = SparkSession.builder.getOrCreate()

    if config.job not in JOBS:
        raise ValueError(
            f"Unknown job {config.job!r}. Available: {sorted(JOBS)}"
        )

    job = JOBS[config.job](reader, writer, spark, **config.params)
    job.run()


def _coerce(value: str) -> Any:
    """Parse an override value as YAML so ints/bools/lists work naturally."""
    return yaml.safe_load(value)


def _apply_override(raw: dict, dotted_key: str, value: Any) -> None:
    keys = dotted_key.split(".")
    node = raw
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ga4-sparkify")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run a job from a YAML config")
    run.add_argument("--config", required=True, help="Path to the job YAML config")
    run.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a config value by dotted path, e.g. --set params.event_name=purchase",
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        with open(args.config, "r") as fh:
            raw = yaml.safe_load(fh)
        for item in args.set:
            if "=" not in item:
                parser.error(f"--set expects KEY=VALUE, got {item!r}")
            key, _, value = item.partition("=")
            _apply_override(raw, key.strip(), _coerce(value))
        config = parse_config(raw)
        logger.info(f"Running job '{config.job}' ({config.source.type} -> {config.sink.type})")
        run_from_config(config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
