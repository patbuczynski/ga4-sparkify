"""Declarative job configuration loaded from YAML.

The config selects the source backend, the sink backend, the job and its
parameters, and (optionally) how to build a local SparkSession. String values
support ``${ENV_VAR}`` interpolation from the environment.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate(value: Any) -> Any:
    """Recursively expand ``${VAR}`` references in strings using os.environ."""
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    return value


@dataclass
class BackendConfig:
    """A source or sink: a backend ``type`` plus its constructor params."""

    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class SparkConfig:
    """Optional local/dev SparkSession settings. Ignored on injected sessions."""

    master: str | None = None
    conf: dict[str, str] = field(default_factory=dict)
    packages: list[str] = field(default_factory=list)


@dataclass
class JobConfig:
    job: str
    source: BackendConfig
    sink: BackendConfig
    params: dict[str, Any] = field(default_factory=dict)
    run_date: str | None = None
    spark: SparkConfig | None = None


def _backend(raw: dict[str, Any], kind: str) -> BackendConfig:
    if not raw or "type" not in raw:
        raise ValueError(f"Config `{kind}` must specify a `type`")
    params = {k: v for k, v in raw.items() if k != "type"}
    return BackendConfig(type=raw["type"], params=params)


def parse_config(raw: dict[str, Any]) -> JobConfig:
    raw = _interpolate(raw)

    if "job" not in raw:
        raise ValueError("Config must specify a `job`")
    if "source" not in raw or "sink" not in raw:
        raise ValueError("Config must specify both `source` and `sink`")

    spark_raw = raw.get("spark")
    spark = None
    if spark_raw is not None:
        spark = SparkConfig(
            master=spark_raw.get("master"),
            conf={k: str(v) for k, v in (spark_raw.get("conf") or {}).items()},
            packages=list(spark_raw.get("packages") or []),
        )

    return JobConfig(
        job=raw["job"],
        source=_backend(raw["source"], "source"),
        sink=_backend(raw["sink"], "sink"),
        params=raw.get("params") or {},
        run_date=raw.get("run_date"),
        spark=spark,
    )


def load_config(path: str) -> JobConfig:
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh)
    return parse_config(raw)
