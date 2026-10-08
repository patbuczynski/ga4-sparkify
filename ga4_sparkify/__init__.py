"""ga4-sparkify: storage- and service-agnostic Spark jobs for GA4 export data."""

from __future__ import annotations

from .cli import run_from_config
from .config import JobConfig, load_config
from .io import (
    GCSReader,
    GCSWriter,
    ParquetReader,
    ParquetWriter,
    Reader,
    Writer,
    get_reader,
    get_writer,
)
from .jobs import BaseJob, FlattenEventsJob, FlattenSessionsJob
from .session import build_local_session
from .transforms import flatten_events, flatten_sessions

__all__ = [
    "run_from_config",
    "load_config",
    "JobConfig",
    "Reader",
    "Writer",
    "ParquetReader",
    "ParquetWriter",
    "GCSReader",
    "GCSWriter",
    "get_reader",
    "get_writer",
    "BaseJob",
    "FlattenEventsJob",
    "FlattenSessionsJob",
    "build_local_session",
    "flatten_events",
    "flatten_sessions",
]
