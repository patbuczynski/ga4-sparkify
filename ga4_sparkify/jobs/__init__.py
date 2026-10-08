"""Job definitions and the job registry."""

from __future__ import annotations

from .base import BaseJob
from .flatten_events import FlattenEventsJob
from .flatten_sessions import FlattenSessionsJob

#: Maps the config ``job:`` field to a job class.
JOBS: dict[str, type[BaseJob]] = {
    "flatten_events": FlattenEventsJob,
    "flatten_sessions": FlattenSessionsJob,
}

__all__ = ["BaseJob", "FlattenEventsJob", "FlattenSessionsJob", "JOBS"]
