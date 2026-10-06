"""Job definitions and the job registry."""

from typing import Type

from .base import BaseJob
from .flatten_events import FlattenEventsJob
from .flatten_sessions import FlattenSessionsJob

#: Maps the config ``job:`` field to a job class.
JOBS: dict[str, Type[BaseJob]] = {
    "flatten_events": FlattenEventsJob,
    "flatten_sessions": FlattenSessionsJob,
}

__all__ = ["BaseJob", "FlattenEventsJob", "FlattenSessionsJob", "JOBS"]
