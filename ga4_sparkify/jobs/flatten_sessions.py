"""The flatten-sessions job."""

from __future__ import annotations

from pyspark.sql import DataFrame

from ..transforms.flatten_sessions import flatten_sessions
from .base import BaseJob


class FlattenSessionsJob(BaseJob):
    """Aggregates GA4 events into session-level rows.

    Takes no transform parameters -- the session grain is fixed -- so it uses
    ``BaseJob``'s ``(reader, writer, spark)`` constructor unchanged.
    """

    def transform(self, data: DataFrame) -> DataFrame:
        return flatten_sessions(data)
