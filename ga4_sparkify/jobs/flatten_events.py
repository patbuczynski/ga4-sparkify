"""The flatten-events job."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ..io.base import Reader, Writer
from ..transforms.flatten_events import flatten_events
from .base import BaseJob


class FlattenEventsJob(BaseJob):
    """Flattens GA4 events for a single event (or event pattern/list)."""

    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        spark: SparkSession,
        event_name: str,
        event_type: str = "standard",
        additional_columns_to_select: list[str] | None = None,
        event_list: list[str] | None = None,
        event_pattern: str = "",
    ) -> None:
        super().__init__(reader, writer, spark)
        self.event_name = event_name
        self.event_type = event_type
        self.additional_columns_to_select = additional_columns_to_select or []
        self.event_list = event_list or []
        self.event_pattern = event_pattern

    def transform(self, data: DataFrame) -> DataFrame:
        return flatten_events(
            data,
            event_name=self.event_name,
            event_type=self.event_type,
            additional_columns_to_select=self.additional_columns_to_select,
            event_list=self.event_list,
            event_pattern=self.event_pattern,
        )
