"""Job orchestration: wire a Reader, a transform and a Writer together."""

from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from ..io.base import Reader, Writer


class BaseJob:
    """Reads via ``reader``, transforms, then writes via ``writer``.

    Subclasses implement :meth:`transform`. The reader and writer are the storage
    boundary; ``spark`` is supplied by the caller so the engine stays the user's
    choice.
    """

    def __init__(self, reader: Reader, writer: Writer, spark: SparkSession) -> None:
        self.reader = reader
        self.writer = writer
        self.spark = spark

    def transform(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError("Subclasses must implement transform()")

    def run(self) -> None:
        data = self.reader.read(self.spark)
        logger.info("Data loaded. Starting transform")

        out = self.transform(data)

        # DataFrame-native emptiness check (Spark Connect safe).
        if out.isEmpty():
            logger.warning("No data to write after transform. Skipping write")
            return

        logger.info("Transform complete. Starting write")
        self.writer.write(out)
