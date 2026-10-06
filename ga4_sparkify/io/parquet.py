"""Filesystem-agnostic Parquet backend (default / reference implementation).

Works against any filesystem Spark/Hadoop can reach -- local paths, ``gs://``,
``s3a://``, ``abfs://`` -- because the path scheme is handled by the runtime's
Hadoop connectors, not by this code. No cloud-specific configuration lives here.
"""

from __future__ import annotations

import os

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from .base import Reader, Writer


class ParquetReader(Reader):
    """Reads GA4-export parquet shards from a path.

    :param path: Base path/URI containing the parquet files.
    :param run_date: Logical date, used only when ``layout="date_partitioned"``.
    :param layout: ``"flat"`` reads ``{path}/*parquet``; ``"date_partitioned"``
        reads ``{path}/{run_date}/*parquet`` (mirrors the original scheduled vs
        backfill glob behaviour).
    """

    jars: list[str] = []

    def __init__(
        self,
        path: str,
        run_date: str | None = None,
        layout: str = "flat",
    ) -> None:
        if layout not in ("flat", "date_partitioned"):
            raise ValueError(f"Unknown parquet layout: {layout!r}")
        if layout == "date_partitioned" and not run_date:
            raise ValueError("layout='date_partitioned' requires run_date")

        self.path = path
        self.run_date = run_date
        self.layout = layout

        if layout == "date_partitioned":
            self.data_path = os.path.join(path, run_date, "*parquet")
        else:
            self.data_path = os.path.join(path, "*parquet")

    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Loading parquet from: {self.data_path}")
        data = spark.read.parquet(self.data_path)
        logger.info(f"Loaded {data.count()} rows from {self.data_path}")
        return data


class ParquetWriter(Writer):
    """Writes the flattened output as parquet to any filesystem.

    :param path: Destination path/URI.
    :param mode: Spark save mode (``overwrite``, ``append``, ...).
    :param partition_by: Optional column (or list of columns) to partition by.
    """

    jars: list[str] = []

    def __init__(
        self,
        path: str,
        mode: str = "overwrite",
        partition_by: str | list[str] | None = None,
    ) -> None:
        self.path = path
        self.mode = mode
        if partition_by is None:
            self.partition_by: list[str] = []
        elif isinstance(partition_by, str):
            self.partition_by = [partition_by]
        else:
            self.partition_by = list(partition_by)

    def write(self, data: DataFrame) -> None:
        logger.info(f"Writing parquet to: {self.path} (mode={self.mode})")
        writer = data.write.mode(self.mode)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(self.path)
        logger.info(f"Write complete: {self.path}")
