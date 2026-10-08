"""Google Cloud Storage backend.

Reads/writes GA4 data on ``gs://`` paths in a user-chosen file **format**
(``parquet`` by default, but any Spark data source: ``csv``, ``json``, ``avro``,
``orc`` ...). Over a plain filesystem read it adds:

1. the **GCS Hadoop connector** jar, pinned so a local/dev session built by
   :func:`ga4_sparkify.session.build_local_session` can reach ``gs://`` with no
   manual ``spark.jars.packages`` wiring;
2. for ``format="avro"``, the matching ``spark-avro`` jar (the GA4 BigQuery
   export is AVRO), version-matched to the running PySpark;
3. validation that the configured path is a ``gs://`` URI.

Format-specific behaviour (CSV header/delimiter, schema inference, ...) is passed
through ``options``. On managed runtimes (Dataproc, Dataproc Serverless) the
connectors already ship with the cluster, so the pinned jars are ignored.
"""

from __future__ import annotations

import os
from typing import Any

import pyspark
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from .base import Reader, Writer

#: GCS Hadoop connector -- a JVM jar loaded via ``spark.jars.packages``, not a
#: pip package. Override in the ``spark.packages`` config to match your runtime.
GCS_CONNECTOR = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.21"


def _require_gcs_uri(path: str) -> None:
    if not isinstance(path, str) or not path.startswith("gs://"):
        raise ValueError(f"GCS backend requires a gs:// path, got {path!r}")


def _jars_for_format(fmt: str) -> list[str]:
    """Connector jars a given format needs, on top of the GCS connector."""
    jars = [GCS_CONNECTOR]
    if fmt == "avro":
        # spark-avro is an external data source; pin it to the running Spark so
        # the versions always match. Built-in formats (parquet/csv/json/orc)
        # need nothing extra.
        jars.append(f"org.apache.spark:spark-avro_2.13:{pyspark.__version__}")
    return jars


class GCSReader(Reader):
    """Reads GA4-export data from a ``gs://`` path in a chosen format.

    :param path: ``gs://`` base path/URI containing the files.
    :param run_date: Logical date, used only when ``layout="date_partitioned"``.
    :param layout: ``"flat"`` reads ``{path}``; ``"date_partitioned"`` reads
        ``{path}/{run_date}`` (mirrors the scheduled-vs-backfill split).
    :param format: Spark data source format -- ``parquet`` (default), ``csv``,
        ``json``, ``avro``, ``orc`` ...
    :param options: Format-specific read options (e.g. ``{"header": "true"}``).
    """

    def __init__(
        self,
        path: str,
        run_date: str | None = None,
        layout: str = "flat",
        format: str = "parquet",
        options: dict[str, Any] | None = None,
    ) -> None:
        _require_gcs_uri(path)
        if layout not in ("flat", "date_partitioned"):
            raise ValueError(f"Unknown GCS layout: {layout!r}")
        if layout == "date_partitioned" and not run_date:
            raise ValueError("layout='date_partitioned' requires run_date")

        self.path = path
        self.run_date = run_date
        self.layout = layout
        self.format = format.lower()
        self.options = options or {}
        self.jars = _jars_for_format(self.format)

        if layout == "date_partitioned":
            self.data_path = os.path.join(path, run_date)
        else:
            self.data_path = path

    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Loading {self.format} from: {self.data_path}")
        data = spark.read.format(self.format).options(**self.options).load(self.data_path)
        logger.info(f"Loaded {data.count()} rows from {self.data_path}")
        return data


class GCSWriter(Writer):
    """Writes the flattened output to a ``gs://`` path in a chosen format.

    :param path: ``gs://`` destination path/URI.
    :param mode: Spark save mode (``overwrite``, ``append``, ...).
    :param partition_by: Optional column (or columns) to partition by.
    :param format: Spark data source format -- ``parquet`` (default), ``csv`` ...
    :param options: Format-specific write options.
    """

    def __init__(
        self,
        path: str,
        mode: str = "overwrite",
        partition_by: str | list[str] | None = None,
        format: str = "parquet",
        options: dict[str, Any] | None = None,
    ) -> None:
        _require_gcs_uri(path)
        self.path = path
        self.mode = mode
        self.format = format.lower()
        self.options = options or {}
        self.jars = _jars_for_format(self.format)
        if partition_by is None:
            self.partition_by: list[str] = []
        elif isinstance(partition_by, str):
            self.partition_by = [partition_by]
        else:
            self.partition_by = list(partition_by)

    def write(self, data: DataFrame) -> None:
        logger.info(f"Writing {self.format} to: {self.path} (mode={self.mode})")
        writer = data.write.format(self.format).mode(self.mode).options(**self.options)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.save(self.path)
        logger.info(f"Write complete: {self.path}")
