"""BigQuery backend (optional -- ``pip install ga4-sparkify[bigquery]``).

Ports the original Dataproc read/write paths onto the :class:`Reader`/
:class:`Writer` contracts. The spark-bigquery connector is a JVM jar (declared in
``jars`` for local runs via ``spark.jars.packages``); there is no pip dependency
to import here, so the module loads even when the connector is absent -- it only
fails at ``read``/``write`` time if the jar is genuinely missing from the session.
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from .base import Reader, Writer

#: spark-bigquery connector (Scala 2.13, matching PySpark 4.x). Override via config
#: if your runtime pins a different version.
BIGQUERY_CONNECTOR = (
    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1"
)


class BigQueryReader(Reader):
    """Reads GA4-export data directly from BigQuery.

    Uses the AVRO read format by default: the Arrow reader fails on GA4's nested
    ``array<struct>`` columns (e.g. ``event_params``, ``items``), while AVRO reads
    them correctly.

    :param table: Fully-qualified source table (``project.dataset.events_*``).
    :param query: SQL query to read instead of a table (requires
        ``materialization_dataset``).
    :param read_format: ``"AVRO"`` (default) or ``"ARROW"``.
    :param materialization_dataset: Dataset used to materialize ``query`` results.
    """

    jars = [BIGQUERY_CONNECTOR]

    def __init__(
        self,
        table: str | None = None,
        query: str | None = None,
        read_format: str = "AVRO",
        materialization_dataset: str | None = None,
    ) -> None:
        if bool(table) == bool(query):
            raise ValueError("Provide exactly one of `table` or `query`")
        if query and not materialization_dataset:
            raise ValueError("`query` requires `materialization_dataset`")

        self.table = table
        self.query = query
        self.read_format = read_format
        self.materialization_dataset = materialization_dataset

    def read(self, spark: SparkSession) -> DataFrame:
        reader = (
            spark.read.format("bigquery")
            .option("readDataFormat", self.read_format)
        )
        if self.query:
            logger.info("Loading from BigQuery query")
            reader = (
                reader.option("viewsEnabled", "true")
                .option("materializationDataset", self.materialization_dataset)
                .option("query", self.query)
            )
        else:
            logger.info(f"Loading from BigQuery table: {self.table}")
            reader = reader.option("table", self.table)

        data = reader.load()
        return data


class BigQueryWriter(Writer):
    """Writes the flattened output to BigQuery.

    Preserves the original write semantics:

    * ``write_mode="scheduled"`` -- overwrite a single day's partition (via the
      ``datePartition`` decorator); requires ``indirect`` write method.
    * ``write_mode="backfill"`` -- overwrite the whole table.

    :param write_method: ``"indirect"`` (staged through ``temporary_bucket``, the
        only method that supports single-partition overwrite) or ``"direct"``
        (Storage Write API, no bucket -- whole-table replace only).
    """

    jars = [BIGQUERY_CONNECTOR]

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        run_date: str | None = None,
        write_mode: str = "backfill",
        partition_field: str | None = None,
        cluster_fields: str | None = None,
        write_method: str = "indirect",
        temporary_bucket: str | None = None,
    ) -> None:
        if write_mode not in ("scheduled", "backfill"):
            raise ValueError(f"Unknown write_mode: {write_mode!r}")
        if write_method not in ("indirect", "direct"):
            raise ValueError(f"Unknown write_method: {write_method!r}")
        if write_mode == "scheduled" and write_method != "indirect":
            raise ValueError(
                "write_mode='scheduled' (single-partition overwrite) requires "
                "write_method='indirect'"
            )
        if write_method == "indirect" and not temporary_bucket:
            raise ValueError("write_method='indirect' requires temporary_bucket")

        self.project = project
        self.dataset = dataset
        self.table = table
        self.run_date = run_date
        self.run_date_nodash = run_date.replace("-", "") if run_date else None
        self.write_mode = write_mode
        self.partition_field = partition_field
        self.cluster_fields = cluster_fields
        self.write_method = write_method
        self.temporary_bucket = temporary_bucket

    @property
    def target(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    def write(self, data: DataFrame) -> None:
        spark = data.sparkSession
        if self.write_method == "indirect":
            spark.conf.set("temporaryGcsBucket", self.temporary_bucket)

        writer = (
            data.write.format("bigquery")
            .option("writeMethod", self.write_method)
            .option("intermediateFormat", "orc")
            .option("allowFieldAddition", "true")
            .option("allowFieldRelaxation", "true")
        )

        if self.partition_field:
            writer = writer.option("partitionField", self.partition_field).option(
                "partitionType", "DAY"
            )
            # Scheduled runs overwrite only the current day's partition.
            if self.write_mode == "scheduled":
                writer = writer.option("datePartition", self.run_date_nodash)
        if self.cluster_fields:
            writer = writer.option("clusteredFields", self.cluster_fields)

        # Scheduled non-partitioned writes append; everything else overwrites.
        if self.write_mode == "scheduled" and not self.partition_field:
            mode = "append"
        else:
            mode = "overwrite"

        logger.info(
            f"Writing to BigQuery {self.target} "
            f"(mode={self.write_mode}, method={self.write_method}, save={mode})"
        )
        writer.mode(mode).save(self.target)
        logger.info(f"Write complete: {self.target}")
