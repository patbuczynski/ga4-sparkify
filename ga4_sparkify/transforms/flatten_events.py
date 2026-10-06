"""Pure GA4 event-flattening transform.

This module is deliberately free of any I/O or SparkSession configuration: it
takes a DataFrame in the GA4 BigQuery-export schema and returns the defined
flattened output. That purity is what makes the job engine-agnostic -- it runs
identically on local Spark, Dataproc, EMR, Databricks or BigQuery managed Spark.

Ported from the original ``SparkJob.process_data`` with three engine-neutrality /
correctness fixes folded in (see inline notes): the ``float_value`` param variant
now actually created (the original referenced it in the pivot but never selected
it), ``rdd``-based checks replaced with DataFrame-native equivalents, and an
unconditional ``repartition`` that is safe under Spark Connect.
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

import pyspark.sql.functions as F


def flatten_events(
    data: DataFrame,
    event_name: str,
    event_type: str = "standard",
    additional_columns_to_select: list | None = None,
    event_list: list | None = None,
    event_pattern: str = "",
) -> DataFrame:
    """Flatten GA4 export events into the defined wide/pivoted output schema."""

    additional_columns_to_select = additional_columns_to_select or []
    event_list = event_list or []

    logger.info("Starting data processing pipeline")

    data = _repartition_and_filter(data, event_name, event_list, event_pattern)

    # DataFrame.isEmpty() instead of data.rdd.isEmpty() -- rdd is unavailable on
    # Spark Connect / BigQuery managed Spark.
    if data.isEmpty():
        logger.warning(
            f"No data found after filtering for event_name='{event_name}'. "
            "Skipping further processing"
        )
        return data

    data = _create_default_columns(data)
    data = _select_and_explode(data, additional_columns_to_select)
    data = _pivot(data, additional_columns_to_select)
    data = _create_additional_columns(data, additional_columns_to_select)
    if event_type == "ecommerce":
        data = _explode_items(data)
        data = _items_to_array(data)
    data = _post_process(data)

    logger.info("Data processing completed")

    return data


def _repartition_and_filter(
    data: DataFrame, event_name: str, event_list: list, event_pattern: str
) -> DataFrame:
    """Filter to the requested event(s) and repartition for the pivot/shuffle."""

    if event_list:
        data = data.where(F.col("event_name").isin(*event_list))
    elif event_pattern:
        data = data.where(F.col("event_name").like(event_pattern))
    else:
        data = data.where(F.col("event_name") == event_name)

    # Unconditional repartition (no rdd.getNumPartitions) -- Spark Connect safe.
    return data.repartition(128)


def _create_default_columns(data: DataFrame) -> DataFrame:
    """Add default columns if missing, to tolerate source schema changes."""

    column_defaults = {
        "batch_event_index": F.lit(0).cast(LongType()),
        "batch_page_id": F.lit(0).cast(LongType()),
        "batch_ordering_id": F.lit(0).cast(LongType()),
    }

    for col_name, default_value in column_defaults.items():
        if col_name not in data.columns:
            data = data.withColumn(col_name, default_value)
            logger.debug(f"Added missing column: {col_name}")

    return data


def _select_and_explode(data: DataFrame, additional_columns_to_select: list) -> DataFrame:
    """Select the relevant GA4 columns and explode ``event_params``."""

    selected_columns = [
        "user_pseudo_id", "user_id", "event_name", "event_date", "event_timestamp",
        "device", "geo", "event_params", "event_previous_timestamp", "event_value_in_usd",
        "event_bundle_sequence_id", "event_server_timestamp_offset", "stream_id", "platform",
        "event_dimensions", "batch_event_index", "batch_page_id", "batch_ordering_id", "items",
        "user_properties",
    ]

    if additional_columns_to_select:
        selected_columns.extend(additional_columns_to_select)

    data = (
        data.select(
            *[F.col(col) for col in selected_columns],
            F.explode("event_params").alias("event_param"),
        )
        .withColumn("key", F.col("event_param.key"))
        .withColumn("string_value", F.col("event_param.value.string_value"))
        .withColumn("int_value", F.col("event_param.value.int_value"))
        .withColumn("float_value", F.col("event_param.value.float_value"))
        .withColumn("double_value", F.col("event_param.value.double_value"))
    )

    return data


def _pivot(data: DataFrame, additional_columns_to_select: list) -> DataFrame:
    """Pivot the exploded key/value pairs back into an ``event_params`` struct."""

    logger.info("Pivoting data based on event parameters")
    pivot_keys = [
        row["key"]
        for row in data.where(F.col("key") != "items").select("key").distinct().collect()
    ]

    groupby_columns = [
        "user_pseudo_id", "user_id", "event_name", "event_date", "event_timestamp", "geo",
        "device", "items", "user_properties", "batch_event_index",
    ]

    if additional_columns_to_select:
        groupby_columns.extend(additional_columns_to_select)

    # Coalesce the four GA4 event_params.value variants (string/int/float/double).
    # The original job referenced `float_value` in the pivot but never created it in
    # _select_and_explode; it is now created there so float-typed params aren't lost.
    data = data.withColumn(
        "value",
        F.coalesce(
            F.col("string_value"),
            F.col("int_value").cast("string"),
            F.col("float_value").cast("string"),
            F.col("double_value").cast("string"),
        ),
    )

    data = (
        data.drop(
            "event_params", "event_param", "string_value", "int_value",
            "float_value", "double_value", "event_dimensions",
        )
        .where(F.col("key") != "items")
        .groupBy(groupby_columns)
        .pivot("key", pivot_keys)
        .agg(F.first("value"))
    )

    pivot_columns = [
        F.col(column)
        for column in data.columns
        if column
        not in (
            "key",
            "user_pseudo_id",
            "user_id",
            "event_name",
            "event_date",
            "event_timestamp",
            "geo",
            "device",
            "items",
            "user_properties",
            "batch_event_index",
            *additional_columns_to_select,
        )
    ]

    data = data.withColumn("event_params", F.struct(*pivot_columns))
    data = data.drop(*pivot_columns)

    return data


def _create_additional_columns(
    data: DataFrame, additional_columns_to_select: list
) -> DataFrame:
    """Add ``session_id`` and normalise ``event_date`` to a date type."""

    logger.info("Adding session_id and formatting event_date")
    data = data.withColumn(
        "session_id",
        F.concat(F.col("user_pseudo_id"), F.col("event_params.ga_session_id")),
    )
    data = data.withColumn("event_date", F.to_date(F.col("event_date"), "yyyyMMdd"))
    return data.select(
        "user_pseudo_id", "user_id", "session_id", "event_name", "event_date",
        "event_timestamp", "batch_event_index", "geo", "device", "items",
        "user_properties", "event_params", *additional_columns_to_select,
    )


def _explode_items(data: DataFrame) -> DataFrame:
    """Explode the ``items`` array (ecommerce events)."""
    logger.info("Exploding 'items' column")
    return data.withColumn("items", F.explode("items"))


def _items_to_array(data: DataFrame) -> DataFrame:
    """Re-collect exploded items back into a deduplicated array per event."""
    logger.info("Converting 'items' column to array")

    window = Window.partitionBy("user_pseudo_id", "session_id", "event_timestamp")
    data = data.withColumn("items", F.collect_list("items").over(window))

    return data.dropDuplicates(["user_pseudo_id", "session_id", "event_timestamp"])


def _post_process(data: DataFrame) -> DataFrame:
    """Drop identifier fields that should not live inside ``event_params``."""

    logger.info("Post-processing: removing unused fields from 'event_params'")
    event_params_fields_to_drop = ["user_pseudo_id", "user_id"]

    data = data.withColumn(
        "event_params", F.col("event_params").dropFields(*event_params_fields_to_drop)
    )

    return data
