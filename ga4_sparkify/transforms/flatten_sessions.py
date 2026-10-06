"""Pure GA4 session-aggregation transform.

Like :mod:`ga4_sparkify.transforms.flatten_events`, this module is free of any
I/O or SparkSession configuration: it takes a DataFrame in the GA4
BigQuery-export schema and returns one row per session. That purity is what
makes the job engine-agnostic -- it runs identically on local Spark, Dataproc,
EMR, Databricks or BigQuery managed Spark.

Ported from the original session ``SparkJob.process_data`` with the same
engine-neutrality / correctness fixes folded in:

- ``F.lit.alias(...)`` calls building the default structs are corrected to
  ``F.lit("").alias(...)`` (the originals referenced the ``F.lit`` function
  object, which has no ``.alias`` and would raise at build time);
- the ``event_paramas`` typo in the drop list is fixed to ``event_params``;
- single-value ``event_params`` lookups use ``get(array, 0)`` instead of ``[0]``
  indexing, so a missing key yields NULL rather than raising INVALID_ARRAY_INDEX
  under Spark 4 / ANSI (see ``_param`` in ``_compute_session_features``).
"""

from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

import pyspark.sql.functions as F


def flatten_sessions(data: DataFrame) -> DataFrame:
    """Aggregate GA4 export events into the defined session-level output."""

    logger.info("Starting session aggregation process")

    data = _add_missing_columns(data)
    data = _select_relevant_columns(data)
    data = _compute_session_features(data)
    data = _clean(data)
    data = _aggregate_sessions(data)

    logger.info("Session aggregation process completed")

    return data


def _add_missing_columns(data: DataFrame) -> DataFrame:
    """Add default traffic-source columns if missing, to tolerate schema drift."""

    logger.info("Checking for missing columns and adding defaults if necessary")

    manual_campaign_schema = StructType([
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("term", StringType(), True),
        StructField("content", StringType(), True),
        StructField("source_platform", StringType(), True),
        StructField("creative_format", StringType(), True),
        StructField("marketing_tactic", StringType(), True),
    ])

    google_ads_campaign_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("ad_group_id", StringType(), True),
        StructField("ad_group_name", StringType(), True),
    ])

    cross_channel_campaign_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("source_platform", StringType(), True),
        StructField("default_channel_group", StringType(), True),
        StructField("primary_channel_group", StringType(), True),
    ])

    sa360_campaign_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("ad_group_id", StringType(), True),
        StructField("ad_group_name", StringType(), True),
        StructField("creative_format", StringType(), True),
        StructField("engine_account_name", StringType(), True),
        StructField("engine_account_type", StringType(), True),
        StructField("manager_account_name", StringType(), True),
    ])

    cm360_campaign_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("advertiser_id", StringType(), True),
        StructField("advertiser_name", StringType(), True),
        StructField("creative_id", StringType(), True),
        StructField("creative_format", StringType(), True),
        StructField("creative_name", StringType(), True),
        StructField("creative_type", StringType(), True),
        StructField("creative_type_id", StringType(), True),
        StructField("creative_version", StringType(), True),
        StructField("placement_id", StringType(), True),
        StructField("placement_cost_structure", StringType(), True),
        StructField("placement_name", StringType(), True),
        StructField("rendering_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("site_name", StringType(), True),
    ])

    dv360_campaign_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("advertiser_id", StringType(), True),
        StructField("advertiser_name", StringType(), True),
        StructField("creative_id", StringType(), True),
        StructField("creative_format", StringType(), True),
        StructField("creative_name", StringType(), True),
        StructField("exchange_id", StringType(), True),
        StructField("exchange_name", StringType(), True),
        StructField("insertion_order_id", StringType(), True),
        StructField("insertion_order_name", StringType(), True),
        StructField("line_item_id", StringType(), True),
        StructField("line_item_name", StringType(), True),
        StructField("partner_id", StringType(), True),
        StructField("partner_name", StringType(), True),
    ])

    # The two composite schemas below document the expected shapes; the defaults
    # are assembled explicitly to match them.
    default_collected_traffic_source = F.struct(
        F.lit("").alias("manual_campaign_id"),
        F.lit("").alias("manual_campaign_name"),
        F.lit("").alias("manual_source"),
        F.lit("").alias("manual_medium"),
        F.lit("").alias("manual_term"),
        F.lit("").alias("manual_content"),
        F.lit("").alias("manual_source_platform"),
        F.lit("").alias("manual_creative_format"),
        F.lit("").alias("manual_marketing_tactic"),
        F.lit("").alias("gclid"),
        F.lit("").alias("dclid"),
        F.lit("").alias("srsltid"),
    )

    default_session_traffic_source_last_click = F.struct(
        F.struct(*[F.lit("").alias(field.name) for field in manual_campaign_schema.fields]).alias("manual_campaign"),
        F.struct(*[F.lit("").alias(field.name) for field in google_ads_campaign_schema.fields]).alias("google_ads_campaign"),
        F.struct(*[F.lit("").alias(field.name) for field in cross_channel_campaign_schema.fields]).alias("cross_channel_campaign"),
        F.struct(*[F.lit("").alias(field.name) for field in sa360_campaign_schema.fields]).alias("sa360_campaign"),
        F.struct(*[F.lit("").alias(field.name) for field in cm360_campaign_schema.fields]).alias("cm360_campaign"),
        F.struct(*[F.lit("").alias(field.name) for field in dv360_campaign_schema.fields]).alias("dv360_campaign"),
    )

    column_defaults = {
        "session_traffic_source_last_click": default_session_traffic_source_last_click,
        "collected_traffic_source": default_collected_traffic_source,
    }

    for col_name, default_value in column_defaults.items():
        if col_name not in data.columns:
            data = data.withColumn(col_name, default_value)
            logger.debug(f"Added missing column: {col_name}")

    return data


def _select_relevant_columns(data: DataFrame) -> DataFrame:
    """Narrow to the columns the session aggregation needs."""

    logger.info("Selecting relevant columns")

    return data.select(
        "event_date", "event_timestamp", "event_name", "event_params",
        "user_id", "user_pseudo_id", "device", "geo",
        "session_traffic_source_last_click", "collected_traffic_source", "traffic_source",
    )


def _compute_session_features(data: DataFrame) -> DataFrame:
    """Derive per-session window features (start/end, landing/exit page, ...)."""

    logger.info("Computing session-based features")

    session_date = F.to_date(F.col("event_date"), "yyyyMMdd")
    event_timestamp = F.col("event_timestamp")

    # Pull a single event_param value by key. `get(array, 0)` tolerates a missing
    # key (empty filtered array) and returns NULL; plain `[0]` indexing raises
    # INVALID_ARRAY_INDEX under Spark 4 / ANSI. The original ran on a runtime
    # where out-of-bounds returned NULL -- get() keeps that semantics portably.
    def _param(key: str, value_type: str) -> str:
        return f"get(filter(event_params, param -> param.key = '{key}').value.{value_type}, 0)"

    custom_timestamp = F.coalesce(
        F.expr(_param("custom_timestamp", "int_value")),
        (event_timestamp / 1000).cast("long"),
    )

    ga_session_id = F.expr(_param("ga_session_id", "int_value"))

    session_id = F.concat(F.col("user_pseudo_id"), ga_session_id)

    session_window = (
        Window.partitionBy(session_id)
        .orderBy(custom_timestamp)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    ga_session_number = F.max(
        F.expr(_param("ga_session_number", "int_value"))
    ).over(session_window)

    user_id = F.max(F.col("user_id")).over(session_window)

    page_location = F.expr(_param("page_location", "string_value"))
    page_referrer = F.expr(_param("page_referrer", "string_value"))

    session_start_timestamp = F.first(custom_timestamp).over(session_window)
    session_end_timestamp = F.last(custom_timestamp).over(session_window)
    session_landing_page = F.first(page_location).over(session_window)
    session_exit_page = F.last(page_location).over(session_window)
    session_referrer_page = F.first(page_referrer).over(session_window)

    session_engaged = F.expr(_param("session_engaged", "string_value"))

    collected_traffic_source = F.first("collected_traffic_source", ignorenulls=True).over(session_window)
    session_traffic_source_last_click = F.first("session_traffic_source_last_click", ignorenulls=True).over(session_window)
    traffic_source = F.first("traffic_source", ignorenulls=True).over(session_window)
    device = F.first("device").over(session_window)
    geo = F.first("geo").over(session_window)

    data = data.withColumns({
        "session_id": session_id,
        "session_date": session_date,
        "user_id": user_id,
        "ga_session_id": ga_session_id,
        "ga_session_number": ga_session_number,
        "session_start_timestamp": session_start_timestamp,
        "session_end_timestamp": session_end_timestamp,
        "session_landing_page": session_landing_page,
        "session_exit_page": session_exit_page,
        "session_referrer_page": session_referrer_page,
        "session_engaged": session_engaged,
        "collected_traffic_source": collected_traffic_source,
        "session_traffic_source_last_click": session_traffic_source_last_click,
        "traffic_source": traffic_source,
        "device": device,
        "geo": geo,
    })

    logger.info("Session features computed successfully")
    return data


def _clean(data: DataFrame) -> DataFrame:
    """Drop the per-event columns no longer needed after feature derivation."""

    logger.info("Dropping unnecessary columns")

    data = data.drop("event_date", "event_timestamp", "event_name", "event_params")

    logger.info("DataFrame cleaned")
    return data


def _aggregate_sessions(data: DataFrame) -> DataFrame:
    """Collapse to one row per session with the engagement flag."""

    logger.info("Aggregating session-level data")

    data = data.groupBy(
        "session_id", "session_date", "session_start_timestamp", "session_end_timestamp",
        "ga_session_id", "ga_session_number", "user_pseudo_id", "user_id",
        "session_landing_page", "session_exit_page", "session_referrer_page",
        "session_traffic_source_last_click", "collected_traffic_source", "traffic_source", "device", "geo",
    ).agg(
        F.when(F.max("session_engaged") == "1", True).otherwise(False).alias("is_engaged_session")
    )

    logger.info("Session aggregation completed successfully")
    return data
