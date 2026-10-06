# ga4-sparkify

Storage- and service-agnostic Spark jobs for flattening
[GA4 BigQuery-export](https://support.google.com/analytics/answer/7029846) event
data into a wide, pivoted schema.

The GA4-export **input format** and the **flattened output schema** are fixed. The
*storage* the data lives in (any filesystem, BigQuery, ... Snowflake later) and the
*Spark engine* that runs the job (local, Dataproc, EMR, Databricks, BigQuery
managed Spark) are your choice.

## What does it do?

The GA4 export is one row per event, with the useful part — `event_params` — hidden
in a repeated key/value array. Every query has to `UNNEST` it, and a parameter's
value can live in any of four typed fields. Shortened input schema:

```
event_date                         STRING          -- "20210101"
event_timestamp                    INT64
event_name                         STRING          -- "page_view"
event_params                       ARRAY<STRUCT<
                                     key    STRING,
                                     value  STRUCT<string_value  STRING,
                                                   int_value     INT64,
                                                   float_value   FLOAT64,
                                                   double_value  FLOAT64>>>
user_pseudo_id                     STRING
user_id                            STRING
device                             STRUCT<category, operating_system, web_info<browser, ...>, ...>
geo                                STRUCT<continent, country, region, city, ...>
items                              ARRAY<STRUCT<item_id, item_name, price, quantity, ...>>
user_properties                    ARRAY<STRUCT<key, value STRUCT<...>>>
traffic_source                     STRUCT<medium, name, source>
collected_traffic_source           STRUCT<manual_source, manual_medium, gclid, ...>
session_traffic_source_last_click  STRUCT<manual_campaign, google_ads_campaign, ...>
```

A single `page_view` event therefore arrives as one row carrying N nested
parameter entries:

| event_name | event_params.key | string_value | int_value |
|------------|------------------|--------------|-----------|
| page_view  | page_location    | `https://shop.example/cart` | |
| page_view  | page_title       | `Cart`       | |
| page_view  | ga_session_id    |              | 1609459200 |
| page_view  | engagement_time_msec | | 4210 |

Both jobs emit that data as plain top-level columns and dot-addressable structs,
so downstream SQL/BI needs no `UNNEST` at all.

### `flatten_events` — one row per event

The `event_params` array is pivoted into a struct with one named field per
parameter key found in the data:

```
user_pseudo_id    STRING
user_id           STRING
session_id        STRING              -- user_pseudo_id || ga_session_id
event_name        STRING
event_date        DATE                -- parsed from the "yyyyMMdd" string
event_timestamp   INT64
batch_event_index INT64
geo               STRUCT<...>         -- passed through unchanged
device            STRUCT<...>         -- passed through unchanged
items             ARRAY<STRUCT<...>>
user_properties   ARRAY<STRUCT<...>>
event_params      STRUCT<             -- pivoted: one field per param key
                    page_location         STRING,
                    page_title            STRING,
                    ga_session_id         STRING,
                    ga_session_number     STRING,
                    engagement_time_msec  STRING,
                    ...>
```

So the four rows above become one row, queried as
`SELECT event_params.page_location, event_params.page_title FROM ...`.

- Pivoted values are the **coalesce of the four value variants**, so every
  `event_params` field is a `STRING` — cast at the read site.
- The `items` param key is excluded from the pivot; the top-level `items` array is
  kept. With `event_type: ecommerce` it is exploded and re-collected, deduplicated
  per `(user_pseudo_id, session_id, event_timestamp)`.
- `user_pseudo_id` / `user_id` are dropped from inside the struct — they already
  exist as top-level columns.

### `flatten_sessions` — one row per session

Events are aggregated over a session window (`user_pseudo_id || ga_session_id`,
ordered by `custom_timestamp` falling back to `event_timestamp`):

```
session_id                         STRING   -- user_pseudo_id || ga_session_id
session_date                       DATE
session_start_timestamp            INT64    -- first event in the session
session_end_timestamp              INT64    -- last event in the session
ga_session_id                      INT64
ga_session_number                  INT64
user_pseudo_id                     STRING
user_id                            STRING
session_landing_page               STRING   -- first page_location
session_exit_page                  STRING   -- last page_location
session_referrer_page              STRING   -- first page_referrer
session_traffic_source_last_click  STRUCT<manual_campaign, google_ads_campaign,
                                          cross_channel_campaign, sa360_campaign,
                                          cm360_campaign, dv360_campaign>
collected_traffic_source           STRUCT<...>
traffic_source                     STRUCT<medium, name, source>
device                             STRUCT<...>
geo                                STRUCT<...>
is_engaged_session                 BOOLEAN  -- any event with session_engaged = "1"
```

Traffic-source, `device` and `geo` are taken as the first non-null value in the
session; missing traffic-source columns are defaulted, so the output schema is
stable across GA4 export versions.

## Install

```bash
uv sync                              # or: pip install -e .
pip install ga4-sparkify[bigquery]   # + the BigQuery backend
```

Requires Python >=3.11 and a JDK (Spark 4).

## Run

Jobs are described by a YAML config and run through the CLI:

```bash
ga4-sparkify run --config examples/flatten_events_local.yaml
# override any value by dotted path:
ga4-sparkify run --config examples/flatten_events_local.yaml \
  --set params.event_name=purchase --set params.event_type=ecommerce
```

`examples/flatten_events_local.yaml` reads the bundled `data/*.parquet` and writes
parquet to `out/` — no cloud credentials needed. `examples/flatten_events_bigquery.yaml`
and `examples/flatten_sessions_bigquery.yaml` are the BigQuery -> BigQuery references
for the event and session jobs. The session job takes no transform params (`params: {}`);
set `job: flatten_sessions` to run it against any source.

### Config shape

```yaml
job: flatten_events
run_date: "2021-01-01"
params:                       # -> the job's transform args
  event_name: page_view
  event_type: standard        # or "ecommerce"
  event_list: []              # filter by list of events (overrides event_name)
  event_pattern: ""           # or SQL LIKE pattern (overrides event_name)
  additional_columns_to_select: []
source:
  type: parquet               # parquet | gcs | bigquery
  path: data                  # local, gs://, s3a://, abfs:// ...
  layout: flat                # flat | date_partitioned ({path}/{run_date}/*parquet)
sink:
  type: parquet               # parquet | gcs | bigquery
  path: out/page_view
  mode: overwrite
  partition_by: event_date
spark:                        # OPTIONAL — only used when the CLI builds a session
  master: "local[*]"
  conf: { spark.sql.shuffle.partitions: "12" }
```

String values support `${ENV_VAR}` interpolation.

### Notebook

`ga4_flatten_events_notebook.ipynb` is a self-contained PySpark notebook for the
event job — the same transformation steps inlined cell by cell, with no package to
install. It reads the GA4 export straight from BigQuery through the Spark BigQuery
connector and writes the flattened table back, so it drops into BigQuery Studio
(or any managed-Spark notebook) and runs on the session that is already there.

Use it to **try the service out** — point it at one dated export shard, run the
cells, and `printSchema()` after the transformation shows you exactly the output
described above before you commit to wiring up a config and a cluster. Each step
is a separate cell, so it also doubles as the readable walk-through of what the
flattening actually does.

For anything recurring, prefer the package: the notebook's `direct` BigQuery write
replaces the whole destination table rather than a single `RUN_DATE` partition
(the notebook's own cells spell this out), and only `flatten_events` is covered.

## On a managed runtime

Omit the `spark:` block and hand the job a session you already have:

```python
from ga4_sparkify import load_config, run_from_config
run_from_config(load_config("job.yaml"), spark=spark)   # your SparkSession
```

Or assemble the pieces directly:

```python
from ga4_sparkify import FlattenEventsJob, ParquetReader, ParquetWriter
job = FlattenEventsJob(
    reader=ParquetReader("gs://bucket/ga4/"),
    writer=ParquetWriter("gs://bucket/out/", partition_by="event_date"),
    spark=spark,
    event_name="page_view",
)
job.run()
```

## Backends

| `type`     | Source / sink | Notes |
|------------|---------------|-------|
| `parquet`  | both          | Any filesystem Spark can reach (local, `gs://`, `s3a://`, `abfs://`). |
| `gcs`      | both          | `gs://` paths in a chosen `format` (`parquet` default, or `csv`/`json`/`avro`/`orc`…) with format-specific `options`; pins the GCS Hadoop connector (and `spark-avro` when `format: avro`) so local sessions reach GCS with no extra wiring. Same `layout`/`partition_by` as `parquet`. |
| `bigquery` | both          | Reads the GA4 export / writes the flattened table; needs `ga4-sparkify[bigquery]`. |

`examples/flatten_events_gcs.yaml` is the `gcs -> gcs` reference (swap the sink to
`bigquery` for the read-storage / write-BigQuery service path).

## Adding a storage backend

Implement `Reader` / `Writer` (`ga4_sparkify/io/base.py`), declare any connector
`jars`, and register a factory in `ga4_sparkify/io/registry.py`. The BigQuery
backend (`ga4_sparkify/io/bigquery.py`) is the worked example; Snowflake would
follow the same pattern.

## Caveats

- **Dump the export to storage first.** The jobs make the most sense against
  parquet (or any other file format) already sitting in a bucket. Pointing the
  `bigquery` source backend at the GA4 export works, but every run reads the table
  through the Storage Read API and incurs the normal BigQuery query costs — one
  extract, many runs is the cheaper shape.
- **Tune the cluster to the dataset.** The pivot and the session window are
  shuffle-heavy, and the right executor sizing, partition counts and
  `spark.sql.shuffle.partitions` depend entirely on how wide your `event_params`
  are and how much data a run covers. The defaults are a starting point, not a
  tuned configuration — on any engine of choice, maximum throughput and minimum
  cost come from fitting the cluster to the data.
- **Therefore: some Spark knowledge is required** to run this efficiently. The
  jobs are engine-agnostic, but they are not self-tuning.
