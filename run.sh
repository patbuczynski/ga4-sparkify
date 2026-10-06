#!/bin/bash
# Local dev run: flatten page_view events from data/*.parquet into out/ as parquet.
# Requires an installed package (`uv sync` / `pip install -e .`) and a JDK.

export JAVA_HOME="/opt/homebrew/opt/openjdk@21"

# The bundled data/ is ~1 GB across 130 shards; give the local driver room for the
# pivot/shuffle. Bump higher for the full set, or point source.path at a subset.
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"

ga4-sparkify run --config examples/flatten_events_local.yaml
