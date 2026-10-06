"""Optional SparkSession factory for local/dev runs.

Production runtimes (Dataproc, Databricks, EMR, BigQuery managed Spark) own the
SparkSession and inject it into the job. This factory is only a convenience for
local development: it spins up a ``local[*]`` session and wires the chosen
backends' connector jars into ``spark.jars.packages``.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_local_session(
    app_name: str = "ga4-sparkify",
    master: str = "local[*]",
    packages: list[str] | None = None,
    conf: dict[str, str] | None = None,
) -> SparkSession:
    """Build a local SparkSession, adding connector ``packages`` if any.

    :param packages: Maven coordinates to load via ``spark.jars.packages``
        (typically ``reader.jars + writer.jars``).
    :param conf: Extra ``spark.*`` config entries.
    """
    builder = SparkSession.builder.appName(app_name).master(master)
    builder = builder.config("spark.driver.bindAddress", "localhost")

    # De-duplicate while preserving order.
    packages = list(dict.fromkeys(packages or []))
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    for key, value in (conf or {}).items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
