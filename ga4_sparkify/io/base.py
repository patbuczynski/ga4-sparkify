"""Storage-agnostic I/O contracts.

A :class:`Reader` is the *input* boundary: whatever the backend, its ``read``
must return a Spark DataFrame in the canonical GA4 BigQuery-export schema
(``user_pseudo_id``, ``event_name``, ``event_date``, ``event_params`` [key/value
structs], ``items``, ...). Any source that yields that schema is valid.

A :class:`Writer` is the *output* boundary: it accepts the defined flattened
output DataFrame and persists it wherever the backend targets.

Both expose ``jars`` -- the Maven coordinates of any Spark connector the backend
needs. :func:`ga4_sparkify.session.build_local_session` collects these into
``spark.jars.packages`` for local/dev runs. On managed runtimes (Dataproc,
Databricks, BigQuery managed Spark, EMR) the injected session already provides
them, so ``jars`` is simply ignored.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class Reader(ABC):
    """Reads GA4-export data from some storage backend into a DataFrame."""

    #: Maven coordinates of connector jars required by this backend.
    jars: list[str] = []

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """Return a DataFrame in the canonical GA4 BigQuery-export schema."""
        raise NotImplementedError


class Writer(ABC):
    """Persists the flattened output DataFrame to some storage backend."""

    #: Maven coordinates of connector jars required by this backend.
    jars: list[str] = []

    @abstractmethod
    def write(self, data: DataFrame) -> None:
        """Write the transformed data to the destination."""
        raise NotImplementedError
