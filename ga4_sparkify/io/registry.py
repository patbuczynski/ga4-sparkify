"""Backend registry: maps a config ``type`` string to a Reader/Writer factory.

The BigQuery backend is imported lazily inside its factory so that importing the
core package never requires the optional ``[bigquery]`` extra / connector.
"""

from __future__ import annotations

from typing import Callable

from .base import Reader, Writer
from .gcs import GCSReader, GCSWriter
from .parquet import ParquetReader, ParquetWriter


def _parquet_reader(params: dict, run_date: str | None) -> Reader:
    return ParquetReader(run_date=run_date, **params)


def _gcs_reader(params: dict, run_date: str | None) -> Reader:
    return GCSReader(run_date=run_date, **params)


def _bigquery_reader(params: dict, run_date: str | None) -> Reader:
    from .bigquery import BigQueryReader

    return BigQueryReader(**params)


def _parquet_writer(params: dict, run_date: str | None) -> Writer:
    return ParquetWriter(**params)


def _gcs_writer(params: dict, run_date: str | None) -> Writer:
    return GCSWriter(**params)


def _bigquery_writer(params: dict, run_date: str | None) -> Writer:
    from .bigquery import BigQueryWriter

    return BigQueryWriter(run_date=run_date, **params)


READERS: dict[str, Callable[[dict, str | None], Reader]] = {
    "parquet": _parquet_reader,
    "gcs": _gcs_reader,
    "bigquery": _bigquery_reader,
}

WRITERS: dict[str, Callable[[dict, str | None], Writer]] = {
    "parquet": _parquet_writer,
    "gcs": _gcs_writer,
    "bigquery": _bigquery_writer,
}


def get_reader(backend_type: str, params: dict, run_date: str | None = None) -> Reader:
    if backend_type not in READERS:
        raise ValueError(
            f"Unknown source type {backend_type!r}. Available: {sorted(READERS)}"
        )
    return READERS[backend_type](params, run_date)


def get_writer(backend_type: str, params: dict, run_date: str | None = None) -> Writer:
    if backend_type not in WRITERS:
        raise ValueError(
            f"Unknown sink type {backend_type!r}. Available: {sorted(WRITERS)}"
        )
    return WRITERS[backend_type](params, run_date)
