"""Storage-agnostic readers and writers."""

from __future__ import annotations

from .base import Reader, Writer
from .gcs import GCSReader, GCSWriter
from .parquet import ParquetReader, ParquetWriter
from .registry import get_reader, get_writer

__all__ = [
    "Reader",
    "Writer",
    "ParquetReader",
    "ParquetWriter",
    "GCSReader",
    "GCSWriter",
    "get_reader",
    "get_writer",
]
