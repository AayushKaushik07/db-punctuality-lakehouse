"""Read/write helpers for the lakehouse (Delta Lake or plain Parquet, same API).

Layout:  <lakehouse>/<layer>/<table>/   e.g. data/lakehouse/silver/stop_events/
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from dbpipe.config import Settings
from dbpipe.logging_utils import get_logger

log = get_logger("lakehouse")

LAYERS = ("bronze", "silver", "gold")


def table_path(settings: Settings, layer: str, name: str) -> Path:
    if layer not in LAYERS:
        raise ValueError(f"Unknown layer {layer!r}")
    return settings.path("lakehouse") / layer / name


def table_exists(settings: Settings, layer: str, name: str) -> bool:
    path = table_path(settings, layer, name)
    if settings.storage_format == "delta":
        return (path / "_delta_log").is_dir()
    # a parquet table "exists" once it holds at least one data file
    return path.is_dir() and any(path.rglob("*.parquet"))


def write_table(
    df: DataFrame,
    settings: Settings,
    layer: str,
    name: str,
    *,
    partition_by: list[str] | None = None,
    replace_months: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Write a DataFrame as a table.

    replace_months: only replace these `source_month` partitions and keep all others.
    This makes re-running a month idempotent (no duplicates, no data loss for other months).
    """
    path = str(table_path(settings, layer, name))
    fmt = settings.storage_format
    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if fmt == "delta":
        if mode == "overwrite" and replace_months and table_exists(settings, layer, name):
            months = ", ".join(f"'{m}'" for m in replace_months)
            writer = writer.option("replaceWhere", f"source_month IN ({months})")
        elif mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
    # parquet: the session uses partitionOverwriteMode=dynamic, so only the written
    # partitions are replaced when partition_by is set.

    writer.save(path)
    log.info("Wrote %s.%s (%s, mode=%s)", layer, name, fmt, mode)


def read_table(spark: SparkSession, settings: Settings, layer: str, name: str) -> DataFrame:
    if not table_exists(settings, layer, name):
        raise FileNotFoundError(
            f"Table {layer}.{name} does not exist yet at {table_path(settings, layer, name)}. "
            "Run the previous pipeline stage first."
        )
    return spark.read.format(settings.storage_format).load(str(table_path(settings, layer, name)))


def register_view(spark: SparkSession, settings: Settings, layer: str, name: str) -> str:
    """Expose a table to Spark SQL as a temp view called <layer>_<name>."""
    view = f"{layer}_{name}"
    read_table(spark, settings, layer, name).createOrReplaceTempView(view)
    return view


def read_table_pandas(settings: Settings, layer: str, name: str) -> pd.DataFrame:
    """Read a (small) table without Spark, used by the dashboard and the report."""
    path = table_path(settings, layer, name)
    if not table_exists(settings, layer, name):
        raise FileNotFoundError(f"Table {layer}.{name} not found at {path}")
    if settings.storage_format == "delta":
        from deltalake import DeltaTable

        df = DeltaTable(str(path)).to_pandas()
    else:
        df = pd.read_parquet(path)
    return _decimals_to_float(df)


def _decimals_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Spark SQL arithmetic like ROUND(100.0 * x, 2) yields DECIMAL; pandas stores those as
    Python objects, which plotting and numpy can't use. Convert them to float."""
    from decimal import Decimal

    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if not sample.empty and isinstance(sample.iloc[0], Decimal):
                df[col] = df[col].astype(float)
    return df
