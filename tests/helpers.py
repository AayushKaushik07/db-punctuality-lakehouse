"""Helpers to build tiny, hand-written DataFrames in the exact bronze schema."""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from dbpipe.ingest.contract import OPTIONAL, REQUIRED

BRONZE_SCHEMA = T.StructType(
    [T.StructField(name, dtype) for name, dtype in {**REQUIRED, **OPTIONAL}.items()]
    + [
        T.StructField("source_month", T.StringType()),
        T.StructField("_ingested_at", T.TimestampType()),
    ]
)


def stop(
    id: str,
    ride: str,
    seq: int,
    eva: str | None = "8000096",
    arr: datetime | None = None,
    dep: datetime | None = None,
    arr_delay: int = 0,
    dep_delay: int = 0,
    train_type: str = "ICE",
    month: str = "2025-09",
    canceled: bool = False,
    changes: bool = True,
    **extra,
) -> dict:
    """One stop record. Planned times are given; actual = planned + delay (if `changes`)."""
    arr_change = arr + timedelta(minutes=arr_delay) if (arr and changes) else None
    dep_change = dep + timedelta(minutes=dep_delay) if (dep and changes) else None
    row = {name: None for name in BRONZE_SCHEMA.names}
    row.update(
        id=id,
        station_name=f"Station {eva}",
        eva=eva,
        train_number="123",
        line_number=None,
        train_type=train_type,
        final_destination_station="Somewhere Hbf",
        delay_in_min=arr_delay if arr else dep_delay,
        time=arr_change or dep_change or arr or dep,
        arrival_is_canceled=canceled and arr is not None,
        departure_is_canceled=canceled and dep is not None,
        train_line_ride_id=ride,
        train_line_station_num=seq,
        arrival_planned_time=arr,
        arrival_change_time=arr_change,
        departure_planned_time=dep,
        departure_change_time=dep_change,
        is_additional_stop=False,
        is_replacement_train=False,
        source_month=month,
        _ingested_at=datetime(2025, 11, 1, 12, 0),
    )
    row.update(extra)
    return row


def bronze_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    return spark.createDataFrame([tuple(r[c] for c in BRONZE_SCHEMA.names) for r in rows], BRONZE_SCHEMA)
