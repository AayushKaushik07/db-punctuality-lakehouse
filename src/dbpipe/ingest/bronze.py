"""Stage 2 – load landing files into the bronze layer (raw data, validated schema).

* one partition per `source_month`, replaced atomically -> re-running is idempotent
* incremental: a month is skipped when its file checksum equals the last loaded one
* every load is recorded in bronze._ingestion_log (audit trail)
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbpipe.config import Settings
from dbpipe.ingest.contract import enforce_contract
from dbpipe.ingest.download import landing_file, read_manifest, update_manifest
from dbpipe.lakehouse import read_table, table_exists, write_table
from dbpipe.logging_utils import get_logger

log = get_logger("bronze")

LOG_SCHEMA = T.StructType(
    [
        T.StructField("source_month", T.StringType()),
        T.StructField("source_sha256", T.StringType()),
        T.StructField("source_bytes", T.LongType()),
        T.StructField("row_count", T.LongType()),
        T.StructField("missing_optional_cols", T.StringType()),
        T.StructField("dropped_extra_cols", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)


def _last_loaded_checksums(spark: SparkSession, settings: Settings) -> dict[str, str]:
    if not table_exists(settings, "bronze", "_ingestion_log"):
        return {}
    rows = (
        read_table(spark, settings, "bronze", "_ingestion_log")
        .groupBy("source_month")
        .agg(F.max_by("source_sha256", "ingested_at").alias("sha"))
        .collect()
    )
    return {r["source_month"]: r["sha"] for r in rows}


def run_bronze(spark: SparkSession, settings: Settings, force: bool = False) -> list[str]:
    """Load all configured months. Returns the list of months that were (re)loaded."""
    manifest = read_manifest(settings)
    loaded_before = _last_loaded_checksums(spark, settings)
    loaded_now: list[str] = []

    for month in settings.months:
        src = landing_file(settings, month)
        if not src.exists():
            raise FileNotFoundError(f"{src} not found. Run `make download` (or `python -m dbpipe download`) first.")
        entry = manifest.get(month) or update_manifest(settings, month, source="local")

        if not force and loaded_before.get(month) == entry["sha256"]:
            log.info("%s unchanged since last load (sha256 match) – skipping", month)
            continue

        raw = spark.read.parquet(str(src))
        df, report = enforce_contract(raw, source=src.name)
        ingested_at = datetime.now(UTC).isoformat(timespec="seconds")
        df = df.withColumns(
            {
                "source_month": F.lit(month),
                "_source_file": F.lit(entry["file"]),
                "_source_sha256": F.lit(entry["sha256"]),
                "_ingested_at": F.lit(ingested_at).cast("timestamp"),
            }
        )

        write_table(df, settings, "bronze", "stop_events", partition_by=["source_month"], replace_months=[month])
        row_count = (
            spark.read.format(settings.storage_format)
            .load(str(settings.path("lakehouse") / "bronze" / "stop_events"))
            .where(F.col("source_month") == month)
            .count()
        )

        log_row = spark.createDataFrame(
            [
                (
                    month,
                    entry["sha256"],
                    int(entry["bytes"]),
                    int(row_count),
                    ",".join(report.missing_optional),
                    ",".join(report.dropped_extra),
                    ingested_at,
                )
            ],
            LOG_SCHEMA,
        )
        write_table(log_row, settings, "bronze", "_ingestion_log", mode="append")
        log.info("%s loaded into bronze: %s rows", month, f"{row_count:,}")
        loaded_now.append(month)

    return loaded_now
