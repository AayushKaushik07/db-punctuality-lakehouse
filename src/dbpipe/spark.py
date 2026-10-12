"""Create a SparkSession configured for this project (optionally with Delta Lake)."""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession

from dbpipe.config import Settings
from dbpipe.logging_utils import get_logger

log = get_logger("spark")


def build_spark(settings: Settings, app_name: str = "db-punctuality-lakehouse") -> SparkSession:
    cfg = settings.section("spark")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.get("master", "local[6]"))
        .config("spark.driver.memory", cfg.get("driver_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 16)))
        # Source timestamps are German local time without an offset -> interpret them in Berlin time
        .config("spark.sql.session.timeZone", cfg.get("timezone", "Europe/Berlin"))
        .config("spark.ui.showConsoleProgress", "false")
        # bronze has ~25 columns; avoids a "Truncated the string representation of a plan" warning
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.adaptive.enabled", "true")
        # The source files store timestamps in nanoseconds, which Spark can't read as timestamps.
        # Read them as numbers instead; the schema contract converts them (ingest/contract.py).
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    )

    if settings.storage_format == "delta":
        # Delta replaces single months itself (`replaceWhere` in lakehouse.write_table) and
        # refuses Spark's dynamic partition overwrite mode, so it is NOT enabled here.
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        local_jars = os.environ.get("DBPIPE_DELTA_JARS")
        if local_jars:  # offline / behind a proxy: use jars from a local folder
            jars = sorted(glob.glob(os.path.join(local_jars, "*.jar")))
            if not jars:
                raise FileNotFoundError(f"DBPIPE_DELTA_JARS={local_jars} contains no .jar files")
            builder = builder.config("spark.jars", ",".join(jars))
        else:
            from delta import configure_spark_with_delta_pip  # imported lazily: optional dependency

            # Downloads the matching Delta Lake jars from Maven on first run (cached afterwards)
            builder = configure_spark_with_delta_pip(builder)
    else:
        # Plain Parquet: only overwrite the partitions (months) being written, keep the others
        builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    _quiet_global_window_warning(spark)
    log.info(
        "Spark %s started (master=%s, storage=%s)",
        spark.version,
        cfg.get("master", "local[6]"),
        settings.storage_format,
    )
    return spark


def _quiet_global_window_warning(spark: SparkSession) -> None:
    """Silence Spark's "No Partition Defined for Window operation" warning.

    It fires for windows without PARTITION BY, which this project uses on purpose and only on
    small tables: the global hotspot ranking (~200 rows) and the network-wide daily late rate
    in the ML features (~60 rows). Without this, every run prints a dozen alarming but
    irrelevant "serious performance degradation" warnings.
    """
    try:
        jvm = spark.sparkContext._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.sql.execution.window.WindowExec", jvm.org.apache.logging.log4j.Level.ERROR
        )
    except Exception:  # logging tweaks must never break a run
        log.debug("Could not change the WindowExec log level", exc_info=True)
