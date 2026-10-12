"""Ad-hoc Spark SQL against the lakehouse.

Every gold table is available as a view named gold_<table> (e.g. gold_fct_stop_events),
the silver stop events as silver_stop_events.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from dbpipe.config import Settings
from dbpipe.lakehouse import register_view, table_exists
from dbpipe.logging_utils import get_logger
from dbpipe.transform.gold import render_sql

log = get_logger("query")


def register_all_views(spark: SparkSession, settings: Settings) -> list[str]:
    views = []
    gold_dir = settings.path("lakehouse") / "gold"
    for path in sorted(p for p in gold_dir.iterdir() if p.is_dir()) if gold_dir.exists() else []:
        if table_exists(settings, "gold", path.name):
            views.append(register_view(spark, settings, "gold", path.name))
    if table_exists(settings, "silver", "stop_events"):
        views.append(register_view(spark, settings, "silver", "stop_events"))
    return views


def run_query(spark: SparkSession, settings: Settings, sql: str | None, limit: int = 30) -> None:
    views = register_all_views(spark, settings)
    if not sql:
        log.info("Available views: %s", ", ".join(views))
        log.info('Usage: python -m dbpipe query "SELECT * FROM gold_dim_station LIMIT 5"')
        return
    text = Path(sql).read_text(encoding="utf-8") if sql.endswith(".sql") else sql
    df = spark.sql(render_sql(text, settings.section("analytics")))
    df.show(limit, truncate=False)
