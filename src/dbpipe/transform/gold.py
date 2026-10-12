"""Stage 4 – silver -> gold: dimensional model and aggregates.

Dimensions that need Python logic (train categories from config, German public holidays)
are built here; everything else is plain Spark SQL in sql/gold/, run in file order.
Each SQL file creates the table named after it (03_agg_station_monthly.sql ->
gold.agg_station_monthly) and becomes available to later files as view gold_<name>.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

import holidays
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dbpipe.config import PROJECT_ROOT, Settings
from dbpipe.lakehouse import read_table, register_view, write_table
from dbpipe.logging_utils import get_logger

log = get_logger("gold")

SQL_DIR = PROJECT_ROOT / "sql" / "gold"
PARTITIONED = {"fct_stop_events": ["source_month"]}
_PARAM = re.compile(r"\$\{(\w+)\}")


# --------------------------------------------------------------------------- dims
def build_dim_train_type(spark: SparkSession, settings: Settings) -> DataFrame:
    a = settings.section("analytics")
    ld = [t.upper() for t in a["long_distance_types"]]
    sb = [t.upper() for t in a["s_bahn_types"]]
    bus = [t.upper() for t in a["bus_types"]]
    silver = read_table(spark, settings, "silver", "stop_events")
    return (
        silver.groupBy("train_type")
        .agg(F.count("*").alias("stop_events"))
        .where(F.col("train_type").isNotNull())
        .withColumn(
            "train_category",
            F.when(F.col("train_type").isin(ld), "long_distance")
            .when(F.col("train_type").isin(sb), "s_bahn")
            .when(F.col("train_type").isin(bus), "bus")
            # everything else in the timetable is regional rail (RE, RB, IRE, private operators, ...)
            .otherwise("regional"),
        )
    )


def german_holiday_calendar(start, end) -> pd.DataFrame:
    """One row per day with nationwide holidays and how many federal states have a holiday."""
    years = list(range(start.year, end.year + 1))
    national = holidays.country_holidays("DE", years=years)
    states = holidays.country_holidays("DE").subdivisions
    by_state = {s: holidays.country_holidays("DE", subdiv=s, years=years) for s in states}

    rows = []
    day = start
    while day <= end:
        rows.append(
            {
                "date": day,
                "year": day.year,
                "month": day.month,
                "day": day.day,
                "weekday": day.weekday(),  # 0 = Monday
                "weekday_name": day.strftime("%A"),
                "iso_week": day.isocalendar()[1],
                "is_weekend": day.weekday() >= 5,
                "is_public_holiday": day in national,
                "holiday_name": national.get(day),
                "states_with_holiday": sum(day in cal for cal in by_state.values()),
            }
        )
        day += timedelta(days=1)
    return pd.DataFrame(rows)


def build_dim_date(spark: SparkSession, settings: Settings) -> DataFrame:
    silver = read_table(spark, settings, "silver", "stop_events")
    bounds = silver.agg(F.min("service_date").alias("lo"), F.max("service_date").alias("hi")).first()
    cal = german_holiday_calendar(bounds["lo"], bounds["hi"])
    return spark.createDataFrame(cal).withColumn("date", F.col("date").cast("date"))


def load_reference(spark: SparkSession, settings: Settings) -> None:
    ref = pd.read_csv(
        settings.path("reference") / "db_official_punctuality.csv",
        dtype={"period": str, "first_month": str, "last_month": str, "source": str},
    )
    spark.createDataFrame(ref).createOrReplaceTempView("ref_official_punctuality")


# --------------------------------------------------------------------------- SQL
def render_sql(text: str, params: dict) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in params:
            raise KeyError(f"SQL parameter ${{{key}}} is not defined in config 'analytics'")
        return str(params[key])

    return _PARAM.sub(repl, text)


def sql_files() -> list[Path]:
    return sorted(SQL_DIR.glob("[0-9][0-9]_*.sql"))


def table_name(sql_file: Path) -> str:
    return sql_file.stem.split("_", 1)[1]


def run_gold(spark: SparkSession, settings: Settings) -> None:
    register_view(spark, settings, "silver", "stop_events")
    load_reference(spark, settings)

    for name, builder in (("dim_train_type", build_dim_train_type), ("dim_date", build_dim_date)):
        write_table(builder(spark, settings), settings, "gold", name)
        register_view(spark, settings, "gold", name)

    params = settings.section("analytics")
    for sql_file in sql_files():
        name = table_name(sql_file)
        df = spark.sql(render_sql(sql_file.read_text(encoding="utf-8"), params))
        write_table(df, settings, "gold", name, partition_by=PARTITIONED.get(name))
        register_view(spark, settings, "gold", name)

    log.info("Gold layer built: %d SQL models + 2 Python dimensions", len(sql_files()))
