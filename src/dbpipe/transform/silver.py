"""Stage 3 – bronze -> silver: clean, validate, de-duplicate and enrich with PySpark.

What happens here (in this order):
1. standardise text columns and derive analysis columns (delays, date, hour, stop role,
   journey id)
2. validate every row; rows breaking a rule go to silver.quarantine with the reason(s)
3. remove duplicate stop records (the upstream fetch job sometimes captured an hour twice)
4. reconstruct each journey and measure how much delay was gained per segment
   (window functions over the journey, ordered by stop sequence)
5. keep only stops whose service date lies in the file's own month

Journeys: `train_line_ride_id` in the source identifies a *recurring timetable pattern*
(e.g. ICE 711 Köln -> Stuttgart, the same id every day), not one day's run. A single
journey is identified by the stop id without its last part:
    id = "<pattern>-<trip start yymmddHHMM>-<stop number>"   ->   journey = "<pattern>-<trip start>"
"""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dbpipe.config import Settings
from dbpipe.lakehouse import read_table, write_table
from dbpipe.logging_utils import get_logger

log = get_logger("silver")

MAX_DELAY_MIN = 24 * 60  # anything above one day late is treated as a data error
MIN_DELAY_MIN = -30  # trains may be early, but not by more than half an hour


def _minutes_between(later: Column, earlier: Column) -> Column:
    return F.round((F.unix_timestamp(later) - F.unix_timestamp(earlier)) / 60).cast("int")


def standardise(df: DataFrame) -> DataFrame:
    """Trim/normalise text and derive the columns every later stage needs."""
    for c in ("station_name", "eva", "train_number", "line_number", "final_destination_station", "id"):
        df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
    planned_event = F.coalesce("arrival_planned_time", "departure_planned_time")
    return df.withColumns(
        {
            "train_type": F.upper(F.trim("train_type")),
            # no change message from the API = train ran as planned -> actual = planned
            "arrival_actual_time": F.coalesce("arrival_change_time", "arrival_planned_time"),
            "departure_actual_time": F.coalesce("departure_change_time", "departure_planned_time"),
            "arrival_delay_min": _minutes_between(
                F.coalesce("arrival_change_time", "arrival_planned_time"), F.col("arrival_planned_time")
            ),
            "departure_delay_min": _minutes_between(
                F.coalesce("departure_change_time", "departure_planned_time"), F.col("departure_planned_time")
            ),
            "is_canceled": F.coalesce("arrival_is_canceled", F.lit(False))
            | F.coalesce("departure_is_canceled", F.lit(False)),
            "service_date": F.to_date(planned_event),
            "event_hour": F.hour(planned_event),
            # Spark: 1=Sunday..7=Saturday  ->  0=Monday..6=Sunday
            "weekday": (F.dayofweek(planned_event) + 5) % 7,
            "stop_role": F.when(F.col("arrival_planned_time").isNull(), "origin")
            .when(F.col("departure_planned_time").isNull(), "terminus")
            .otherwise("intermediate"),
            "journey_id": journey_id(F.col("id"), F.col("train_line_ride_id"), planned_event),
        }
    )


def journey_id(stop_id: Column, pattern_id: Column, planned_event: Column) -> Column:
    """'<pattern>-<trip start>-<stop no>' -> '<pattern>-<trip start>'.

    Falls back to pattern + planned date if an id doesn't follow that format."""
    from_id = F.regexp_extract(stop_id, r"^(.+)-[0-9]+$", 1)
    fallback = F.concat_ws("|", pattern_id, F.date_format(planned_event, "yyMMdd"))
    return F.when(from_id != "", from_id).otherwise(fallback)


def validation_reasons(df: DataFrame) -> DataFrame:
    """Attach an array column `dq_reasons` listing every rule the row breaks."""
    month_start = F.to_date(F.concat(F.col("source_month"), F.lit("-01")))
    rules = {
        "missing_id": F.col("id").isNull(),
        "missing_eva": F.col("eva").isNull(),
        "missing_ride_id": F.col("train_line_ride_id").isNull(),
        "missing_planned_time": F.col("arrival_planned_time").isNull() & F.col("departure_planned_time").isNull(),
        "delay_out_of_range": (F.col("delay_in_min") > MAX_DELAY_MIN)
        | (F.col("delay_in_min") < MIN_DELAY_MIN)
        | (F.col("arrival_delay_min") > MAX_DELAY_MIN)
        | (F.col("departure_delay_min") > MAX_DELAY_MIN),
        "departure_before_arrival": F.col("departure_planned_time") < F.col("arrival_planned_time"),
        # plausibility: a file may contain stops from the day before/after its month (those are
        # dropped later, not quarantined); anything further away is a data error
        "date_outside_source_month": (F.col("service_date") < F.date_sub(month_start, 1))
        | (F.col("service_date") > F.date_add(F.last_day(month_start), 1)),
    }
    reasons = [F.when(cond, F.lit(name)) for name, cond in rules.items()]
    return df.withColumn("dq_reasons", F.filter(F.array(*reasons), lambda x: x.isNotNull()))


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one record per stop `id`: the most complete one, then the most recently ingested."""
    completeness = F.col("arrival_change_time").isNotNull().cast("int") + F.col(
        "departure_change_time"
    ).isNotNull().cast("int")
    w = Window.partitionBy("id").orderBy(completeness.desc(), F.col("_ingested_at").desc(), F.col("time").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def add_delay_propagation(df: DataFrame) -> DataFrame:
    """Follow each journey stop by stop and compute the delay gained on each segment.

    Note: before 2025-11 the dataset only covers ~100 big stations, so a "segment" is the
    stretch between two consecutive *covered* stations, not necessarily adjacent stations.
    """
    ride = Window.partitionBy("journey_id").orderBy("train_line_station_num")
    prev_dep_delay = F.lag("departure_delay_min").over(ride)
    prev_canceled = F.lag("departure_is_canceled").over(ride)
    prev_dep_planned = F.lag("departure_planned_time").over(ride)
    return df.withColumns(
        {
            "prev_eva": F.lag("eva").over(ride),
            # scheduled running time from the previous covered stop (to compare long and short segments)
            "segment_planned_min": _minutes_between(F.col("arrival_planned_time"), prev_dep_planned),
            "prev_station_name": F.lag("station_name").over(ride),
            "delay_gain_min": F.when(
                (~F.coalesce("arrival_is_canceled", F.lit(False)))
                & (~F.coalesce(prev_canceled, F.lit(False)))
                & prev_dep_delay.isNotNull(),
                F.col("arrival_delay_min") - prev_dep_delay,
            ),
        }
    )


def silver_columns() -> list:
    """Final silver column list (a function: Spark Column objects need an active session)."""
    return [
        "id",
        "source_month",
        "service_date",
        "event_hour",
        "weekday",
        "eva",
        "station_name",
        "xml_station_name",
        "train_type",
        "train_number",
        "line_number",
        F.concat_ws(" ", "train_type", "train_number").alias("train_label"),
        F.col("journey_id").alias("ride_id"),  # one train on one day
        F.col("train_line_ride_id").alias("line_ride_id"),  # the recurring timetable pattern
        F.col("train_line_station_num").alias("stop_seq"),
        "stop_role",
        "final_destination_station",
        F.col("arrival_planned_time").alias("arrival_planned_ts"),
        F.col("arrival_actual_time").alias("arrival_actual_ts"),
        F.col("departure_planned_time").alias("departure_planned_ts"),
        F.col("departure_actual_time").alias("departure_actual_ts"),
        "arrival_delay_min",
        "departure_delay_min",
        F.col("delay_in_min").alias("source_delay_min"),
        "arrival_is_canceled",
        "departure_is_canceled",
        "is_canceled",
        "is_additional_stop",
        "is_replacement_train",
        "prev_eva",
        "prev_station_name",
        "segment_planned_min",
        "delay_gain_min",
    ]


def in_source_month() -> Column:
    return F.date_format("service_date", "yyyy-MM") == F.col("source_month")


def build_silver(
    bronze: DataFrame, persist_level: StorageLevel | None = None
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Pure transformation (no I/O) so it can be unit-tested.

    Returns (clean, quarantine, outside_month):
    outside_month = valid stops dated just before/after the file's month. Each monthly file
    contains a few hundred of them (e.g. late trains from the evening before). They belong
    to the neighbouring month's file, so they are counted but not kept.

    persist_level: keep the expensive intermediate result (after de-duplication and the
    journey window functions) so clean and outside_month don't recompute it.
    """
    checked = validation_reasons(standardise(bronze))
    quarantine = checked.where(F.size("dq_reasons") > 0).withColumn("dq_reasons", F.array_join("dq_reasons", ","))
    valid = checked.where(F.size("dq_reasons") == 0).drop("dq_reasons")
    # propagation runs BEFORE the month filter, so the first stop after midnight on the 1st
    # still knows the delay of the stop before it
    enriched = add_delay_propagation(deduplicate(valid)).select(*silver_columns())
    if persist_level is not None:
        enriched = enriched.persist(persist_level)
    outside_month = enriched.where(~in_source_month())
    clean = enriched.where(in_source_month())
    # does our recomputed delay agree with the dataset's own delay_in_min?
    clean = clean.withColumn(
        "delay_matches_source",
        F.coalesce(F.abs(F.col("source_delay_min") - F.col("arrival_delay_min")) <= 1, F.lit(False))
        | F.coalesce(F.abs(F.col("source_delay_min") - F.col("departure_delay_min")) <= 1, F.lit(False)),
    )
    return clean, quarantine, outside_month


def run_silver(spark: SparkSession, settings: Settings) -> None:
    months = settings.months

    def bronze_months() -> DataFrame:
        return read_table(spark, settings, "bronze", "stop_events").where(F.col("source_month").isin(months))

    # A year is ~24 M rows: keep the expensive intermediate result on DISK, not in memory.
    # Caching it in memory made laptops with 16 GB RAM run out of memory.
    clean, quarantine, outside = build_silver(bronze_months(), persist_level=StorageLevel.DISK_ONLY)

    write_table(clean, settings, "silver", "stop_events", partition_by=["source_month"], replace_months=months)
    write_table(quarantine, settings, "silver", "quarantine", partition_by=["source_month"], replace_months=months)

    # per-month run metrics for the quality stage and the report. Counts come from the tables
    # just written (cheap to read) instead of keeping big DataFrames cached.
    written = read_table(spark, settings, "silver", "stop_events").where(F.col("source_month").isin(months))
    written_q = read_table(spark, settings, "silver", "quarantine").where(F.col("source_month").isin(months))
    b = bronze_months().groupBy("source_month").agg(F.count("*").alias("bronze_rows"))
    q = written_q.groupBy("source_month").agg(F.count("*").alias("quarantined_rows"))
    o = outside.groupBy("source_month").agg(F.count("*").alias("outside_month_rows"))
    s = written.groupBy("source_month").agg(
        F.count("*").alias("silver_rows"),
        F.avg(F.col("delay_matches_source").cast("double")).alias("delay_agreement_rate"),
    )
    metrics = (
        b.join(q, "source_month", "left")
        .join(o, "source_month", "left")
        .join(s, "source_month", "left")
        .fillna(0, subset=["quarantined_rows", "outside_month_rows", "silver_rows"])
        .withColumn(
            "duplicates_removed",
            F.col("bronze_rows") - F.col("quarantined_rows") - F.col("outside_month_rows") - F.col("silver_rows"),
        )
        .withColumn("run_ts", F.current_timestamp())
    )
    write_table(metrics, settings, "silver", "_run_metrics", partition_by=["source_month"], replace_months=months)

    for r in (
        read_table(spark, settings, "silver", "_run_metrics")
        .where(F.col("source_month").isin(months))
        .orderBy("source_month")
        .collect()
    ):
        log.info(
            "%s: bronze=%s quarantined=%s outside-month=%s duplicates=%s silver=%s delay-agreement=%.1f%%",
            r["source_month"],
            f"{r['bronze_rows']:,}",
            f"{r['quarantined_rows']:,}",
            f"{r['outside_month_rows']:,}",
            f"{r['duplicates_removed']:,}",
            f"{r['silver_rows']:,}",
            100 * (r["delay_agreement_rate"] or 0),
        )
    spark.catalog.clearCache()
