"""Stage 5 – data quality checks on bronze, silver and gold.

Each check returns a row (name, layer, severity, status, observed value, threshold).
Results are stored in gold.dq_results so the history of every run is visible.
An ERROR-severity failure stops the pipeline (non-zero exit code); WARN is only logged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dbpipe.config import Settings
from dbpipe.lakehouse import read_table, write_table
from dbpipe.logging_utils import get_logger

log = get_logger("quality")


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    check: str
    layer: str
    severity: str  # ERROR | WARN
    passed: bool
    observed: float
    threshold: str
    detail: str = ""


def run_quality(spark: SparkSession, settings: Settings) -> list[CheckResult]:
    q = settings.section("quality")
    months = settings.months
    results: list[CheckResult] = []

    def add(check, layer, severity, passed, observed, threshold, detail=""):
        results.append(CheckResult(check, layer, severity, bool(passed), float(observed), str(threshold), detail))

    bronze = read_table(spark, settings, "bronze", "stop_events").where(F.col("source_month").isin(months))
    silver = read_table(spark, settings, "silver", "stop_events")
    metrics = read_table(spark, settings, "silver", "_run_metrics").where(F.col("source_month").isin(months))
    fct = read_table(spark, settings, "gold", "fct_stop_events")
    dim_station = read_table(spark, settings, "gold", "dim_station")
    dim_date = read_table(spark, settings, "gold", "dim_date")

    # ---- bronze: completeness of key columns --------------------------------------
    key_nulls = bronze.agg(
        F.count("*").alias("n"),
        F.sum((F.col("id").isNull() | F.col("eva").isNull() | F.col("train_line_ride_id").isNull()).cast("int")).alias(
            "bad"
        ),
    ).first()
    rate = (key_nulls["bad"] or 0) / max(key_nulls["n"], 1)
    add("null_key_rate", "bronze", "ERROR", rate <= q["max_null_key_rate"], rate, f"<= {q['max_null_key_rate']}")

    # ---- silver: per-month volume, quarantine share, agreement with source ---------
    for r in metrics.collect():
        m = r["source_month"]
        qrate = r["quarantined_rows"] / max(r["bronze_rows"], 1)
        add(
            f"quarantine_rate[{m}]",
            "silver",
            "ERROR",
            qrate <= q["max_quarantine_rate"],
            qrate,
            f"<= {q['max_quarantine_rate']}",
        )
        agree = r["delay_agreement_rate"] or 0.0
        add(
            f"delay_agreement_rate[{m}]",
            "silver",
            "WARN",
            agree >= q["min_delay_agreement_rate"],
            agree,
            f">= {q['min_delay_agreement_rate']}",
            "recomputed delay vs. delay_in_min from the dataset",
        )

    loaded = {r["source_month"] for r in silver.select("source_month").distinct().collect()}
    missing = sorted(set(months) - loaded)
    add("all_months_present", "silver", "ERROR", not missing, len(missing), "0 missing", ",".join(missing))

    days = (
        silver.where(F.col("source_month").isin(months))
        .groupBy("source_month")
        .agg(F.countDistinct("service_date").alias("d"))
    )
    for r in days.collect():
        add(f"days_covered[{r['source_month']}]", "silver", "WARN", r["d"] >= 25, r["d"], ">= 25")

    # journey reconstruction: consecutive stops of one journey must be different stations.
    # (If journeys were built from the recurring timetable pattern instead, almost every
    # "segment" would go from a station to itself.)
    seg = (
        silver.where(F.col("prev_eva").isNotNull())
        .agg(F.count("*").alias("n"), F.sum((F.col("prev_eva") == F.col("eva")).cast("int")).alias("same"))
        .first()
    )
    self_rate = (seg["same"] or 0) / max(seg["n"], 1)
    add(
        "journey_self_segment_rate",
        "silver",
        "ERROR",
        self_rate <= 0.01,
        self_rate,
        "<= 0.01",
        "share of segments that start and end at the same station",
    )

    dup = silver.groupBy("id").count().where("count > 1").count()
    add("unique_stop_id", "silver", "ERROR", dup == 0, dup, "0 duplicates")

    # ---- gold: model integrity ------------------------------------------------------
    n_silver, n_fct = silver.count(), fct.count()
    add(
        "fct_rowcount_matches_silver",
        "gold",
        "ERROR",
        n_silver == n_fct,
        n_fct - n_silver,
        "difference 0",
        "a join must not add or drop rows",
    )

    dup_st = dim_station.groupBy("eva").count().where("count > 1").count()
    add("dim_station_unique_eva", "gold", "ERROR", dup_st == 0, dup_st, "0 duplicates")

    orphans = fct.join(dim_station, "eva", "left_anti").count()
    add("fct_station_referential_integrity", "gold", "ERROR", orphans == 0, orphans, "0 orphans")

    missing_dates = (
        fct.select("service_date").distinct().join(dim_date, fct.service_date == dim_date.date, "left_anti").count()
    )
    add("dim_date_covers_facts", "gold", "ERROR", missing_dates == 0, missing_dates, "0 missing dates")

    bad_pct = (
        read_table(spark, settings, "gold", "agg_punctuality_daily")
        .where((F.col("punctuality_pct") < 0) | (F.col("punctuality_pct") > 100))
        .count()
    )
    add("punctuality_pct_in_range", "gold", "ERROR", bad_pct == 0, bad_pct, "0 rows outside 0..100")

    comp = read_table(spark, settings, "gold", "agg_official_comparison").where(
        F.col("official_punctuality_pct").isNotNull()
    )
    for r in comp.collect():
        diff = abs(r["difference_pp"] or 0)
        add(
            f"official_gap[{r['period']}|{r['segment']}]",
            "gold",
            "WARN",
            diff <= 15,
            diff,
            "<= 15 pp",
            "plausibility vs. DB published numbers",
        )

    _store_and_report(spark, settings, results)
    return results


def _store_and_report(spark: SparkSession, settings: Settings, results: list[CheckResult]) -> None:
    run_ts = datetime.now(UTC).isoformat(timespec="seconds")
    rows = [
        (run_ts, r.check, r.layer, r.severity, "PASS" if r.passed else "FAIL", r.observed, r.threshold, r.detail)
        for r in results
    ]
    df = spark.createDataFrame(
        rows,
        "run_ts string, check string, layer string, severity string, status string, "
        "observed double, threshold string, detail string",
    )
    write_table(df, settings, "gold", "dq_results", mode="append")

    for r in results:
        level = log.info if r.passed else (log.error if r.severity == "ERROR" else log.warning)
        level(
            "[%s] %-45s %s observed=%s (%s)",
            "PASS" if r.passed else "FAIL",
            r.check,
            r.severity,
            round(r.observed, 4),
            r.threshold,
        )

    errors = [r for r in results if not r.passed and r.severity == "ERROR"]
    warns = [r for r in results if not r.passed and r.severity == "WARN"]
    log.info("Quality: %d checks, %d errors, %d warnings", len(results), len(errors), len(warns))
    if errors:
        raise DataQualityError(f"{len(errors)} data quality check(s) failed: {[e.check for e in errors]}")
