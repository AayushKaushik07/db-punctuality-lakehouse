"""Command-line entry point:  python -m dbpipe <stage> [options]

Stages (in pipeline order):
  download   fetch monthly files from Hugging Face into data/landing
  bronze     validate schema and load raw data            (PySpark)
  silver     clean, de-duplicate, quarantine, enrich        (PySpark)
  gold       star schema + aggregates                        (Spark SQL)
  quality    data quality checks (fails on errors)
  features   leak-free ML features                           (PySpark)
  train      train and evaluate the delay model              (scikit-learn)
  report     charts + docs/RESULTS.md
  run        bronze -> ... -> report (everything except download)
  sample     write a small SYNTHETIC sample into data/landing (tests / smoke runs)
  query      run Spark SQL against the gold tables, e.g.
             python -m dbpipe query sql/analysis/01_bad_day_streaks.sql
"""

from __future__ import annotations

import argparse
import time

from dbpipe.config import load_settings
from dbpipe.logging_utils import get_logger

log = get_logger("cli")

SPARK_STAGES = ("bronze", "silver", "gold", "quality", "features", "query")
RUN_ORDER = ("bronze", "silver", "gold", "quality", "features", "train", "report")


def _run_stage(stage: str, settings, spark, args) -> None:
    started = time.time()
    log.info("===== stage: %s =====", stage)
    if stage == "download":
        from dbpipe.ingest.download import download_months

        download_months(settings, force=args.force)
    elif stage == "sample":
        from dbpipe.sample_data import write_sample

        write_sample(settings, days=args.sample_days, rides_per_day=args.sample_rides)
    elif stage == "bronze":
        from dbpipe.ingest.bronze import run_bronze

        run_bronze(spark, settings, force=args.force)
    elif stage == "silver":
        from dbpipe.transform.silver import run_silver

        run_silver(spark, settings)
    elif stage == "gold":
        from dbpipe.transform.gold import run_gold

        run_gold(spark, settings)
    elif stage == "quality":
        from dbpipe.quality import run_quality

        run_quality(spark, settings)
    elif stage == "features":
        from dbpipe.ml.features import run_features

        run_features(spark, settings)
    elif stage == "train":
        from dbpipe.ml.train import run_train

        run_train(settings)
    elif stage == "query":
        from dbpipe.query import run_query

        run_query(spark, settings, args.sql, limit=args.limit)
    elif stage == "report":
        from dbpipe.report import run_report

        run_report(settings)
    log.info("===== %s finished in %.0fs =====", stage, time.time() - started)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dbpipe", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("stage", choices=("download", "sample", "run", "query", *RUN_ORDER))
    parser.add_argument("sql", nargs="?", help="for `query`: a .sql file or a SQL string")
    parser.add_argument("--limit", type=int, default=30, help="for `query`: rows to show")
    parser.add_argument("--config", help="path to a pipeline.yaml (default: config/pipeline.yaml)")
    parser.add_argument("--force", action="store_true", help="re-download / re-load even if unchanged")
    parser.add_argument("--sample-days", type=int, default=30, help="days per month for `sample`")
    parser.add_argument("--sample-rides", type=int, default=250, help="train rides per day for `sample`")
    parser.add_argument("-v", "--verbose", action="store_true", help="show full stack traces on errors")
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    stages = RUN_ORDER if args.stage == "run" else (args.stage,)
    log.info("Months: %s | storage: %s", ", ".join(settings.months), settings.storage_format)

    spark = None
    if any(s in SPARK_STAGES for s in stages):
        from dbpipe.spark import build_spark

        spark = build_spark(settings)
    try:
        for stage in stages:
            _run_stage(stage, settings, spark, args)
    except Exception as exc:  # show a short, readable error instead of a Java stack trace
        log.error("Stage failed: %s: %s", type(exc).__name__, str(exc).splitlines()[0][:500])
        if args.verbose:
            raise
        return 1
    finally:
        if spark is not None:
            spark.stop()
    return 0
