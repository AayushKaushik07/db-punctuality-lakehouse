"""End-to-end test: synthetic sample -> bronze -> silver -> gold -> quality -> features -> model.

Also checks that the pipeline is idempotent: re-running a stage must not duplicate data.
"""

import json

import pytest

from dbpipe.config import PROJECT_ROOT
from dbpipe.ingest.bronze import run_bronze
from dbpipe.lakehouse import read_table, table_exists
from dbpipe.ml.features import run_features
from dbpipe.ml.train import run_train
from dbpipe.quality import run_quality
from dbpipe.query import register_all_views
from dbpipe.sample_data import write_sample
from dbpipe.transform.gold import render_sql, run_gold
from dbpipe.transform.silver import run_silver


@pytest.fixture
def pipeline_settings(settings):
    settings.raw["dataset"]["months"] = ["2025-09", "2025-10"]
    settings.raw["analytics"]["min_station_arrivals"] = 100
    settings.raw["analytics"]["min_segment_observations"] = 10
    settings.raw["ml"]["test_days"] = 7
    return settings


def test_full_pipeline_on_sample(spark, pipeline_settings):
    s = pipeline_settings
    write_sample(s, days=30, rides_per_day=60)

    assert run_bronze(spark, s) == ["2025-09", "2025-10"]
    assert run_bronze(spark, s) == []  # unchanged files are skipped (incremental load)

    run_silver(spark, s)
    silver_rows = read_table(spark, s, "silver", "stop_events").count()
    run_silver(spark, s)  # idempotent: replacing the same months must not duplicate rows
    assert read_table(spark, s, "silver", "stop_events").count() == silver_rows

    run_gold(spark, s)
    for table in (
        "dim_station",
        "dim_date",
        "dim_train_type",
        "fct_stop_events",
        "agg_punctuality_daily",
        "agg_station_monthly",
        "agg_segment_hotspots",
        "agg_official_comparison",
    ):
        assert table_exists(s, "gold", table), table

    results = run_quality(spark, s)  # raises DataQualityError on any ERROR-level failure
    assert all(r.passed for r in results if r.severity == "ERROR")

    # every ad-hoc analysis query in sql/analysis/ must run against the gold layer
    register_all_views(spark, s)
    for sql_file in sorted((PROJECT_ROOT / "sql" / "analysis").glob("*.sql")):
        spark.sql(render_sql(sql_file.read_text(), s.section("analytics"))).collect()

    run_features(spark, s)
    summary = run_train(s)
    assert set(summary["metrics"]) == {"baseline", "logistic", "gbm"}
    assert (s.path("artifacts") / "model" / "delay_model_gbm.joblib").exists()
    assert json.loads((s.path("artifacts") / "metrics.json").read_text())["split"]["test_rows"] > 0

    # the dashboard must render every tab on the freshly built gold layer
    import os

    from streamlit.testing.v1 import AppTest

    os.environ.update(
        DBPIPE_STORAGE_FORMAT="parquet",
        DBPIPE_LAKEHOUSE_PATH=str(s.path("lakehouse")),
        DBPIPE_ARTIFACTS_PATH=str(s.path("artifacts")),
    )
    try:
        app = AppTest.from_file(str(PROJECT_ROOT / "dashboard" / "app.py"), default_timeout=120).run()
        assert not app.exception, [e.value for e in app.exception]
        assert len(app.tabs) == 7
    finally:
        for key in ("DBPIPE_STORAGE_FORMAT", "DBPIPE_LAKEHOUSE_PATH", "DBPIPE_ARTIFACTS_PATH"):
            os.environ.pop(key, None)
