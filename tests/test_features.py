"""The most important ML test: features for day D must never use information from day D or later."""

from datetime import date

import pytest
from pyspark.sql import types as T

from dbpipe.ml.features import LABEL, build_features

FCT_SCHEMA = T.StructType(
    [
        T.StructField("stop_event_id", T.StringType()),
        T.StructField("source_month", T.StringType()),
        T.StructField("service_date", T.DateType()),
        T.StructField("event_hour", T.IntegerType()),
        T.StructField("weekday", T.IntegerType()),
        T.StructField("eva", T.StringType()),
        T.StructField("train_type", T.StringType()),
        T.StructField("train_category", T.StringType()),
        T.StructField("train_label", T.StringType()),
        T.StructField("line_number", T.StringType()),
        T.StructField("stop_seq", T.IntegerType()),
        T.StructField("stop_role", T.StringType()),
        T.StructField("arrival_delay_min", T.IntegerType()),
        T.StructField("is_arrival_measured", T.BooleanType()),
    ]
)
DATE_SCHEMA = "date date, is_weekend boolean, is_public_holiday boolean, states_with_holiday int"

# station A: day 1 -> 1 of 2 late, day 2 -> 2 of 2 late, day 3 -> 0 of 2 late
DELAYS = {date(2025, 9, 1): [0, 10], date(2025, 9, 2): [8, 20], date(2025, 9, 3): [0, 1]}


@pytest.fixture
def features(spark):
    rows = []
    for d, delays in DELAYS.items():
        for i, delay in enumerate(delays):
            rows.append(
                (
                    f"{d}-{i}",
                    "2025-09",
                    d,
                    8,
                    d.weekday(),
                    "A",
                    "RE",
                    "regional",
                    "RE 1",
                    "RE1",
                    2,
                    "intermediate",
                    delay,
                    True,
                )
            )
    fct = spark.createDataFrame(rows, FCT_SCHEMA)
    dim_date = spark.createDataFrame([(d, False, False, 0) for d in DELAYS], DATE_SCHEMA)
    out = build_features(fct, dim_date, threshold_min=6).collect()
    return {r["service_date"]: r for r in out}


def test_label_uses_threshold(features):
    assert features[date(2025, 9, 2)][LABEL] == 1
    assert features[date(2025, 9, 3)][LABEL] == 0


def test_first_day_has_no_history(features):
    assert features[date(2025, 9, 1)]["station_late_rate_7d"] is None
    assert features[date(2025, 9, 1)]["network_late_rate_prev_day"] is None


def test_rolling_rate_only_uses_previous_days(features):
    assert features[date(2025, 9, 2)]["station_late_rate_7d"] == pytest.approx(0.5)  # day 1 only
    # day 3 itself had 0% late; if it leaked in, this would be 0.5 instead of 0.75
    assert features[date(2025, 9, 3)]["station_late_rate_7d"] == pytest.approx(0.75)
    assert features[date(2025, 9, 3)]["station_hour_late_rate_hist"] == pytest.approx(0.75)


def test_network_rate_is_previous_day_only(features):
    assert features[date(2025, 9, 3)]["network_late_rate_prev_day"] == pytest.approx(1.0)  # day 2
