from datetime import datetime

import pytest
from pyspark.sql import types as T

from dbpipe.ingest.contract import REQUIRED, SchemaContractError, enforce_contract


def _source_df(spark, drop=(), extra=None, overrides=None):
    fields = {**REQUIRED, **(overrides or {})}
    for name in drop:
        fields.pop(name)
    if extra:
        fields.update(extra)
    schema = T.StructType([T.StructField(n, t) for n, t in fields.items()])
    return spark.createDataFrame([], schema)


def test_valid_file_passes_and_optional_columns_are_added(spark):
    df, report = enforce_contract(_source_df(spark))
    assert set(report.missing_optional) == {
        "xml_station_name",
        "is_additional_stop",
        "is_replacement_train",
        "replaced_train_number",
    }
    assert "is_replacement_train" in df.columns


def test_missing_required_column_fails_loudly(spark):
    with pytest.raises(SchemaContractError, match="delay_in_min"):
        enforce_contract(_source_df(spark, drop=["delay_in_min"]))


def test_extra_columns_are_dropped(spark):
    df, report = enforce_contract(_source_df(spark, extra={"is_canceled": T.BooleanType()}))
    assert report.dropped_extra == ["is_canceled"]
    assert "is_canceled" not in df.columns


def test_compatible_types_are_cast(spark):
    overrides = {"delay_in_min": T.LongType(), "time": T.TimestampNTZType()}
    df, report = enforce_contract(_source_df(spark, overrides=overrides))
    assert dict(df.dtypes)["delay_in_min"] == "int"
    assert dict(df.dtypes)["time"] == "timestamp"
    assert len(report.casted) == 2


def test_incompatible_type_is_rejected(spark):
    with pytest.raises(SchemaContractError, match="arrival_is_canceled"):
        enforce_contract(_source_df(spark, overrides={"arrival_is_canceled": T.TimestampType()}))


def test_values_survive_the_contract(spark):
    schema = T.StructType([T.StructField(n, t) for n, t in REQUIRED.items()])
    row = {n: None for n in REQUIRED}
    row.update(id="a", delay_in_min=7, time=datetime(2025, 9, 1, 8, 0))
    df, _ = enforce_contract(spark.createDataFrame([tuple(row.values())], schema))
    out = df.first()
    assert (out["id"], out["delay_in_min"]) == ("a", 7)


def test_nanosecond_wall_clock_timestamps_keep_their_clock_time(spark, tmp_path):
    """The real files store German clock time as INT64 TIMESTAMP(NANOS, isAdjustedToUTC=false).
    After the contract, 23:58 must still be 23:58, also in the night the clocks go back."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    walls = [
        datetime(2025, 8, 31, 23, 58),
        datetime(2025, 10, 26, 2, 30),  # exists twice that night (CEST -> CET)
        datetime(2025, 10, 26, 3, 30),
        None,
    ]
    arrays = {name: pa.nulls(len(walls), pa.string()) for name in REQUIRED}
    for name, dtype in REQUIRED.items():
        if isinstance(dtype, T.TimestampType):
            arrays[name] = pa.array(walls, pa.timestamp("ns"))
        elif isinstance(dtype, T.IntegerType):
            arrays[name] = pa.nulls(len(walls), pa.int32())
        elif isinstance(dtype, T.BooleanType):
            arrays[name] = pa.nulls(len(walls), pa.bool_())
    path = tmp_path / "nanos.parquet"
    pq.write_table(pa.table(arrays), path)

    df, report = enforce_contract(spark.read.parquet(str(path)))
    assert dict(df.dtypes)["arrival_planned_time"] == "timestamp"
    assert any(c.startswith("arrival_planned_time:bigint") for c in report.casted)
    got = [r["t"] for r in df.selectExpr("date_format(arrival_planned_time, 'yyyy-MM-dd HH:mm') AS t").collect()]
    assert got == ["2025-08-31 23:58", "2025-10-26 02:30", "2025-10-26 03:30", None]
