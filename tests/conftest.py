"""Shared pytest fixtures: one local SparkSession for the whole test run (starting Spark is slow)."""

from __future__ import annotations

import copy
import os
import time

import pytest
from pyspark.sql import SparkSession

from dbpipe.config import Settings, load_settings


def pytest_configure(config):
    """Test rows are built from naive Python datetimes, which PySpark interprets in the
    process's local timezone. Pin it to the pipeline's timezone so the tests give the same
    result on every machine (a laptop in Berlin, a Docker container or CI runner in UTC)."""
    os.environ["TZ"] = "Europe/Berlin"
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("dbpipe-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Europe/Berlin")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Project settings, but writing into a temporary folder and using plain Parquet."""
    base = load_settings()
    raw = copy.deepcopy(base.raw)
    raw["storage"]["format"] = "parquet"
    raw["paths"]["landing"] = str(tmp_path / "landing")
    raw["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    raw["paths"]["artifacts"] = str(tmp_path / "artifacts")
    raw["paths"]["docs"] = str(tmp_path / "docs")
    return Settings(raw=raw)
