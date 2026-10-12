from datetime import date

import pytest

from dbpipe.config import load_settings
from dbpipe.transform.gold import german_holiday_calendar, render_sql, sql_files, table_name


def test_env_overrides_config(monkeypatch):
    monkeypatch.setenv("DBPIPE_MONTHS", "2025-07, 2025-08")
    monkeypatch.setenv("DBPIPE_STORAGE_FORMAT", "parquet")
    s = load_settings()
    assert s.months == ["2025-07", "2025-08"]
    assert s.storage_format == "parquet"


def test_invalid_month_is_rejected(monkeypatch):
    monkeypatch.setenv("DBPIPE_MONTHS", "2025-13")
    with pytest.raises(ValueError, match="Invalid month"):
        load_settings()


def test_sql_parameters_are_rendered():
    assert render_sql("x < ${limit}", {"limit": 6}) == "x < 6"
    with pytest.raises(KeyError, match="missing"):
        render_sql("${missing}", {})


def test_every_sql_model_has_a_unique_table_name():
    names = [table_name(f) for f in sql_files()]
    assert "fct_stop_events" in names and "dim_station" in names
    assert len(names) == len(set(names))


def test_holiday_calendar():
    cal = german_holiday_calendar(date(2025, 10, 1), date(2025, 11, 2)).set_index("date")
    assert cal.loc[date(2025, 10, 3), "is_public_holiday"]  # Tag der Deutschen Einheit (nationwide)
    assert not cal.loc[date(2025, 11, 1), "is_public_holiday"]  # All Saints' Day is regional only…
    assert cal.loc[date(2025, 11, 1), "states_with_holiday"] >= 5  # …e.g. in Baden-Württemberg and Bavaria
    assert cal.loc[date(2025, 11, 1), "is_weekend"]  # it was a Saturday
