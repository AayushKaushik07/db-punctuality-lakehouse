from datetime import datetime

from dbpipe.transform.silver import build_silver
from tests.helpers import bronze_df, stop


def _dt(h, m=0, day=10, month=9):
    return datetime(2025, month, day, h, m)


def _ride(ride="R1"):
    """Origin A -> B -> terminus C, getting later along the way."""
    return [
        stop(f"{ride}-1", ride, 1, eva="A", dep=_dt(8), dep_delay=2),
        stop(f"{ride}-2", ride, 2, eva="B", arr=_dt(9), dep=_dt(9, 2), arr_delay=7, dep_delay=7),
        stop(f"{ride}-3", ride, 3, eva="C", arr=_dt(10), arr_delay=12),
    ]


def _run(spark, rows):
    clean, quarantine, _ = build_silver(bronze_df(spark, rows))
    return {r["id"]: r for r in clean.collect()}, quarantine.collect()


def test_delays_are_recomputed_from_timestamps(spark):
    clean, _ = _run(spark, _ride())
    assert clean["R1-2"]["arrival_delay_min"] == 7
    assert clean["R1-1"]["departure_delay_min"] == 2
    assert clean["R1-1"]["arrival_delay_min"] is None  # origin has no arrival
    assert clean["R1-1"]["stop_role"] == "origin"
    assert clean["R1-3"]["stop_role"] == "terminus"
    assert all(r["delay_matches_source"] for r in clean.values())


def test_no_change_message_means_on_time(spark):
    rows = [stop("X-1", "X", 1, arr=_dt(9), dep=_dt(9, 1), changes=False)]
    clean, _ = _run(spark, rows)
    assert clean["X-1"]["arrival_delay_min"] == 0


def test_delay_gain_per_segment(spark):
    clean, _ = _run(spark, _ride())
    assert clean["R1-2"]["prev_eva"] == "A"
    assert clean["R1-2"]["delay_gain_min"] == 5  # left A 2 min late, reached B 7 min late
    assert clean["R1-3"]["delay_gain_min"] == 5  # left B 7 min late, reached C 12 min late
    assert clean["R1-1"]["delay_gain_min"] is None  # no previous stop


def test_no_delay_gain_for_cancelled_stops(spark):
    rows = _ride()
    rows[2] = stop("R1-3", "R1", 3, eva="C", arr=_dt(10), arr_delay=12, canceled=True)
    clean, _ = _run(spark, rows)
    assert clean["R1-3"]["delay_gain_min"] is None


def test_duplicates_keep_the_most_complete_record(spark):
    rows = _ride() + [stop("R1-2", "R1", 2, eva="B", arr=_dt(9), dep=_dt(9, 2), changes=False)]
    clean, _ = _run(spark, rows)
    assert len(clean) == 3
    assert clean["R1-2"]["arrival_delay_min"] == 7  # the record with the change message wins


def test_invalid_rows_go_to_quarantine_with_reasons(spark):
    rows = _ride() + [
        stop("bad-eva", "R2", 1, eva=None, dep=_dt(8)),
        stop("bad-delay", "R3", 2, arr=_dt(9), delay_in_min=99999),
        stop("bad-order", "R4", 2, arr=_dt(9), dep=_dt(8, 30)),
        stop("bad-date", "R5", 2, arr=_dt(9, day=5, month=10)),
    ]
    clean, quarantine = _run(spark, rows)
    reasons = {r["id"]: r["dq_reasons"] for r in quarantine}
    assert reasons == {
        "bad-eva": "missing_eva",
        "bad-delay": "delay_out_of_range",
        "bad-order": "departure_before_arrival",
        "bad-date": "date_outside_source_month",
    }
    assert set(clean) == {"R1-1", "R1-2", "R1-3"}


def test_stops_from_the_neighbouring_month_are_dropped_not_quarantined(spark):
    # the September file contains a train from the evening of 31 August and one after midnight on 1 October
    rows = [
        stop("P-2508312350-2", "P", 2, arr=datetime(2025, 8, 31, 23, 58), month="2025-09"),
        stop("Q-2509302350-2", "Q", 2, arr=datetime(2025, 10, 1, 0, 30), month="2025-09"),
        stop("R-2509101000-2", "R", 2, arr=datetime(2025, 9, 10, 10, 0), month="2025-09"),
    ]
    clean, quarantine, outside = build_silver(bronze_df(spark, rows))
    assert [r["id"] for r in clean.collect()] == ["R-2509101000-2"]
    assert sorted(r["id"] for r in outside.collect()) == ["P-2508312350-2", "Q-2509302350-2"]
    assert quarantine.count() == 0


def test_journeys_are_built_from_the_stop_id_not_the_timetable_pattern(spark):
    """Real data: `train_line_ride_id` is the same pattern id on every day. Two days of the
    same pattern are two separate journeys and must never be chained together."""
    rows = []
    for day, delays in ((10, (2, 9)), (11, (0, 1))):
        start = f"2509{day}0800"
        rows += [
            stop(f"PAT-{start}-1", "PAT", 1, eva="A", dep=_dt(8, day=day), dep_delay=delays[0]),
            stop(f"PAT-{start}-2", "PAT", 2, eva="B", arr=_dt(9, day=day), arr_delay=delays[1]),
        ]
    clean, _ = _run(spark, rows)
    day10, day11 = clean["PAT-2509100800-2"], clean["PAT-2509110800-2"]
    assert day10["ride_id"] == "PAT-2509100800" and day10["line_ride_id"] == "PAT"
    assert (day10["prev_eva"], day10["delay_gain_min"]) == ("A", 7)
    assert (day11["prev_eva"], day11["delay_gain_min"]) == ("A", 1)
    assert clean["PAT-2509110800-1"]["prev_eva"] is None  # first stop of day 11 has no predecessor


def test_weekday_is_monday_based(spark):
    rows = [stop("mon", "M", 1, arr=datetime(2025, 9, 8, 10, 0))]  # 2025-09-08 was a Monday
    clean, _ = _run(spark, rows)
    assert clean["mon"]["weekday"] == 0
    assert clean["mon"]["event_hour"] == 10
