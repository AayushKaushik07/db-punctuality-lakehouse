"""Generate a SMALL SYNTHETIC sample with exactly the same schema as the real monthly files.

Purpose: unit tests, CI and a quick smoke run without downloading ~100 MB per month.
It is NOT used for any analysis or result in this project – the report marks runs on
sample data with a clear warning.

It deliberately plants the data-quality problems the real pipeline must handle:
exact duplicate rows, missing station ids, impossible delays and inverted timestamps.
"""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dbpipe.config import Settings
from dbpipe.ingest.download import landing_file, read_manifest, update_manifest
from dbpipe.logging_utils import get_logger

log = get_logger("sample")

# (eva, name, extra delay a train picks up when arriving here – "hotspot" factor)
STATIONS = [
    ("8000096", "Stuttgart Hbf", 1.2),
    ("8000105", "Frankfurt(Main)Hbf", 1.5),
    ("8000244", "Mannheim Hbf", 0.8),
    ("8000207", "Köln Hbf", 1.8),
    ("8002549", "Hamburg Hbf", 1.4),
    ("8000261", "München Hbf", 0.9),
    ("8011160", "Berlin Hbf", 0.7),
    ("8000191", "Karlsruhe Hbf", 0.5),
    ("8000157", "Heilbronn Hbf", 0.3),
    ("8000284", "Nürnberg Hbf", 0.6),
    ("8000152", "Hannover Hbf", 1.0),
    ("8000085", "Düsseldorf Hbf", 1.3),
]

TRAIN_TYPES = {  # type: (probability, stops range, minutes between stops, base delay mean)
    "ICE": (0.25, (4, 7), (35, 80), 3.5),
    "IC": (0.10, (4, 6), (35, 70), 3.0),
    "RE": (0.25, (3, 5), (20, 45), 1.5),
    "RB": (0.15, (3, 5), (15, 35), 1.2),
    "S": (0.25, (3, 4), (8, 18), 0.8),
}

HOUR_WEIGHTS = np.array([0, 0, 0, 0, 1, 3, 6, 9, 8, 6, 5, 5, 5, 5, 6, 7, 9, 9, 8, 6, 5, 4, 3, 1], dtype=float)


def _month_days(month: str, days: int) -> list[datetime]:
    year, mon = map(int, month.split("-"))
    n = min(days, calendar.monthrange(year, mon)[1])
    return [datetime(year, mon, d) for d in range(1, n + 1)]


def _timetable(rng: np.random.Generator, size: int) -> list[dict]:
    """Recurring timetable patterns, like the real data: one pattern (= `train_line_ride_id`)
    runs on many days with the same route, train number and departure time."""
    types = list(TRAIN_TYPES)
    type_p = np.array([TRAIN_TYPES[t][0] for t in types])
    patterns = []
    for _ in range(size):
        ttype = str(rng.choice(types, p=type_p))
        _, stop_range, gap_range, base = TRAIN_TYPES[ttype]
        n_stops = int(rng.integers(stop_range[0], stop_range[1] + 1))
        patterns.append(
            {
                # real pattern ids are large signed integers, e.g. "-2846549535632273880"
                "pattern_id": str(int(rng.integers(-(2**62), 2**62))),
                "ttype": ttype,
                "route": rng.choice(len(STATIONS), size=n_stops, replace=False),
                "gaps": [int(rng.integers(*gap_range)) for _ in range(n_stops)],
                "hour": int(rng.choice(24, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())),
                "minute": int(rng.integers(0, 60)),
                "train_number": str(int(rng.integers(100, 99999))),
                "line": None if ttype in ("ICE", "IC") else f"{ttype}{int(rng.integers(1, 12))}",
                "base": base,
            }
        )
    return patterns


def generate_month(month: str, days: int = 28, rides_per_day: int = 250, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed + int(month.replace("-", "")))
    timetable = _timetable(rng, size=int(rides_per_day * 1.25))
    rows: list[dict] = []

    for day in _month_days(month, days):
        weekend = day.weekday() >= 5
        disruption = rng.lognormal(mean=0.0, sigma=0.45)  # some days are simply bad days
        n_today = rides_per_day if not weekend else int(rides_per_day * 0.7)
        for k in rng.choice(len(timetable), size=min(n_today, len(timetable)), replace=False):
            p = timetable[k]
            ttype, route, base, hour = p["ttype"], p["route"], p["base"], p["hour"]
            n_stops = len(route)
            start = day + timedelta(hours=hour, minutes=p["minute"])
            planned = start
            # real stop ids: "<pattern>-<trip start yymmddHHMM>-<stop number>"
            journey = f"{p['pattern_id']}-{start:%y%m%d%H%M}"
            peak = 1.6 if hour in (7, 8, 16, 17, 18) else 1.0
            delay = max(0.0, rng.exponential(base * 0.5 * peak * disruption))
            cancel_from = n_stops + 1 if rng.random() > 0.03 else int(rng.integers(1, n_stops))
            dest_name = STATIONS[route[-1]][1]

            for seq, st_idx in enumerate(route, start=1):
                eva, name, hotspot = STATIONS[st_idx]
                is_origin, is_terminus = seq == 1, seq == n_stops
                if not is_origin:
                    gain = rng.normal(0.35 * base * peak * disruption + 0.4 * hotspot, 1.2)
                    delay = max(0.0, delay + gain)
                arr_delay = int(round(delay))
                dep_delay = max(0, arr_delay - int(rng.integers(0, 2)))  # a little dwell recovery
                arr_planned = None if is_origin else planned
                dep_planned = None if is_terminus else planned + timedelta(minutes=2)
                canceled = seq >= cancel_from
                arr_change = arr_planned + timedelta(minutes=arr_delay) if arr_planned else None
                dep_change = dep_planned + timedelta(minutes=dep_delay) if dep_planned else None
                rows.append(
                    {
                        "station_name": name,
                        "xml_station_name": name,
                        "eva": eva,
                        "train_number": p["train_number"],
                        "line_number": p["line"],
                        "final_destination_station": dest_name,
                        "delay_in_min": arr_delay if arr_planned else dep_delay,
                        "time": arr_change or dep_change,
                        "arrival_is_canceled": bool(canceled and arr_planned is not None),
                        "departure_is_canceled": bool(canceled and dep_planned is not None),
                        "train_type": ttype,
                        "is_additional_stop": False,
                        "is_replacement_train": False,
                        "replaced_train_number": None,
                        "train_line_ride_id": p["pattern_id"],
                        "train_line_station_num": seq,
                        "arrival_planned_time": arr_planned,
                        "arrival_change_time": arr_change,
                        "departure_planned_time": dep_planned,
                        "departure_change_time": dep_change,
                        "id": f"{journey}-{seq}",
                    }
                )
                planned = planned + timedelta(minutes=p["gaps"][seq - 1])

    df = pd.DataFrame(rows)
    return _plant_quality_issues(df, rng)


def _plant_quality_issues(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = len(df)
    dupes = df.sample(n=max(1, int(n * 0.003)), random_state=int(rng.integers(1e6)))
    df = pd.concat([df, dupes], ignore_index=True)  # exact duplicate fetches
    idx = rng.choice(len(df), size=max(1, int(n * 0.001)), replace=False)
    df.loc[idx, "eva"] = None  # missing station id
    idx = rng.choice(len(df), size=max(1, int(n * 0.0005)), replace=False)
    df.loc[idx, "delay_in_min"] = 99999  # impossible delay
    through = df.index[df["arrival_planned_time"].notna() & df["departure_planned_time"].notna()]
    idx = rng.choice(through, size=max(1, int(n * 0.0005)), replace=False)
    df.loc[idx, "departure_planned_time"] = df.loc[idx, "arrival_planned_time"] - pd.Timedelta(minutes=30)
    return df.sample(frac=1.0, random_state=1).reset_index(drop=True)


ARROW_SCHEMA = pa.schema(
    [
        ("station_name", pa.string()),
        ("xml_station_name", pa.string()),
        ("eva", pa.string()),
        ("train_number", pa.string()),
        ("line_number", pa.string()),
        ("final_destination_station", pa.string()),
        ("delay_in_min", pa.int64()),
        ("time", pa.timestamp("ns")),
        ("arrival_is_canceled", pa.bool_()),
        ("departure_is_canceled", pa.bool_()),
        ("train_type", pa.string()),
        ("is_additional_stop", pa.bool_()),
        ("is_replacement_train", pa.bool_()),
        ("replaced_train_number", pa.string()),
        ("train_line_ride_id", pa.string()),
        ("train_line_station_num", pa.int64()),
        ("arrival_planned_time", pa.timestamp("ns")),
        ("arrival_change_time", pa.timestamp("ns")),
        ("departure_planned_time", pa.timestamp("ns")),
        ("departure_change_time", pa.timestamp("ns")),
        ("id", pa.string()),
    ]
)


def write_sample(settings: Settings, days: int = 28, rides_per_day: int = 250) -> None:
    manifest = read_manifest(settings)
    real = [m for m in settings.months if manifest.get(m, {}).get("source", "synthetic-sample") != "synthetic-sample"]
    if real:
        raise RuntimeError(
            f"Refusing to overwrite REAL downloaded data for {real} in {settings.path('landing')}. "
            "Use `make sample-run`, which writes the sample to a separate folder."
        )
    for month in settings.months:
        df = generate_month(month, days=days, rides_per_day=rides_per_day)
        target = landing_file(settings, month)
        target.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df[ARROW_SCHEMA.names], schema=ARROW_SCHEMA, preserve_index=False)
        pq.write_table(table, target)
        update_manifest(settings, month, source="synthetic-sample")
        log.info("Wrote SYNTHETIC sample for %s: %s rows -> %s", month, f"{len(df):,}", target)
