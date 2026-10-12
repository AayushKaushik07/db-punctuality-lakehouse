"""Streamlit dashboard on top of the gold layer.

Run:  streamlit run dashboard/app.py      (after the pipeline has run)
It reads the gold tables directly (Delta via `deltalake` or Parquet via pandas) –
no Spark needed, so it starts in seconds.
"""

from __future__ import annotations

import json

import pandas as pd
import plotly.express as px
import streamlit as st

from dbpipe.config import load_settings
from dbpipe.lakehouse import read_table_pandas
from dbpipe.plotting import CATEGORY_COLORS, CATEGORY_LABELS, NEUTRAL

st.set_page_config(page_title="DB Punctuality Lakehouse", page_icon="🚆", layout="wide")
settings = load_settings()

COLOR_MAP = {CATEGORY_LABELS[k]: v for k, v in CATEGORY_COLORS.items()}
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@st.cache_data(ttl=600)
def load(name: str) -> pd.DataFrame:
    return read_table_pandas(settings, "gold", name)


def with_labels(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(category=df["train_category"].map(CATEGORY_LABELS).fillna(df["train_category"]))


try:
    daily = with_labels(load("agg_punctuality_daily"))
except FileNotFoundError:
    st.error("No gold tables found. Run the pipeline first: `make run` (see docs/SETUP.md).")
    st.stop()

daily["service_date"] = pd.to_datetime(daily["service_date"])

# ----------------------------------------------------------------------- sidebar
st.sidebar.title("🚆 DB Punctuality")
st.sidebar.caption("Data: Deutsche Bahn via piebro/deutsche-bahn-data (CC BY 4.0)")
min_d, max_d = daily.service_date.min().date(), daily.service_date.max().date()
date_range = st.sidebar.date_input("Date range", (min_d, max_d), min_value=min_d, max_value=max_d)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    start, end = pd.Timestamp(min_d), pd.Timestamp(max_d)
categories = st.sidebar.multiselect(
    "Train categories", ["Long-distance", "Regional", "S-Bahn"], default=["Long-distance", "Regional", "S-Bahn"]
)
st.sidebar.markdown("**Punctual** = arrival < 6 min late (DB definition). Cancelled stops excluded.")

view = daily[(daily.service_date >= start) & (daily.service_date <= end)]

tab_overview, tab_time, tab_stations, tab_segments, tab_validation, tab_model, tab_dq = st.tabs(
    ["Overview", "Time patterns", "Stations", "Segments & lines", "Validation vs. DB", "Delay model", "Data quality"]
)

# ----------------------------------------------------------------------- overview
with tab_overview:
    total = view[view.train_category == "all_rail"]
    measured = total.measured_arrivals.sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Punctuality (all rail)", f"{100 * total.punctual_arrivals.sum() / max(measured, 1):.1f}%")
    c2.metric("Measured arrivals", f"{measured:,}")
    c3.metric("Cancelled stops", f"{100 * total.canceled_stops.sum() / max(total.scheduled_stops.sum(), 1):.1f}%")
    c4.metric(
        "Avg. arrival delay",
        f"{(total.avg_arrival_delay_min * total.measured_arrivals).sum() / max(measured, 1):.1f} min",
    )

    per_cat = view[view.category.isin(categories)]
    fig = px.line(
        per_cat.sort_values("service_date"),
        x="service_date",
        y="punctuality_pct_7d_avg",
        color="category",
        color_discrete_map=COLOR_MAP,
        labels={"punctuality_pct_7d_avg": "punctual arrivals, % (7-day avg)", "service_date": "", "category": ""},
        title="Punctuality by train category",
    )
    fig.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.15))
    st.plotly_chart(fig, width="stretch")

    dist = with_labels(load("agg_delay_distribution"))
    dist = dist[dist.category.isin(categories)].sort_values("delay_bucket")
    dist["bucket"] = dist.delay_bucket.str.split(": ").str[1]
    fig = px.bar(
        dist,
        x="bucket",
        y="pct_of_category",
        color="category",
        barmode="group",
        color_discrete_map=COLOR_MAP,
        labels={"pct_of_category": "% of arrivals", "bucket": "arrival delay", "category": ""},
        title="How late are trains?",
    )
    st.plotly_chart(fig, width="stretch")

# ----------------------------------------------------------------------- time
with tab_time:
    hourly = with_labels(load("agg_hourly_profile"))
    choice = st.selectbox("Category", ["All rail", *categories])
    h = hourly[(hourly.category == choice) & (hourly.measured_arrivals >= 30)].copy()
    h["late_pct"] = 100 - h.punctuality_pct
    grid = h.pivot_table(index="weekday", columns="event_hour", values="late_pct").reindex(range(7))
    fig = px.imshow(
        grid,
        y=WEEKDAYS,
        color_continuous_scale="Blues",
        aspect="auto",
        labels={"x": "hour", "y": "", "color": "late %"},
        title=f"Share of late arrivals by weekday and hour — {choice}",
    )
    st.plotly_chart(fig, width="stretch")
    st.caption("Darker = more late arrivals. Cells with fewer than 30 arrivals are hidden.")

# ----------------------------------------------------------------------- stations
with tab_stations:
    stations = load("agg_station_monthly")
    month = st.selectbox("Month", sorted(stations.source_month.unique(), reverse=True))
    s = stations[(stations.source_month == month) & stations.is_rankable]
    n = st.slider("Show worst N stations", 5, 40, 15)
    worst = s.nsmallest(n, "punctuality_pct").sort_values("punctuality_pct", ascending=False)
    fig = px.bar(
        worst,
        x="punctuality_pct",
        y="station_name",
        orientation="h",
        color_discrete_sequence=[CATEGORY_COLORS["long_distance"]],
        hover_data=["measured_arrivals", "cancellation_pct", "avg_arrival_delay_min", "punctuality_change_pp"],
        labels={"punctuality_pct": "punctual arrivals, %", "station_name": ""},
        title=f"Least punctual stations, {month}",
    )
    fig.update_layout(height=max(350, 28 * n))
    st.plotly_chart(fig, width="stretch")
    st.dataframe(s.sort_values("worst_punctuality_rank"), width="stretch", hide_index=True)

# ----------------------------------------------------------------------- segments
with tab_segments:
    hot = with_labels(load("agg_segment_hotspots").rename(columns={"main_train_category": "train_category"}))
    top = hot.nsmallest(20, "hotspot_rank").assign(segment=lambda d: d.from_station + " → " + d.to_station)
    fig = px.bar(
        top.sort_values("median_gain_per_hour"),
        x="median_gain_per_hour",
        y="segment",
        color="category",
        orientation="h",
        color_discrete_map=COLOR_MAP,
        hover_data=["observations", "median_planned_min", "median_delay_gain_min", "pct_trains_gaining_3min"],
        labels={"median_gain_per_hour": "median min lost per scheduled hour", "segment": "", "category": ""},
        title="Where trains lose the most time (per hour of scheduled running time)",
    )
    fig.update_layout(height=650)
    st.plotly_chart(fig, width="stretch")

    lines = with_labels(load("agg_line_reliability"))
    st.subheader("Least reliable lines / trains")
    st.dataframe(
        lines[lines.category.isin(categories)].sort_values("punctuality_pct").head(50),
        width="stretch",
        hide_index=True,
    )

# ----------------------------------------------------------------------- validation
with tab_validation:
    comp = load("agg_official_comparison").dropna(subset=["official_punctuality_pct"])
    if comp.empty:
        st.info("No overlapping periods with reference/db_official_punctuality.csv.")
    else:
        comp = comp.sort_values("first_month")
        long = comp.melt(
            id_vars=["period", "segment"],
            value_vars=["our_punctuality_pct", "official_punctuality_pct"],
            var_name="series",
            value_name="punctuality_pct",
        )
        long["series"] = long.series.map({"our_punctuality_pct": "This pipeline", "official_punctuality_pct": "DB"})
        fig = px.bar(
            long,
            x="period",
            y="punctuality_pct",
            color="series",
            barmode="group",
            facet_col="segment",
            color_discrete_map={"This pipeline": CATEGORY_COLORS["long_distance"], "DB": NEUTRAL},
            labels={"punctuality_pct": "punctual, %", "series": "", "period": ""},
            title="Our numbers vs. Deutsche Bahn's published punctuality",
        )
        st.plotly_chart(fig, width="stretch")
        st.dataframe(comp, width="stretch", hide_index=True)
        st.caption(
            "Differences are expected: until 2025-10 the dataset only covers ~100 big stations, it includes "
            "non-DB operators, and DB uses internal data for every stop."
        )

# ----------------------------------------------------------------------- model
with tab_model:
    metrics_file = settings.path("artifacts") / "metrics.json"
    if not metrics_file.exists():
        st.info("Model not trained yet: `python -m dbpipe train`.")
    else:
        m = json.loads(metrics_file.read_text())
        st.markdown(f"**Task:** {m['task']} &nbsp;·&nbsp; test period {' → '.join(m['split']['test_period'])}")
        st.dataframe(pd.DataFrame(m["metrics"]).T.round(3), width="stretch")
        imp = pd.DataFrame(m["feature_importance_gbm"]).sort_values("importance")
        fig = px.bar(
            imp,
            x="importance",
            y="feature",
            orientation="h",
            color_discrete_sequence=[CATEGORY_COLORS["long_distance"]],
            title="Permutation importance (gradient boosting)",
            labels={"feature": ""},
        )
        st.plotly_chart(fig, width="stretch")

# ----------------------------------------------------------------------- data quality
with tab_dq:
    dq = load("dq_results")
    latest = dq[dq.run_ts == dq.run_ts.max()]
    c1, c2, c3 = st.columns(3)
    c1.metric("Checks (latest run)", len(latest))
    c2.metric("Errors", int(((latest.status == "FAIL") & (latest.severity == "ERROR")).sum()))
    c3.metric("Warnings", int(((latest.status == "FAIL") & (latest.severity == "WARN")).sum()))
    st.dataframe(latest.sort_values(["status", "layer"]), width="stretch", hide_index=True)
