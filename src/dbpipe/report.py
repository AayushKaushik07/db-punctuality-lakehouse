"""Stage 8 – build charts and docs/RESULTS.md from the gold tables.

All numbers in RESULTS.md are computed here from the data, so the write-up can never
drift from what the pipeline actually produced. Charts go to docs/images/ so GitHub
renders them directly in the README / RESULTS page.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from dbpipe.config import Settings
from dbpipe.ingest.download import read_manifest
from dbpipe.lakehouse import read_table_pandas
from dbpipe.logging_utils import get_logger
from dbpipe.plotting import CATEGORY_COLORS, CATEGORY_LABELS, INK_2, NEUTRAL, apply_style

log = get_logger("report")

QUARANTINE_REASONS = {
    "missing_planned_time": "delay update without its timetable entry (no scheduled time, train number or type); "
    "mostly a few days where the source is missing timetable data",
    "delay_out_of_range": "delay above 24 h or more than 30 min early",
    "departure_before_arrival": "scheduled departure before scheduled arrival",
    "missing_eva": "no station id",
    "missing_id": "no stop id",
    "missing_ride_id": "no train ride id",
    "date_outside_source_month": "dated more than a day outside the file's month",
}
_IMAGES: dict = {}  # set by run_report (output folder is configurable)
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _save(fig, name: str) -> str:
    fig.tight_layout()
    fig.savefig(_IMAGES["dir"] / name, dpi=140)
    plt.close(fig)
    return f"images/{name}"


# --------------------------------------------------------------------------- charts
def chart_daily(daily: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10, 4))
    for cat in ["long_distance", "regional", "s_bahn", "all_rail"]:
        d = daily[daily.train_category == cat].sort_values("service_date")
        if d.empty:
            continue
        lw = 2.4 if cat == "all_rail" else 2
        ax.plot(
            d.service_date,
            d.punctuality_pct_7d_avg,
            color=CATEGORY_COLORS[cat],
            lw=lw,
            ls="--" if cat == "all_rail" else "-",
            label=CATEGORY_LABELS[cat],
        )
        last = d.iloc[-1]
        ax.annotate(
            f"{last.punctuality_pct_7d_avg:.0f}%",
            (last.service_date, last.punctuality_pct_7d_avg),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            color=INK_2,
            fontsize=8.5,
        )
    ax.set(title="Punctuality by train category (7-day moving average)", ylabel="punctual arrivals, %")
    ax.legend(loc="lower left", ncols=4)
    return _save(fig, "punctuality_daily.png")


def chart_heatmap(hourly: pd.DataFrame) -> str:
    h = hourly[(hourly.train_category == "all_rail") & (hourly.measured_arrivals >= 30)]
    grid = h.pivot_table(index="weekday", columns="event_hour", values="punctuality_pct")
    grid = grid.reindex(index=range(7), columns=range(24))
    fig, ax = plt.subplots(figsize=(10, 3.4))
    ax.grid(False)
    # single hue, light -> dark: darker = LESS punctual (the thing to look at)
    im = ax.imshow(100 - grid.to_numpy(), cmap="Blues", aspect="auto", vmin=0)
    ax.set_yticks(range(7), WEEKDAYS)
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}" for h in range(0, 24, 2)])
    ax.set(title="Share of late arrivals by weekday and hour (all rail)", xlabel="hour of planned arrival")
    cb = fig.colorbar(im, ax=ax, pad=0.01)
    cb.set_label("late arrivals, %", color=INK_2)
    cb.outline.set_visible(False)
    return _save(fig, "late_share_heatmap.png")


def station_period_ranking(stations: pd.DataFrame, months: list[str], min_arrivals_per_month: int) -> pd.DataFrame:
    """Station punctuality pooled over all processed months, worst first."""
    m = stations[stations.source_month.isin(months)].copy()
    m["punctual"] = m.punctuality_pct * m.measured_arrivals / 100
    m["canceled"] = m.cancellation_pct * m.scheduled_stops / 100
    m["delay_sum"] = m.avg_arrival_delay_min * m.measured_arrivals
    worst_month = m.loc[m.groupby("eva").punctuality_pct.idxmin(), ["eva", "source_month"]].set_index("eva")
    g = m.groupby(["eva", "station_name"], as_index=False)[
        ["measured_arrivals", "scheduled_stops", "punctual", "canceled", "delay_sum"]
    ].sum()
    g = g[g.measured_arrivals >= min_arrivals_per_month * len(months)]
    out = pd.DataFrame(
        {
            "station_name": g.station_name,
            "measured_arrivals": g.measured_arrivals.astype(int),
            "punctuality_pct": (100 * g.punctual / g.measured_arrivals).round(2),
            "cancellation_pct": (100 * g.canceled / g.scheduled_stops).round(2),
            "avg_arrival_delay_min": (g.delay_sum / g.measured_arrivals).round(2),
            "worst_month": g.eva.map(worst_month.source_month),
        }
    ).sort_values("punctuality_pct")
    out.insert(0, "rank", range(1, len(out) + 1))
    return out.reset_index(drop=True)


def chart_worst_stations(ranking: pd.DataFrame, period: str, n: int = 15) -> str:
    s = ranking.head(n).sort_values("punctuality_pct", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 0.32 * len(s) + 1.2))
    y = np.arange(len(s))
    ax.barh(y, s.punctuality_pct, color=CATEGORY_COLORS["long_distance"], height=0.7)
    ax.set_yticks(y, s.station_name)
    for yi, v in zip(y, s.punctuality_pct, strict=True):
        ax.text(v + 0.5, yi, f"{v:.1f}%", va="center", color=INK_2, fontsize=8.5)
    ax.set(title=f"{len(s)} least punctual stations, {period}", xlabel="punctual arrivals, %")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, 100)
    return _save(fig, "worst_stations.png")


def chart_hotspots(hot: pd.DataFrame, n: int = 15) -> str | None:
    if hot.empty:  # e.g. a small sample where no segment reaches min_segment_observations
        return None
    h = hot.nsmallest(n, "hotspot_rank").iloc[::-1]  # worst at the top
    labels = h.from_station + " → " + h.to_station
    fig, ax = plt.subplots(figsize=(8.5, 0.32 * len(h) + 1.4))
    colors = [CATEGORY_COLORS.get(c, NEUTRAL) for c in h.main_train_category]
    y = np.arange(len(h))
    ax.barh(y, h.median_gain_per_hour, color=colors, height=0.7)
    ax.set_yticks(y, labels)
    for yi, v, share, plan in zip(
        y, h.median_gain_per_hour, h.pct_trains_gaining_3min, h.median_planned_min, strict=True
    ):
        ax.text(
            v + 0.3,
            yi,
            f"{v:.0f} min/h  ·  {share:.0f}% lose 3+ min  ·  {plan:.0f} min scheduled",
            va="center",
            color=INK_2,
            fontsize=8,
        )
    present = [c for c in ("long_distance", "regional", "s_bahn") if c in set(h.main_train_category)]
    handles = [plt.Rectangle((0, 0), 1, 1, color=CATEGORY_COLORS[c]) for c in present]
    ax.legend(handles, [CATEGORY_LABELS[c] for c in present], loc="upper center", bbox_to_anchor=(0.4, -0.13), ncols=3)
    ax.set_xlim(0, h.median_gain_per_hour.max() * 2.4)  # room for the text labels
    ax.set(
        title="Where trains lose the most time",
        xlabel="median minutes of delay gained per hour of scheduled running time",
    )
    ax.grid(axis="y", visible=False)
    return _save(fig, "segment_hotspots.png")


SEGMENT_LABELS = {"fernverkehr": "Long-distance", "regio": "Regional", "gesamt": "All passenger rail"}


def chart_official(comp: pd.DataFrame) -> str | None:
    c = comp.dropna(subset=["official_punctuality_pct"])
    if c.empty:
        return None
    periods = sorted(c.period.unique(), key=lambda p: (c[c.period == p].first_month.iloc[0], p))
    segments = [s for s in ("fernverkehr", "regio", "gesamt") if s in set(c.segment)]
    fig, axes = plt.subplots(1, len(segments), figsize=(4.2 * len(segments), 3.8), sharey=True, squeeze=False)
    x = np.arange(len(periods))
    for ax, seg in zip(axes[0], segments, strict=True):
        d = c[c.segment == seg].set_index("period").reindex(periods)
        ax.bar(x - 0.2, d.our_punctuality_pct, 0.38, color=CATEGORY_COLORS["long_distance"], label="This pipeline")
        ax.bar(x + 0.2, d.official_punctuality_pct, 0.38, color=NEUTRAL, label="DB published")
        ax.set_xticks(x, [p.replace("-H1", "\nH1") for p in periods], fontsize=8.5)
        ax.set_title(SEGMENT_LABELS.get(seg, seg), fontsize=10.5)
        ax.set_ylim(0, 100)
        ax.grid(axis="x", visible=False)
    axes[0][0].set_ylabel("punctual, %")
    fig.suptitle("Our punctuality vs. Deutsche Bahn's published figures", x=0.01, ha="left", fontweight="bold")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncols=2, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(_IMAGES["dir"] / "official_comparison.png", dpi=140)
    plt.close(fig)
    return "images/official_comparison.png"


def chart_distribution(dist: pd.DataFrame) -> str:
    buckets = sorted(dist.delay_bucket.unique())
    x = np.arange(len(buckets))
    cats = [c for c in ("long_distance", "regional", "s_bahn") if c in set(dist.train_category)]
    width = 0.8 / max(len(cats), 1)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    for i, cat in enumerate(cats):
        d = dist[dist.train_category == cat].set_index("delay_bucket").reindex(buckets)
        ax.bar(
            x + (i - (len(cats) - 1) / 2) * width,
            d.pct_of_category.fillna(0),
            width * 0.92,
            color=CATEGORY_COLORS[cat],
            label=CATEGORY_LABELS[cat],
        )
    ax.set_xticks(x, [b.split(": ", 1)[1] for b in buckets])
    ax.set(title="How late? Distribution of arrival delays", ylabel="% of arrivals in category")
    ax.grid(axis="x", visible=False)
    ax.legend()
    return _save(fig, "delay_distribution.png")


# --------------------------------------------------------------------------- text
def _md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |")
    return "\n".join(lines)


def _monthly_table(daily: pd.DataFrame) -> pd.DataFrame:
    """Punctuality per month and category (pooled over the month's days)."""
    d = daily.assign(month=daily.service_date.dt.strftime("%Y-%m"))
    g = d.groupby(["month", "train_category"])[["punctual_arrivals", "measured_arrivals"]].sum()
    pct = (100 * g.punctual_arrivals / g.measured_arrivals).unstack("train_category")
    cols = [c for c in ("long_distance", "regional", "s_bahn", "all_rail") if c in pct.columns]
    out = pct[cols].round(1).rename(columns=CATEGORY_LABELS).reset_index().rename(columns={"month": "Month"})
    return out


def _category_kpis(daily: pd.DataFrame) -> pd.DataFrame:
    g = daily.groupby("train_category").agg(
        measured=("measured_arrivals", "sum"),
        punctual=("punctual_arrivals", "sum"),
        stops=("scheduled_stops", "sum"),
        canceled=("canceled_stops", "sum"),
    )
    out = pd.DataFrame(
        {
            "Category": [CATEGORY_LABELS.get(i, i) for i in g.index],
            "Measured arrivals": g.measured.map("{:,}".format),
            "Punctuality": (100 * g.punctual / g.measured).map("{:.1f}%".format),
            "Cancelled stops": (100 * g.canceled / g.stops).map("{:.1f}%".format),
        }
    )
    order = {"Long-distance": 0, "Regional": 1, "S-Bahn": 2, "All rail": 3}
    return out.sort_values("Category", key=lambda s: s.map(order)).reset_index(drop=True)


def run_report(settings: Settings) -> None:
    apply_style()
    docs = settings.path("docs")
    images = docs / "images"
    images.mkdir(parents=True, exist_ok=True)
    _IMAGES["dir"] = images

    def g(name: str) -> pd.DataFrame:
        return read_table_pandas(settings, "gold", name)

    daily, hourly, stations = g("agg_punctuality_daily"), g("agg_hourly_profile"), g("agg_station_monthly")
    hot, comp, dist = g("agg_segment_hotspots"), g("agg_official_comparison"), g("agg_delay_distribution")
    dq = g("dq_results")
    daily["service_date"] = pd.to_datetime(daily["service_date"])
    months = sorted(settings.months)
    period = months[0] if len(months) == 1 else f"{months[0]} to {months[-1]}"
    ranking = station_period_ranking(stations, months, settings.section("analytics")["min_station_arrivals"])

    charts = {
        "daily": chart_daily(daily),
        "heatmap": chart_heatmap(hourly),
        "stations": chart_worst_stations(ranking, period),
        "hotspots": chart_hotspots(hot),
        "official": chart_official(comp),
        "distribution": chart_distribution(dist),
    }

    # model charts produced by the train stage
    model_charts = {}
    for name in ("model_roc_pr.png", "model_feature_importance.png", "model_calibration.png"):
        src = settings.path("artifacts") / "charts" / name
        if src.exists():
            shutil.copy(src, images / name)
            model_charts[name] = f"images/{name}"
    metrics_path = settings.path("artifacts") / "metrics.json"
    metrics = json.loads(metrics_path.read_text()) if metrics_path.exists() else None

    manifest = read_manifest(settings)
    synthetic = any(manifest.get(m, {}).get("source") == "synthetic-sample" for m in settings.months)

    md = _build_markdown(
        settings, daily, hourly, ranking, hot, comp, dq, charts, model_charts, metrics, synthetic, period
    )
    (docs / "RESULTS.md").write_text(md, encoding="utf-8")
    log.info("Wrote %s and %d charts", docs / "RESULTS.md", len(charts) + len(model_charts))


def _build_markdown(
    settings, daily, hourly, ranking, hot, comp, dq, charts, model_charts, metrics, synthetic, period
) -> str:
    out: list[str] = ["# Results", ""]
    if synthetic:
        out += [
            "> **⚠️ Generated from the SYNTHETIC test sample, not real data.** These numbers only prove",
            "> the pipeline runs. Run the full pipeline on the downloaded data (docs/SETUP.md) for real results.",
            "",
        ]
    out += [
        f"_Generated automatically by `python -m dbpipe report` on {datetime.now():%Y-%m-%d %H:%M}. "
        f"Months: {', '.join(settings.months)}. Data: Deutsche Bahn via "
        f"[piebro/deutsche-bahn-data](https://huggingface.co/datasets/{settings.hf_repo_id}) (CC BY 4.0)._",
        "",
        "## 1. Headline KPIs",
        "",
        _md_table(_category_kpis(daily)),
        "",
        "Punctual = arrival less than 6 minutes late (Deutsche Bahn definition). Cancelled stops are excluded "
        "from punctuality and reported separately.",
        "",
        f"![Daily punctuality]({charts['daily']})",
        "",
        "Punctuality (%) by month:",
        "",
        _md_table(_monthly_table(daily)),
        "",
    ]

    # official comparison
    c = comp.dropna(subset=["official_punctuality_pct"]).sort_values(["first_month", "segment"])
    out += ["## 2. Validation against Deutsche Bahn's published numbers", ""]
    if c.empty:
        out += ["No overlapping periods with `reference/db_official_punctuality.csv`.", ""]
    else:
        t = c[["period", "segment", "our_punctuality_pct", "official_punctuality_pct", "difference_pp"]].rename(
            columns={
                "our_punctuality_pct": "ours %",
                "official_punctuality_pct": "DB published %",
                "difference_pp": "difference (pp)",
            }
        )
        out += [
            _md_table(t),
            "",
            "DB's figures: monthly values from DB's punctuality page and the H1 (January–June) average from DB's "
            "2025 interim report; for a half-year our arrivals are pooled over the same months. Months without "
            "a published DB value (see `reference/README.md`) are not compared.",
            "",
            f"Mean absolute gap: **{c.difference_pp.abs().mean():.1f} percentage points**. Gaps are expected: until "
            "2025-10 the dataset only covers the ~100 biggest stations (hubs are typically less punctual than small "
            "stations), it includes non-DB operators, and DB uses internal data for every stop.",
            "",
        ]
        if charts["official"]:
            out += [f"![Official comparison]({charts['official']})", ""]

    # time patterns
    h = hourly[(hourly.train_category == "all_rail") & (hourly.measured_arrivals >= 100)]
    by_hour = h.groupby("event_hour").apply(
        lambda d: np.average(d.punctuality_pct, weights=d.measured_arrivals), include_groups=False
    )
    by_day = h.groupby("weekday").apply(
        lambda d: np.average(d.punctuality_pct, weights=d.measured_arrivals), include_groups=False
    )
    out += ["## 3. When are trains late?", ""]
    if len(by_hour):
        out += [
            f"- Least punctual hour: **{int(by_hour.idxmin()):02d}:00** ({by_hour.min():.1f}% punctual); "
            f"most punctual: **{int(by_hour.idxmax()):02d}:00** ({by_hour.max():.1f}%).",
            f"- Least punctual weekday: **{WEEKDAYS[int(by_day.idxmin())]}** ({by_day.min():.1f}%); "
            f"most punctual: **{WEEKDAYS[int(by_day.idxmax())]}** ({by_day.max():.1f}%).",
            "",
        ]
    out += [f"![Heatmap]({charts['heatmap']})", "", f"![Delay distribution]({charts['distribution']})", ""]

    # stations & segments
    min_arr = settings.section("analytics")["min_station_arrivals"]
    out += [
        "## 4. Stations and segments",
        "",
        f"Least punctual stations, {period}, pooled over all months (min. {min_arr} arrivals per month; "
        "`worst_month` = the station's least punctual month):",
        "",
        _md_table(ranking.head(10)),
        "",
        f"![Worst stations]({charts['stations']})",
        "",
        "Segments where trains lose the most time, as median minutes of delay gained per hour of scheduled "
        "running time (between two consecutive covered stations; min. "
        f"{settings.section('analytics')['min_segment_observations']} trains and "
        f"{settings.section('analytics')['min_segment_planned_min']} scheduled minutes):",
        "",
        _md_table(
            hot.nsmallest(10, "hotspot_rank")[
                [
                    "hotspot_rank",
                    "from_station",
                    "to_station",
                    "main_train_category",
                    "observations",
                    "median_planned_min",
                    "median_gain_per_hour",
                    "median_delay_gain_min",
                    "pct_trains_gaining_3min",
                ]
            ]
        ),
        "",
    ]
    if charts["hotspots"]:
        out += [f"![Hotspots]({charts['hotspots']})", ""]

    # model
    out += ["## 5. Delay prediction model", ""]
    if metrics:
        m = metrics["metrics"]
        rows = pd.DataFrame(
            [
                {
                    "model": name,
                    "ROC-AUC": f"{v['roc_auc']:.3f}",
                    "PR-AUC": f"{v['pr_auc']:.3f}",
                    "Brier": f"{v['brier']:.4f}",
                    "precision @ top 20%": f"{v['precision_top20pct']:.1%}",
                    "lift @ top 20%": f"{v['lift_top20pct']:.2f}x",
                }
                for name, v in m.items()
            ]
        )
        sp = metrics["split"]
        top = ", ".join(f"`{f['feature']}`" for f in metrics["feature_importance_gbm"][:3])
        gain = m["gbm"]["roc_auc"] - m["baseline"]["roc_auc"]
        out += [
            f"Task: {metrics['task']} for a scheduled arrival, using only information available before the day "
            f"starts. Time-based split: train {sp['train_period'][0]} → {sp['train_period'][1]} "
            f"({sp['train_rows']:,} rows), test {sp['test_period'][0]} → {sp['test_period'][1]} "
            f"({sp['test_rows']:,} rows). Late rate in the test period: {m['gbm']['base_rate']:.1%}.",
            "",
            _md_table(rows),
            "",
            f"- Gradient boosting vs. the no-ML baseline: **{gain:+.3f} ROC-AUC**.",
            f"- Most important features: {top}.",
            f"- Flagging the 20% riskiest arrivals catches {m['gbm']['recall_top20pct']:.0%} of all late arrivals.",
            "",
        ]
        out += [f"![{n}]({p})" for n, p in model_charts.items()] + [""]
    else:
        out += ["Model not trained yet (`python -m dbpipe train`).", ""]

    # data quality
    latest = dq[dq.run_ts == dq.run_ts.max()]
    failed = latest[latest.status == "FAIL"]
    out += [
        "## 6. Data quality (latest run)",
        "",
        f"{len(latest)} checks: **{(latest.status == 'PASS').sum()} passed**, "
        f"{((latest.status == 'FAIL') & (latest.severity == 'ERROR')).sum()} errors, "
        f"{((latest.status == 'FAIL') & (latest.severity == 'WARN')).sum()} warnings.",
        "",
    ]
    if not failed.empty:
        out += [_md_table(failed[["check", "severity", "observed", "threshold", "detail"]].round(3)), ""]
    try:
        q = read_table_pandas(settings, "silver", "quarantine")
        reasons = q.dq_reasons.str.split(",").explode().value_counts().rename_axis("reason").reset_index(name="rows")
        reasons["meaning"] = reasons.reason.map(QUARANTINE_REASONS).fillna("")
        out += ["Rows quarantined in silver, by reason:", "", _md_table(reasons), ""]
    except FileNotFoundError:
        pass
    return "\n".join(out)
