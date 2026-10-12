"""Stage 7 – train and evaluate the delay model (scikit-learn).

Evaluation is done the way the model would be used: train on the past, test on the
most recent `test_days` days (time-based split, never a random split for time data).

Three models are compared:
  1. baseline      – historical late rate of the station at that hour (no ML at all)
  2. logistic      – logistic regression (interpretable linear model)
  3. gbm           – histogram gradient boosting (non-linear, handles missing values)
"""

from __future__ import annotations

import json
from datetime import timedelta

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

from dbpipe.config import Settings
from dbpipe.lakehouse import read_table_pandas
from dbpipe.logging_utils import get_logger
from dbpipe.ml.features import CATEGORICAL, LABEL, NUMERIC
from dbpipe.plotting import MODEL_COLORS, NEUTRAL, apply_style

log = get_logger("train")


# --------------------------------------------------------------------------- data
def time_split(df: pd.DataFrame, test_days: int, warmup_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["service_date"] = pd.to_datetime(df["service_date"])
    first, last = df["service_date"].min(), df["service_date"].max()
    df = df[df["service_date"] >= first + timedelta(days=warmup_days)]  # rolling features not filled yet
    cutoff = last - timedelta(days=test_days - 1)
    train, test = df[df["service_date"] < cutoff], df[df["service_date"] >= cutoff]
    if train.empty or test.empty:
        raise ValueError(
            f"Not enough days for the split (data {first.date()}..{last.date()}, "
            f"warmup={warmup_days}, test={test_days}). Process more months or lower ml.test_days."
        )
    return train, test


# --------------------------------------------------------------------------- models
def make_logistic(random_state: int) -> Pipeline:
    pre = ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=50), CATEGORICAL + ["event_hour", "weekday"]),
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), NUMERIC),
        ]
    )
    return Pipeline([("pre", pre), ("model", LogisticRegression(max_iter=1000, random_state=random_state))])


def make_gbm(random_state: int) -> Pipeline:
    pre = ColumnTransformer(
        [
            (
                "cat",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1),
                CATEGORICAL,
            ),
            ("num", "passthrough", NUMERIC),  # gradient boosting handles NaN natively
        ],
        verbose_feature_names_out=False,
    )
    model = HistGradientBoostingClassifier(
        learning_rate=0.08,
        max_iter=300,
        max_leaf_nodes=31,
        min_samples_leaf=100,
        l2_regularization=1.0,
        categorical_features=list(range(len(CATEGORICAL))),
        early_stopping=True,
        validation_fraction=0.1,
        random_state=random_state,
    )
    return Pipeline([("pre", pre), ("model", model)])


def baseline_scores(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Historical station-hour late rate; fall back to the station's 7-day rate, then the overall rate."""
    overall = train[LABEL].mean()
    return test["station_hour_late_rate_hist"].fillna(test["station_late_rate_7d"]).fillna(overall).to_numpy()


# --------------------------------------------------------------------------- metrics
def evaluate(y: np.ndarray, p: np.ndarray, top_share: float = 0.2) -> dict:
    """Standard metrics + a business-style one: how good are the X% highest-risk predictions?"""
    k = max(1, int(len(p) * top_share))
    top = np.argsort(-p)[:k]
    precision_top = float(y[top].mean())
    return {
        "roc_auc": float(roc_auc_score(y, p)),
        "pr_auc": float(average_precision_score(y, p)),
        "brier": float(brier_score_loss(y, p)),
        "base_rate": float(y.mean()),
        f"precision_top{int(top_share * 100)}pct": precision_top,
        f"lift_top{int(top_share * 100)}pct": precision_top / max(float(y.mean()), 1e-9),
        f"recall_top{int(top_share * 100)}pct": float(y[top].sum() / max(y.sum(), 1)),
    }


def _plot_curves(y: np.ndarray, scores: dict[str, np.ndarray], out) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    for name, p in scores.items():
        fpr, tpr, _ = roc_curve(y, p)
        prec, rec, _ = precision_recall_curve(y, p)
        axes[0].plot(fpr, tpr, color=MODEL_COLORS[name], label=f"{name} (AUC {roc_auc_score(y, p):.3f})")
        axes[1].plot(rec, prec, color=MODEL_COLORS[name], label=f"{name} (AP {average_precision_score(y, p):.3f})")
    axes[0].plot([0, 1], [0, 1], ls="--", c=NEUTRAL, lw=1)
    axes[1].axhline(y.mean(), ls="--", c=NEUTRAL, lw=1, label=f"base rate {y.mean():.2f}")
    axes[0].set(title="ROC curve (test period)", xlabel="False positive rate", ylabel="True positive rate")
    axes[1].set(title="Precision-recall (test period)", xlabel="Recall", ylabel="Precision")
    for ax in axes:
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)


def _plot_importance(model: Pipeline, test: pd.DataFrame, random_state: int, out) -> list[dict]:
    sample = test.sample(n=min(len(test), 20000), random_state=random_state)
    result = permutation_importance(
        model,
        sample[CATEGORICAL + NUMERIC],
        sample[LABEL],
        scoring="roc_auc",
        n_repeats=3,
        random_state=random_state,
        n_jobs=1,
    )
    imp = pd.DataFrame({"feature": CATEGORICAL + NUMERIC, "importance": result.importances_mean}).sort_values(
        "importance", ascending=True
    )
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(imp["feature"], imp["importance"], color=MODEL_COLORS["gbm"])
    ax.set(title="Permutation importance (drop in ROC-AUC)", xlabel="importance")
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return imp.sort_values("importance", ascending=False).round(5).to_dict("records")


def _plot_calibration(y: np.ndarray, p: np.ndarray, out) -> None:
    bins = pd.qcut(p, q=10, duplicates="drop")
    cal = (
        pd.DataFrame({"p": p, "y": y, "bin": bins})
        .groupby("bin", observed=True)
        .agg(pred=("p", "mean"), obs=("y", "mean"))
    )
    fig, ax = plt.subplots(figsize=(4.8, 4.5))
    ax.plot([0, 1], [0, 1], ls="--", c=NEUTRAL, lw=1)
    ax.plot(cal["pred"], cal["obs"], marker="o", color=MODEL_COLORS["gbm"])
    ax.set(title="Calibration (gbm, test period)", xlabel="predicted probability", ylabel="observed late rate")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    plt.close(fig)


# --------------------------------------------------------------------------- main
def run_train(settings: Settings) -> dict:
    apply_style()
    ml = settings.section("ml")
    rs = ml["random_state"]
    out_dir = settings.path("artifacts")
    (out_dir / "charts").mkdir(parents=True, exist_ok=True)
    (out_dir / "model").mkdir(parents=True, exist_ok=True)

    df = read_table_pandas(settings, "gold", "ml_training_set")
    train, test = time_split(df, ml["test_days"], ml["warmup_days"])
    X_cols = CATEGORICAL + NUMERIC
    log.info(
        "Train: %s rows (%s..%s) | Test: %s rows (%s..%s) | late rate train %.1f%% / test %.1f%%",
        f"{len(train):,}",
        train.service_date.min().date(),
        train.service_date.max().date(),
        f"{len(test):,}",
        test.service_date.min().date(),
        test.service_date.max().date(),
        100 * train[LABEL].mean(),
        100 * test[LABEL].mean(),
    )

    y_test = test[LABEL].to_numpy()
    scores = {"baseline": baseline_scores(train, test)}
    models = {"logistic": make_logistic(rs), "gbm": make_gbm(rs)}
    for name, model in models.items():
        model.fit(train[X_cols], train[LABEL])
        scores[name] = model.predict_proba(test[X_cols])[:, 1]

    metrics = {name: evaluate(y_test, p) for name, p in scores.items()}
    for name, m in metrics.items():
        log.info(
            "%-9s ROC-AUC %.3f | PR-AUC %.3f | Brier %.4f | precision@top20%% %.3f (lift %.2fx)",
            name,
            m["roc_auc"],
            m["pr_auc"],
            m["brier"],
            m["precision_top20pct"],
            m["lift_top20pct"],
        )

    _plot_curves(y_test, scores, out_dir / "charts" / "model_roc_pr.png")
    _plot_calibration(y_test, scores["gbm"], out_dir / "charts" / "model_calibration.png")
    importance = _plot_importance(models["gbm"], test, rs, out_dir / "charts" / "model_feature_importance.png")

    joblib.dump(models["gbm"], out_dir / "model" / "delay_model_gbm.joblib")
    summary = {
        "task": f"P(arrival delay >= {ml['label_threshold_min']} min)",
        "split": {
            "train_rows": len(train),
            "test_rows": len(test),
            "train_period": [str(train.service_date.min().date()), str(train.service_date.max().date())],
            "test_period": [str(test.service_date.min().date()), str(test.service_date.max().date())],
        },
        "features": {"categorical": CATEGORICAL, "numeric": NUMERIC},
        "metrics": metrics,
        "feature_importance_gbm": importance,
    }
    (out_dir / "metrics.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("Saved model, metrics.json and charts to %s", out_dir)
    return summary
