import numpy as np
import pandas as pd
import pytest

from dbpipe.ml.train import evaluate, time_split


def _frame(days=30):
    dates = pd.date_range("2025-09-01", periods=days, freq="D")
    return pd.DataFrame({"service_date": np.repeat(dates, 3), "label_late": [0, 1, 0] * days})


def test_time_split_is_strictly_chronological():
    train, test = time_split(_frame(), test_days=10, warmup_days=7)
    assert train.service_date.max() < test.service_date.min()
    assert test.service_date.nunique() == 10
    assert train.service_date.min() == pd.Timestamp("2025-09-08")  # first 7 days dropped as warm-up


def test_time_split_fails_clearly_when_data_is_too_short():
    with pytest.raises(ValueError, match="Not enough days"):
        time_split(_frame(days=12), test_days=10, warmup_days=7)


def test_evaluate_perfect_ranking():
    y = np.array([0, 0, 0, 0, 1])
    p = np.array([0.1, 0.2, 0.3, 0.4, 0.9])
    m = evaluate(y, p, top_share=0.2)
    assert m["roc_auc"] == pytest.approx(1.0)
    assert m["precision_top20pct"] == 1.0
    assert m["lift_top20pct"] == pytest.approx(5.0)
