"""Shared chart style so every figure in the project looks like one system.

Colours come from a colour-blind-validated categorical palette. Colour follows the
entity: each train category (and each model) keeps the same colour in every chart.
"""

import matplotlib.pyplot as plt

CATEGORY_COLORS = {"long_distance": "#2a78d6", "regional": "#eb6834", "s_bahn": "#1baf7a", "all_rail": "#52514e"}
CATEGORY_LABELS = {"long_distance": "Long-distance", "regional": "Regional", "s_bahn": "S-Bahn", "all_rail": "All rail"}
MODEL_COLORS = {"gbm": "#2a78d6", "logistic": "#eb6834", "baseline": "#9a9890"}
NEUTRAL = "#9a9890"
INK = "#0b0b0b"
INK_2 = "#52514e"


def apply_style() -> None:
    plt.switch_backend("Agg")  # render without a display (servers, Docker, CI)
    plt.rcParams.update(
        {
            "figure.facecolor": "#fcfcfb",
            "axes.facecolor": "#fcfcfb",
            "axes.edgecolor": "#d6d5d0",
            "axes.labelcolor": INK_2,
            "axes.titlecolor": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": "#e8e7e2",
            "grid.linewidth": 0.8,
            "lines.linewidth": 2,
            "xtick.color": INK_2,
            "ytick.color": INK_2,
            "legend.frameon": False,
            "font.size": 9.5,
        }
    )
