"""
Plot the pass/fail/skipped breakdown from benchmark.py's results.csv.
"""

import csv
import os
from collections import Counter

import matplotlib.pyplot as plt

RESULTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")
PLOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results_plot.png")

STATUS_COLORS = {"True": "#0ca30c", "False": "#d03b3b", "skipped": "#898781"}
STATUS_LABELS = {"True": "Pass", "False": "Fail", "skipped": "Skipped"}


def main():
    with open(RESULTS_PATH) as f:
        counts = Counter(row["pass"] for row in csv.DictReader(f))

    order = ["True", "False", "skipped"]
    total = sum(counts.values())
    labels = [STATUS_LABELS[k] for k in order]
    values = [counts.get(k, 0) for k in order]
    colors = [STATUS_COLORS[k] for k in order]

    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(labels, values, color=colors, width=0.6)

    for bar, value in zip(bars, values):
        pct = value / total * 100
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + total * 0.01,
            f"{value} ({pct:.0f}%)",
            ha="center",
            va="bottom",
            color="#0b0b0b",
        )

    ax.set_title(f"AskQL vs BIRD gold SQL — retails benchmark (n={total})", color="#0b0b0b")
    ax.set_ylabel("Number of questions", color="#52514e")
    ax.set_ylim(0, max(values) * 1.15)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#c3c2b7")
    ax.spines["bottom"].set_color("#c3c2b7")
    ax.tick_params(colors="#898781")
    ax.yaxis.grid(True, color="#e1e0d9", linewidth=0.8)
    ax.set_axisbelow(True)

    fig.tight_layout()
    fig.savefig(PLOT_PATH, dpi=150, facecolor="#fcfcfb")
    print(f"Saved plot to {PLOT_PATH}")


if __name__ == "__main__":
    main()