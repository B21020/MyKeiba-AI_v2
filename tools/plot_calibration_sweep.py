"""校正sweep結果の可視化。

models/YYYYMMDD/calibration_sweep_summary.pickle から
ヒートマップ・比較プロットを images/ に保存する。

Usage:
  .venv/Scripts/python.exe tools/plot_calibration_sweep.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

plt.rcParams["font.family"] = "MS Gothic"
plt.rcParams["axes.unicode_minus"] = False


def plot_heatmap(summary: pd.DataFrame, save_path: str) -> None:
    """window × calibration のヒートマップ（tansho_ev_rr, fukusho_rr, edge_rr）。"""
    metrics = [
        ("tansho_ev_rr", "単勝EV 回収率"),
        ("fukusho_rr", "複勝 回収率"),
        ("edge_rr", "Edge 回収率"),
    ]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    windows = summary["window"].unique()
    cals = summary["calibration"].unique()

    for ax, (col, title) in zip(axes, metrics):
        matrix = np.full((len(windows), len(cals)), np.nan)
        for i, w in enumerate(windows):
            for j, c in enumerate(cals):
                row = summary[(summary["window"] == w) & (summary["calibration"] == c)]
                if len(row) > 0:
                    matrix[i, j] = row[col].values[0]

        im = ax.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=0.6, vmax=1.0)
        ax.set_xticks(range(len(cals)))
        ax.set_xticklabels(cals)
        ax.set_yticks(range(len(windows)))
        ax.set_yticklabels(windows)
        ax.set_xlabel("Calibration")
        ax.set_ylabel("Window")
        ax.set_title(title)

        for i in range(len(windows)):
            for j in range(len(cals)):
                val = matrix[i, j]
                if not np.isnan(val):
                    color = "white" if val < 0.75 else "black"
                    ax.text(j, i, f"{val:.4f}", ha="center", va="center",
                            fontsize=10, fontweight="bold", color=color)

        fig.colorbar(im, ax=ax, shrink=0.8)

    fig.suptitle("校正 × 窓長 回収率ヒートマップ (n_bets≥500)", fontsize=14)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {save_path}")


def plot_grouped_bar(summary: pd.DataFrame, save_path: str) -> None:
    """3ポリシー×3校正を窓長別にグループ棒グラフ。"""
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    windows = summary["window"].unique()
    cals = summary["calibration"].unique()

    policies_info = [
        ("tansho_ev_rr", "tansho_ev_n_bets", "単勝EV"),
        ("fukusho_rr", "fukusho_n_bets", "複勝"),
        ("edge_rr", "edge_n_bets", "Edge単勝"),
    ]

    colors = {"none": "#4c72b0", "platt": "#dd8452", "isotonic": "#55a868"}
    x = np.arange(len(windows))
    width = 0.25

    for ax, (rr_col, n_col, title) in zip(axes, policies_info):
        for k, cal in enumerate(cals):
            vals = []
            nbets = []
            for w in windows:
                row = summary[(summary["window"] == w) & (summary["calibration"] == cal)]
                if len(row) > 0:
                    vals.append(float(row[rr_col].values[0]))
                    nbets.append(int(row[n_col].values[0]))
                else:
                    vals.append(0)
                    nbets.append(0)
            bars = ax.bar(x + k * width, vals, width, label=cal, color=colors.get(cal, "gray"))
            for bar, n in zip(bars, nbets):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005,
                        f"n={n}", ha="center", va="bottom", fontsize=7, rotation=45)

        ax.axhline(1.0, color="red", linestyle="--", alpha=0.5, label="RR=1.0")
        ax.set_xticks(x + width)
        ax.set_xticklabels(windows)
        ax.set_ylabel("return_rate")
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.set_ylim(0.5, 1.1)
        ax.grid(True, axis="y", alpha=0.3)

    fig.suptitle("ポリシー別 回収率比較 (n_bets≥500)", fontsize=14)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {save_path}")


def plot_sweep_curves(summary: pd.DataFrame, detail_dir: str, save_path: str,
                      min_bets: int = 500) -> None:
    """各comboのtansho EV sweepカーブを重ね描き。"""
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    titles = ["単勝EV sweep", "複勝 sweep", "Edge sweep"]
    suffixes = ["_tansho_ev.pickle", "_fukusho.pickle", "_edge.pickle"]
    xlabels = ["margin", "threshold", "edge_threshold"]

    for ax, title, suffix, xlabel in zip(axes, titles, suffixes, xlabels):
        for _, row in summary.iterrows():
            combo = row["combo"]
            path = os.path.join(detail_dir, f"{combo}{suffix}")
            if not os.path.isfile(path):
                continue
            df = pd.read_pickle(path)
            if "n_bets" in df.columns:
                mask = df["n_bets"] >= min_bets
                df = df[mask]
            if len(df) == 0:
                continue
            ax.plot(df.index, df["return_rate"], label=combo, linewidth=1, alpha=0.8)

        ax.axhline(1.0, color="red", linestyle="--", alpha=0.5)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("return_rate")
        ax.set_title(f"{title} (n_bets≥{min_bets})")
        ax.legend(fontsize=7, ncol=2)
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {save_path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", default="models/20260208/calibration_sweep_summary.pickle")
    ap.add_argument("--out-dir", default="images")
    ap.add_argument("--min-bets", type=int, default=500)
    args = ap.parse_args()

    if not os.path.isfile(args.summary):
        print(f"ERROR: not found: {args.summary}")
        return 1

    summary = pd.read_pickle(args.summary)
    print(f"Loaded: {len(summary)} combos")
    os.makedirs(args.out_dir, exist_ok=True)

    plot_heatmap(summary, os.path.join(args.out_dir, "calibration_heatmap.png"))
    plot_grouped_bar(summary, os.path.join(args.out_dir, "calibration_grouped_bar.png"))

    detail_dir = os.path.join(os.path.dirname(args.summary), "calibration_sweep_detail")
    if os.path.isdir(detail_dir):
        plot_sweep_curves(summary, detail_dir,
                          os.path.join(args.out_dir, "calibration_sweep_curves.png"),
                          min_bets=args.min_bets)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
