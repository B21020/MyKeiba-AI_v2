"""窓長比較結果の可視化スクリプト。

models/YYYYMMDD/window_comparison_summary.pickle と各 sweep pickle を読み込み、
以下のプロットを images/ に保存する。

1. AUC比較（棒グラフ: train vs test）
2. 単勝 EV return_rate vs margin（折れ線, 窓長別）
3. 複勝 return_rate vs threshold（折れ線, 窓長別）
4. 信頼性散布図（n_bets vs return_rate, 窓長別）

Usage:
  .venv/Scripts/python.exe tools/plot_window_comparison.py
  .venv/Scripts/python.exe tools/plot_window_comparison.py --summary models/20260208/window_comparison_summary.pickle
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

# 日本語フォント設定（Windows向け）
plt.rcParams["font.family"] = "MS Gothic"
plt.rcParams["axes.unicode_minus"] = False


def _load_sweep_pickles(summary: pd.DataFrame, col: str) -> dict[str, pd.DataFrame]:
    """sweep pickle を辞書 {label: DataFrame} として読み込む。"""
    out = {}
    for _, row in summary.iterrows():
        label = row["label"]
        path = row[col]
        if pd.notna(path) and os.path.isfile(path):
            out[label] = pd.read_pickle(path)
    return out


# -----------------------------------------------------------------------
# Plot 1: AUC comparison
# -----------------------------------------------------------------------
def plot_auc_comparison(summary: pd.DataFrame, save_path: str) -> None:
    labels = summary["label"].tolist()
    auc_train = summary["auc_train"].tolist()
    auc_test = summary["auc_test"].tolist()

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    bars1 = ax.bar(x - width / 2, auc_train, width, label="Train AUC", color="#4c72b0")
    bars2 = ax.bar(x + width / 2, auc_test, width, label="Test AUC", color="#dd8452")

    ax.set_ylabel("AUC")
    ax.set_title("窓長別 AUC 比較")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.set_ylim(0.65, 0.90)

    # 値表示
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.003,
                f"{bar.get_height():.4f}", ha="center", va="bottom", fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.003,
                f"{bar.get_height():.4f}", ha="center", va="bottom", fontsize=9)

    # train行数を副情報として
    for i, row in summary.iterrows():
        ax.text(x[i], 0.66, f"train={row['train_rows']:,}", ha="center", fontsize=7, color="gray")

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  saved: {save_path}")


# -----------------------------------------------------------------------
# Plot 2: 単勝EV return_rate vs margin
# -----------------------------------------------------------------------
def plot_tansho_sweep(sweeps: dict[str, pd.DataFrame], save_path: str,
                      min_bets: int = 500) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # 左: return_rate
    ax = axes[0]
    for label, df in sweeps.items():
        mask = df["n_bets"] >= min_bets if "n_bets" in df.columns else pd.Series(True, index=df.index)
        d = df[mask]
        ax.plot(d.index, d["return_rate"], label=label, marker="", linewidth=1.5)
    ax.axhline(1.0, color="red", linestyle="--", alpha=0.5, label="回収率=1.0")
    ax.set_xlabel("margin")
    ax.set_ylabel("return_rate")
    ax.set_title(f"単勝EV sweep (return_rate, n_bets≥{min_bets})")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 右: n_bets
    ax = axes[1]
    for label, df in sweeps.items():
        ax.plot(df.index, df["n_bets"], label=label, marker="", linewidth=1.5)
    ax.axhline(min_bets, color="red", linestyle="--", alpha=0.5, label=f"n_bets={min_bets}")
    ax.set_xlabel("margin")
    ax.set_ylabel("n_bets")
    ax.set_title("単勝EV sweep (n_bets)")
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  saved: {save_path}")


# -----------------------------------------------------------------------
# Plot 3: 複勝 return_rate vs threshold
# -----------------------------------------------------------------------
def plot_fukusho_sweep(sweeps: dict[str, pd.DataFrame], save_path: str,
                       min_bets: int = 500) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax = axes[0]
    for label, df in sweeps.items():
        mask = df["n_bets"] >= min_bets if "n_bets" in df.columns else pd.Series(True, index=df.index)
        d = df[mask]
        ax.plot(d.index, d["return_rate"], label=label, marker="", linewidth=1.5)
    ax.axhline(1.0, color="red", linestyle="--", alpha=0.5, label="回収率=1.0")
    ax.set_xlabel("threshold")
    ax.set_ylabel("return_rate")
    ax.set_title(f"複勝 sweep (return_rate, n_bets≥{min_bets})")
    ax.legend()
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    for label, df in sweeps.items():
        ax.plot(df.index, df["n_bets"], label=label, marker="", linewidth=1.5)
    ax.axhline(min_bets, color="red", linestyle="--", alpha=0.5, label=f"n_bets={min_bets}")
    ax.set_xlabel("threshold")
    ax.set_ylabel("n_bets")
    ax.set_title("複勝 sweep (n_bets)")
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  saved: {save_path}")


# -----------------------------------------------------------------------
# Plot 4: n_bets vs return_rate 散布図（best ポイント表示）
# -----------------------------------------------------------------------
def plot_reliability_scatter(summary: pd.DataFrame, save_path: str) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    colors = ["#4c72b0", "#dd8452", "#55a868", "#c44e52"]

    # 単勝
    ax = axes[0]
    for i, (_, row) in enumerate(summary.iterrows()):
        c = colors[i % len(colors)]
        ax.scatter(row["best_tansho_n_bets"], row["best_tansho_return_rate"],
                   s=100, color=c, zorder=5)
        ax.annotate(row["label"],
                    (row["best_tansho_n_bets"], row["best_tansho_return_rate"]),
                    textcoords="offset points", xytext=(8, 4), fontsize=10, color=c)
    ax.axhline(1.0, color="red", linestyle="--", alpha=0.5)
    ax.axvline(500, color="gray", linestyle=":", alpha=0.5, label="n_bets=500")
    ax.set_xlabel("n_bets")
    ax.set_ylabel("return_rate")
    ax.set_title("単勝 best: n_bets vs return_rate")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 複勝
    ax = axes[1]
    for i, (_, row) in enumerate(summary.iterrows()):
        c = colors[i % len(colors)]
        ax.scatter(row["best_fukusho_n_bets"], row["best_fukusho_return_rate"],
                   s=100, color=c, zorder=5)
        ax.annotate(row["label"],
                    (row["best_fukusho_n_bets"], row["best_fukusho_return_rate"]),
                    textcoords="offset points", xytext=(8, 4), fontsize=10, color=c)
    ax.axhline(1.0, color="red", linestyle="--", alpha=0.5)
    ax.axvline(500, color="gray", linestyle=":", alpha=0.5, label="n_bets=500")
    ax.set_xlabel("n_bets")
    ax.set_ylabel("return_rate")
    ax.set_title("複勝 best: n_bets vs return_rate")
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(save_path, dpi=150)
    plt.close(fig)
    print(f"  saved: {save_path}")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="窓長比較プロット生成")
    ap.add_argument("--summary",
                    default="models/20260208/window_comparison_summary.pickle",
                    help="summary pickle path")
    ap.add_argument("--out-dir", default="images", help="画像出力先")
    ap.add_argument("--min-bets", type=int, default=500,
                    help="sweepプロットの信頼性フィルタ (n_bets下限)")
    args = ap.parse_args()

    if not os.path.isfile(args.summary):
        print(f"ERROR: not found: {args.summary}")
        return 1

    summary = pd.read_pickle(args.summary)
    print(f"Loaded summary: {len(summary)} rows")
    print(summary[["label", "auc_test", "best_tansho_return_rate", "best_fukusho_return_rate"]].to_string())

    os.makedirs(args.out_dir, exist_ok=True)

    # AUC比較
    plot_auc_comparison(summary, os.path.join(args.out_dir, "window_auc_comparison.png"))

    # sweep pickles 読み込み
    tansho_sweeps = _load_sweep_pickles(summary, "tansho_pickle")
    fukusho_sweeps = _load_sweep_pickles(summary, "fukusho_pickle")

    if tansho_sweeps:
        plot_tansho_sweep(tansho_sweeps,
                          os.path.join(args.out_dir, "window_tansho_sweep.png"),
                          min_bets=args.min_bets)
    if fukusho_sweeps:
        plot_fukusho_sweep(fukusho_sweeps,
                           os.path.join(args.out_dir, "window_fukusho_sweep.png"),
                           min_bets=args.min_bets)

    # 信頼性散布図
    plot_reliability_scatter(summary, os.path.join(args.out_dir, "window_reliability_scatter.png"))

    # n_bets >= min_bets でフィルタした best を表示
    print(f"\n--- Best with n_bets >= {args.min_bets} ---")
    for label, df in tansho_sweeps.items():
        reliable = df[df["n_bets"] >= args.min_bets] if "n_bets" in df.columns else df
        if len(reliable) > 0:
            best = reliable.sort_values("return_rate", ascending=False).iloc[0]
            best_margin = reliable.sort_values("return_rate", ascending=False).index[0]
            print(f"  [{label}] tansho: margin={best_margin:.4f}  RR={best['return_rate']:.4f}  n={best['n_bets']:.0f}  std={best.get('std', float('nan')):.4f}")
    for label, df in fukusho_sweeps.items():
        reliable = df[df["n_bets"] >= args.min_bets] if "n_bets" in df.columns else df
        if len(reliable) > 0:
            best = reliable.sort_values("return_rate", ascending=False).iloc[0]
            best_th = reliable.sort_values("return_rate", ascending=False).index[0]
            print(f"  [{label}] fukusho: th={best_th:.4f}  RR={best['return_rate']:.4f}  n={best['n_bets']:.0f}  std={best.get('std', float('nan')):.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
