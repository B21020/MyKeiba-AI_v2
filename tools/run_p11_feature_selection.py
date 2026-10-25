"""P11: 特徴量選択 (Feature Selection).

LightGBM の feature importance (gain) と permutation importance を用い、
342 列のオリジナル特徴量からノイズを除去して段階的に HPO + Bootstrap 評価を行う。

**元データは一切変更しない。** 選択した特徴量リストを JSON で保存し、
run_p9_fukusho_hpo.py --selected-features で読み込む形とする。

Phase 1: 重要度分析 (gain + permutation)
Phase 2: 段階的 HPO (top-200 / top-100 / top-50)
Phase 3: 結果比較

Usage:
  # Analysis only (no HPO)
  .venv/Scripts/python.exe tools/run_p11_feature_selection.py --skip-hpo

  # Full run (analysis + HPO for each subset)
  .venv/Scripts/python.exe tools/run_p11_feature_selection.py

  # Custom subsets
  .venv/Scripts/python.exe tools/run_p11_feature_selection.py --top-n 200 100 50 30
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import subprocess
import sys
import time
from pathlib import Path

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import lightgbm as lgb
from sklearn.metrics import roc_auc_score
from sklearn.inspection import permutation_importance

from modules.constants import ResultsCols

FEATURED_REL_EXT_PATH = "data/tmp/featured_data_20260205_rel_ext.pickle"

# System columns excluded from feature importance (not model features)
DROP_COLS = {'rank', 'win', 'longshot', 'date',
             ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id', 'horse_id'}


# ---------------------------------------------------------------------------
# Phase 1: Feature Importance Analysis
# ---------------------------------------------------------------------------

def compute_importance(
    featured_data: pd.DataFrame,
    test_start: str = "2024-07-01",
    valid_ratio: float = 0.2,
    n_perm_repeats: int = 5,
    seed: int = 42,
) -> pd.DataFrame:
    """Compute gain importance + permutation importance.

    Returns DataFrame with columns: feature, gain_importance, gain_rank,
    perm_importance_mean, perm_importance_std, perm_rank, avg_rank
    """
    dates = pd.to_datetime(featured_data["date"])

    # Train/test split
    train_mask = dates < pd.Timestamp(test_start)
    test_mask = dates >= pd.Timestamp(test_start)
    train_df = featured_data[train_mask]
    test_df = featured_data[test_mask]

    feature_cols = [c for c in featured_data.columns if c not in DROP_COLS]

    X_train = train_df[feature_cols]
    y_train = train_df["rank"]
    X_test = test_df[feature_cols]
    y_test = test_df["rank"]

    # Further split train into train_sub / valid for early stopping
    train_dates = pd.to_datetime(train_df["date"])
    cutoff = train_dates.quantile(1.0 - valid_ratio)
    train_sub_mask = train_dates < cutoff
    valid_mask = train_dates >= cutoff

    X_train_sub = X_train[train_sub_mask]
    y_train_sub = y_train[train_sub_mask]
    X_valid = X_train[valid_mask]
    y_valid = y_train[valid_mask]

    cat_cols = X_train_sub.select_dtypes(include="category").columns.tolist()

    print(f"  Train: {len(X_train_sub):,}  Valid: {len(X_valid):,}  Test: {len(X_test):,}")
    print(f"  Features: {len(feature_cols)}  Cat features: {len(cat_cols)}")

    # Train default LightGBM
    model = lgb.LGBMClassifier(
        objective="binary", verbosity=-1, n_estimators=500,
        num_leaves=63, max_depth=6, learning_rate=0.05,
        min_child_samples=50, subsample=0.8, colsample_bytree=0.8,
        reg_alpha=0.01, reg_lambda=0.1, random_state=seed,
    )
    model.fit(
        X_train_sub, y_train_sub,
        eval_set=[(X_valid, y_valid)],
        eval_metric="auc",
        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
    )

    proba_test = model.predict_proba(X_test)[:, 1]
    auc_test = roc_auc_score(y_test, proba_test)
    proba_train = model.predict_proba(X_train_sub)[:, 1]
    auc_train = roc_auc_score(y_train_sub, proba_train)
    print(f"  AUC: {auc_train:.4f} (train), {auc_test:.4f} (test)")
    print(f"  Best iteration: {model.best_iteration_}")

    # 1) Gain importance
    gain_imp = model.booster_.feature_importance(importance_type="gain")
    gain_df = pd.DataFrame({
        "feature": feature_cols,
        "gain_importance": gain_imp,
    })
    gain_df = gain_df.sort_values("gain_importance", ascending=False).reset_index(drop=True)
    gain_df["gain_rank"] = range(1, len(gain_df) + 1)

    # 2) Permutation importance (on test data for honest evaluation)
    print(f"\n  Computing permutation importance ({n_perm_repeats} repeats)...")
    t0 = time.time()
    perm_result = permutation_importance(
        model, X_test, y_test,
        n_repeats=n_perm_repeats,
        scoring="roc_auc",
        random_state=seed,
        n_jobs=-1,
    )
    print(f"  Permutation importance done in {time.time()-t0:.1f}s")

    perm_df = pd.DataFrame({
        "feature": feature_cols,
        "perm_importance_mean": perm_result.importances_mean,
        "perm_importance_std": perm_result.importances_std,
    })
    perm_df = perm_df.sort_values("perm_importance_mean", ascending=False).reset_index(drop=True)
    perm_df["perm_rank"] = range(1, len(perm_df) + 1)

    # Merge
    imp_df = gain_df.merge(perm_df, on="feature")
    imp_df["avg_rank"] = (imp_df["gain_rank"] + imp_df["perm_rank"]) / 2.0
    imp_df = imp_df.sort_values("avg_rank").reset_index(drop=True)

    return imp_df, auc_test, model


def save_feature_lists(
    imp_df: pd.DataFrame,
    top_n_list: list[int],
    out_dir: str,
) -> dict[int, list[str]]:
    """Save top-N feature lists as JSON files. Returns dict of feature lists."""
    os.makedirs(out_dir, exist_ok=True)
    results = {}
    for n in top_n_list:
        top_features = imp_df.head(n)["feature"].tolist()
        path = os.path.join(out_dir, f"selected_top{n}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(top_features, f, ensure_ascii=False, indent=2)
        results[n] = top_features
        print(f"  Saved top-{n} features → {path}")
    return results


# ---------------------------------------------------------------------------
# Phase 2: Run HPO for each subset
# ---------------------------------------------------------------------------

def run_hpo_for_subset(
    n: int,
    json_path: str,
    featured_path: str,
    out_dir_base: str,
    n_trials: int = 100,
    n_boot: int = 2000,
    n_sweep_test: int = 200,
) -> str:
    """Run run_p9_fukusho_hpo.py with --selected-features for a feature subset."""
    sub_out_dir = os.path.join(out_dir_base, f"top{n}")
    os.makedirs(sub_out_dir, exist_ok=True)

    python_exe = sys.executable
    cmd = [
        python_exe, "tools/run_p9_fukusho_hpo.py",
        "--featured", featured_path,
        "--selected-features", json_path,
        "--n-trials", str(n_trials),
        "--n-boot", str(n_boot),
        "--n-sweep-test", str(n_sweep_test),
        "--out-dir", sub_out_dir,
    ]

    print(f"\n{'='*60}")
    print(f"HPO: top-{n} features ({n} cols)")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'='*60}")

    result = subprocess.run(
        cmd, cwd=str(REPO_ROOT),
        capture_output=False, text=True,
    )
    return sub_out_dir


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="P11: 特徴量選択 (Feature Selection)")
    ap.add_argument("--featured", default=None,
                    help="featured_data path (default: rel_ext)")
    ap.add_argument("--top-n", nargs="+", type=int, default=[200, 100, 50],
                    help="Top-N feature subsets to evaluate (default: 200 100 50)")
    ap.add_argument("--n-trials", type=int, default=100,
                    help="Optuna trials per subset (default: 100)")
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--n-perm-repeats", type=int, default=5,
                    help="Permutation importance repeats (default: 5)")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--skip-hpo", action="store_true",
                    help="Only run importance analysis, skip HPO")
    ap.add_argument("--skip-analysis", action="store_true",
                    help="Skip importance analysis, only run HPO (requires existing JSONs)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    t_total = time.time()

    # Determine paths
    if args.featured:
        featured_path = args.featured
    elif os.path.isfile(FEATURED_REL_EXT_PATH):
        featured_path = FEATURED_REL_EXT_PATH
    else:
        print("ERROR: rel_ext pickle not found and no --featured specified.")
        return 1

    import datetime
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd, "p11")
    os.makedirs(out_dir, exist_ok=True)

    print(f"\n{'#'*70}")
    print(f"# P11: 特徴量選択 (Feature Selection)")
    print(f"# featured: {featured_path}")
    print(f"# top-N subsets: {args.top_n}")
    print(f"# out_dir: {out_dir}")
    print(f"{'#'*70}")

    # ================================================================
    # Phase 1: Importance Analysis
    # ================================================================
    if not args.skip_analysis:
        print(f"\n{'='*60}")
        print(f"Phase 1: Feature Importance Analysis")
        print(f"{'='*60}")

        print(f"\nLoading featured_data: {featured_path}")
        featured_data = pd.read_pickle(featured_path)
        print(f"  shape: {featured_data.shape}")

        imp_df, baseline_auc, model = compute_importance(
            featured_data,
            n_perm_repeats=args.n_perm_repeats,
            seed=args.seed,
        )

        # Save full importance table
        imp_csv_path = os.path.join(out_dir, "feature_importance.csv")
        imp_df.to_csv(imp_csv_path, index=False, encoding="utf-8-sig")
        print(f"\n  Full importance table saved → {imp_csv_path}")
        print(f"  Baseline AUC (test, all {len(imp_df)} features): {baseline_auc:.4f}")

        # Print top-30
        print(f"\n  Top-30 features (by avg_rank):")
        print(f"  {'Rank':<5} {'Feature':<45} {'Gain':>10} {'GainR':>6} {'PermMean':>10} {'PermR':>6} {'AvgR':>6}")
        print(f"  {'-'*90}")
        for _, row in imp_df.head(30).iterrows():
            print(f"  {int(row['avg_rank']*2)/2:<5.1f} {row['feature']:<45} "
                  f"{row['gain_importance']:>10.1f} {int(row['gain_rank']):>6} "
                  f"{row['perm_importance_mean']:>10.6f} {int(row['perm_rank']):>6} "
                  f"{row['avg_rank']:>6.1f}")

        # Print bottom-30 (noise candidates)
        print(f"\n  Bottom-30 features (noise candidates):")
        for _, row in imp_df.tail(30).iterrows():
            gain_val = row['gain_importance']
            perm_val = row['perm_importance_mean']
            print(f"  R={row['avg_rank']:<6.1f} {row['feature']:<45} "
                  f"gain={gain_val:>8.1f}  perm={perm_val:>10.6f}")

        # Count zero-importance features
        zero_gain = (imp_df["gain_importance"] == 0).sum()
        neg_perm = (imp_df["perm_importance_mean"] <= 0).sum()
        print(f"\n  Zero gain importance: {zero_gain} features")
        print(f"  Non-positive perm importance: {neg_perm} features")

        # Save feature lists for each top-N
        print(f"\n  Saving feature lists...")
        feature_lists = save_feature_lists(imp_df, args.top_n, out_dir)

        # Also save a "positive_perm" list (features with perm_importance > 0)
        positive_perm_features = imp_df[imp_df["perm_importance_mean"] > 0]["feature"].tolist()
        pos_path = os.path.join(out_dir, "selected_positive_perm.json")
        with open(pos_path, "w", encoding="utf-8") as f:
            json.dump(positive_perm_features, f, ensure_ascii=False, indent=2)
        print(f"  Saved positive_perm features ({len(positive_perm_features)} cols) → {pos_path}")

        del featured_data  # free memory
    else:
        print("\n  Skipping importance analysis (--skip-analysis)")

    # ================================================================
    # Phase 2: HPO for each subset
    # ================================================================
    if not args.skip_hpo:
        print(f"\n{'='*60}")
        print(f"Phase 2: HPO for each feature subset")
        print(f"{'='*60}")

        for n in sorted(args.top_n, reverse=True):
            json_path = os.path.join(out_dir, f"selected_top{n}.json")
            if not os.path.isfile(json_path):
                print(f"  WARNING: {json_path} not found, skipping top-{n}")
                continue
            run_hpo_for_subset(
                n=n,
                json_path=json_path,
                featured_path=featured_path,
                out_dir_base=out_dir,
                n_trials=args.n_trials,
                n_boot=args.n_boot,
            )
    else:
        print(f"\n  --skip-hpo: Skipping HPO runs.")

    # ================================================================
    # Summary
    # ================================================================
    elapsed = time.time() - t_total
    print(f"\n{'='*60}")
    print(f"P11 Complete: {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"  Output: {out_dir}")
    print(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
