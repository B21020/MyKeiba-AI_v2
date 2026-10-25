"""P14: 異なるテスト期間での検証。

P11-ext best params + pos-perm 145 cols を固定し、
test_start を複数変更して同一モデル・パラメータのエッジが
特定テスト期間に依存するかを検証する。

各テスト期間で:
  1. モデル学習 (固定 params)
  2. 複勝 threshold sweep (none/platt/isotonic × 200 thresholds)
  3. Bootstrap CI (best threshold)
  4. 月別・場別ブレイクダウン

Usage:
    python tools/run_p14_test_periods.py \
        --params models/20260212/p11_ext/optuna_best_params.json \
        --selected-features models/20260210/p11/selected_positive_perm.json \
        --out-dir models/20260213/p14
"""
from __future__ import annotations

import argparse
import json
import os
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

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._calibration import fit_calibrator
from modules.training._data_splitter import DataSplitter


# ===========================================================================
# Constants
# ===========================================================================
FEATURED_REL_EXT_PATH = "data/tmp/featured_data_20260205_rel_ext.pickle"
FEATURED_BASELINE_PATH = "data/tmp/featured_data_20260205_baseline.pickle"

# テスト期間候補 (test_start, label)
DEFAULT_TEST_PERIODS = [
    ("2023-01-01", "2023-01"),
    ("2023-07-01", "2023-07"),
    ("2024-01-01", "2024-01"),
    ("2024-07-01", "2024-07"),  # P11-ext と同一
    ("2025-01-01", "2025-01"),
]


# ===========================================================================
# Helpers
# ===========================================================================

def _linspace_inclusive(low: float, high: float, n: int) -> list[float]:
    return list(np.linspace(low, high, n))


def _filter_by_date(df, start, end):
    dates = pd.to_datetime(df["date"])
    mask = dates >= pd.Timestamp(start)
    if end:
        mask &= dates <= pd.Timestamp(end)
    return df[mask]


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
    """レース内 z-score 標準化。"""
    df = score_table.copy()
    scores = pd.to_numeric(df["score"], errors="coerce").astype(float)
    if "race_id" in df.columns:
        g = scores.groupby(df["race_id"])
    else:
        g = scores.groupby(level=0)
    mean = g.transform("mean")
    std = g.transform("std")
    std = std.replace(0, 1)
    df["score"] = (scores - mean) / std
    return df


def _convert_to_sklearn_params(params: dict) -> dict:
    """raw Optuna params → LGBMClassifier kwargs."""
    out = {
        "objective": "binary",
        "metric": "auc",
        "verbosity": -1,
        "boosting_type": "gbdt",
    }
    for k in [
        "num_leaves", "max_depth", "learning_rate", "n_estimators",
        "min_child_samples", "subsample", "subsample_freq",
        "colsample_bytree", "reg_alpha", "reg_lambda", "min_split_gain",
    ]:
        if k in params:
            out[k] = params[k]
    return out


# ===========================================================================
# Fukusho sweep
# ===========================================================================

def fukusho_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    max_bets_per_race: int = 3,
) -> list[dict]:
    """複勝 threshold sweep。"""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(th),
            max_bets_per_race=max_bets_per_race,
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        results.append(ret)
    return results


# ===========================================================================
# Bootstrap CI
# ===========================================================================

def bootstrap_ci(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    n_boot: int,
    seed: int = 42,
    max_bets_per_race: int = 3,
) -> dict:
    """レース単位 Bootstrap で 複勝 RR の CI を推定。"""
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return {
            "mean": 0, "ci_lower": 0, "ci_upper": 0, "p_value_lt_1": 1.0,
            "n_races": 0, "n_bets": 0, "n_hits": 0, "point_rr": 0.0,
        }

    race_ids = per_race.index.values
    n_races = len(race_ids)

    rng = np.random.RandomState(seed)
    rr_dist = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.choice(n_races, size=n_races, replace=True)
        sampled = per_race.iloc[idx]
        total_bet = sampled["bet_amount"].sum()
        total_ret = sampled["return_amount"].sum()
        rr_dist[b] = total_ret / total_bet if total_bet > 0 else 0.0

    ci_lower = float(np.percentile(rr_dist, 2.5))
    ci_upper = float(np.percentile(rr_dist, 97.5))
    p_lt_1 = float(np.mean(rr_dist < 1.0))

    return {
        "mean": float(np.mean(rr_dist)),
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "p_value_lt_1": p_lt_1,
        "rr_distribution": rr_dist,
        "n_races": n_races,
        "n_bets": int(per_race["n_bets"].sum()),
        "n_hits": int(per_race["hit_or_not"].sum()),
        "point_rr": float(per_race["return_amount"].sum() / per_race["bet_amount"].sum()),
    }


# ===========================================================================
# Monthly / Venue breakdown
# ===========================================================================

def monthly_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    test_data: pd.DataFrame,
    max_bets_per_race: int = 3,
) -> pd.DataFrame:
    """月別の成績。"""
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    if "race_id" in test_data.columns:
        date_map = test_data.groupby("race_id")["date"].first()
    else:
        date_map = test_data.groupby(level=0)["date"].first()

    per_race["month"] = pd.to_datetime(
        date_map.reindex(per_race.index)
    ).dt.to_period("M").astype(str)

    month_df = per_race.groupby("month").agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    month_df["return_rate"] = month_df["total_return"] / month_df["total_bet"]
    month_df["profit"] = month_df["total_return"] - month_df["total_bet"]
    return month_df


def venue_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    test_data: pd.DataFrame,
    max_bets_per_race: int = 3,
) -> pd.DataFrame:
    """場別の成績。"""
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    if "race_id" in test_data.columns:
        race_ids = test_data.groupby("race_id").first().index
    else:
        race_ids = test_data.index.get_level_values(0).unique()

    venue_map = pd.Series(race_ids, index=race_ids).apply(lambda x: str(x)[4:6])
    per_race["venue_code"] = venue_map.reindex(per_race.index)

    VENUE_NAMES = {
        "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
        "05": "東京", "06": "中山", "07": "中京", "08": "京都",
        "09": "阪神", "10": "小倉",
    }
    per_race["venue_name"] = per_race["venue_code"].map(VENUE_NAMES)

    venue_df = per_race.groupby(["venue_code", "venue_name"]).agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    venue_df["return_rate"] = venue_df["total_return"] / venue_df["total_bet"]
    venue_df["profit"] = venue_df["total_return"] - venue_df["total_bet"]
    return venue_df


# ===========================================================================
# Evaluate one test period
# ===========================================================================

def evaluate_period(
    featured_data: pd.DataFrame,
    best_params: dict,
    test_start: str,
    test_end: str | None,
    sim: simulation.Simulator,
    n_sweep: int,
    min_bets: int,
    n_boot: int,
    valid_ratio: float,
    seed: int,
    out_dir: str,
    label: str,
) -> dict:
    """1つのテスト期間を評価。学習→sweep→bootstrap→breakdown。"""
    t0 = time.time()
    period_dir = os.path.join(out_dir, label.replace("-", ""))
    os.makedirs(period_dir, exist_ok=True)

    dates = pd.to_datetime(featured_data["date"])
    data_min_date = dates.min().strftime("%Y-%m-%d")

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, data_min_date, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    if len(test_df) == 0:
        print(f"  [SKIP] No test data for test_start={test_start}")
        return {"label": label, "test_start": test_start, "skip": True}

    print(f"  train: {len(train_df):,} rows, test: {len(test_df):,} rows")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    # --- Train ---
    ds = DataSplitter(combined.copy(), test_size=test_ratio, valid_size=valid_ratio)
    keiba_ai = training.KeibaAI(ds)
    keiba_ai.set_params(_convert_to_sklearn_params(best_params))
    keiba_ai.train_without_tuning()

    # --- AUC ---
    X_test = ds.X_test
    y_test = ds.y_test
    raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)
    try:
        auc_train = roc_auc_score(
            ds.y_train,
            keiba_ai.calc_score(ds.X_train, policies.BasicScorePolicy)["score"].astype(float),
        )
    except Exception:
        auc_train = float("nan")
    try:
        auc_test = roc_auc_score(y_test, raw_scores)
    except Exception:
        auc_test = float("nan")

    print(f"  AUC: {auc_train:.3f}(train), {auc_test:.3f}(test), gap={auc_train-auc_test:.3f}")

    # --- Calibrations ---
    calibrations = {"none": raw_scores.copy()}
    for cal_name in ["platt", "isotonic"]:
        try:
            X_valid = ds.X_valid
            y_valid = ds.y_valid
            valid_score = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
            valid_raw = pd.to_numeric(valid_score["score"], errors="coerce").astype(float)
            calibrator = fit_calibrator(cal_name, y_valid.values, valid_raw.values)
            calibrations[cal_name] = pd.Series(
                calibrator(raw_scores.values), index=raw_scores.index,
            )
        except Exception as e:
            print(f"  calibration {cal_name} failed: {e}")

    test_data = ds.test_data

    # --- Sweep ---
    thresholds = _linspace_inclusive(0.0, 4.0, n_sweep)

    overall_best_rr = -1
    overall_best = None

    for cal_name, cal_scores in calibrations.items():
        cal_score_table = raw_score_table.copy()
        cal_score_table["score"] = cal_scores.values
        score_std = _standardize_within_race(cal_score_table)

        fk_results = fukusho_sweep(score_std, sim, thresholds, max_bets_per_race=3)
        fk_df = pd.DataFrame(fk_results)

        valid_fk = fk_df[fk_df["n_bets"] >= min_bets]
        if len(valid_fk) > 0:
            best_idx = valid_fk["return_rate"].idxmax()
            r = valid_fk.loc[best_idx]
            rr = r["return_rate"]
            th = r["threshold"]
            n = r["n_bets"]
            print(f"  [{cal_name}] th={th:.3f}  RR={rr:.4f}  n={n:.0f}")

            if rr > overall_best_rr:
                overall_best_rr = rr
                overall_best = {
                    "cal": cal_name, "threshold": th, "rr": rr,
                    "n_bets": n, "score_std": score_std,
                }
        else:
            print(f"  [{cal_name}] no valid threshold")

        fk_df.to_csv(os.path.join(period_dir, f"fukusho_sweep_{cal_name}.csv"), index=False)

    if overall_best is None:
        print(f"  [WARN] No valid threshold for period {label}")
        return {
            "label": label, "test_start": test_start, "skip": False,
            "n_train": len(train_df), "n_test": len(test_df),
            "auc_train": auc_train, "auc_test": auc_test,
            "best_cal": None, "best_th": None, "best_rr": 0,
            "n_bets": 0, "ci_lower": 0, "ci_upper": 0, "p_rr_lt_1": 1.0,
        }

    best_cal = overall_best["cal"]
    best_th = overall_best["threshold"]
    best_rr = overall_best["rr"]
    best_n = overall_best["n_bets"]
    best_score_std = overall_best["score_std"]

    # --- Bootstrap CI ---
    boot = bootstrap_ci(
        best_score_std, sim, threshold=best_th,
        n_boot=n_boot, seed=seed, max_bets_per_race=3,
    )

    hit_rate = boot["n_hits"] / boot["n_races"] * 100 if boot["n_races"] > 0 else 0
    print(f"  Best: [{best_cal}] th={best_th:.3f}  RR={best_rr:.4f}  n={best_n:.0f}")
    print(f"  CI: [{boot['ci_lower']:.4f}, {boot['ci_upper']:.4f}]  P(RR<1)={boot['p_value_lt_1']:.4f}")
    print(f"  Hits: {boot['n_hits']}/{boot['n_races']} ({hit_rate:.1f}%)")

    # Save bootstrap
    ci_row = {
        "label": label, "test_start": test_start,
        "calibration": best_cal, "threshold": best_th,
        "point_rr": boot["point_rr"], "n_bets": boot["n_bets"],
        "n_races": boot["n_races"], "n_hits": boot["n_hits"],
        "ci_lower": boot["ci_lower"], "ci_upper": boot["ci_upper"],
        "p_rr_lt_1": boot["p_value_lt_1"], "boot_mean": boot["mean"],
    }
    pd.DataFrame([ci_row]).to_csv(os.path.join(period_dir, "bootstrap_ci.csv"), index=False)

    # --- Monthly breakdown ---
    month_df = monthly_breakdown(best_score_std, sim, best_th, test_data)
    if not month_df.empty:
        month_df.to_csv(os.path.join(period_dir, "monthly.csv"))
        profit_months = (month_df["profit"] > 0).sum()
        total_months = len(month_df)
        print(f"  Profit months: {profit_months}/{total_months}")

    # --- Venue breakdown ---
    venue_df = venue_breakdown(best_score_std, sim, best_th, test_data)
    if not venue_df.empty:
        venue_df.to_csv(os.path.join(period_dir, "venue.csv"))

    elapsed = time.time() - t0
    print(f"  Time: {elapsed:.1f}s")

    return {
        "label": label, "test_start": test_start, "skip": False,
        "n_train": len(train_df), "n_test": len(test_df),
        "auc_train": auc_train, "auc_test": auc_test,
        "auc_gap": auc_train - auc_test,
        "best_cal": best_cal, "best_th": best_th, "best_rr": best_rr,
        "n_bets": int(best_n),
        "n_races": boot["n_races"], "n_hits": boot["n_hits"],
        "ci_lower": boot["ci_lower"], "ci_upper": boot["ci_upper"],
        "p_rr_lt_1": boot["p_value_lt_1"],
        "boot_mean": boot["mean"],
        "profit_months": f"{profit_months}/{total_months}" if not month_df.empty else "N/A",
        "elapsed": elapsed,
    }


# ===========================================================================
# Main
# ===========================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="P14: 異なるテスト期間での検証")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--params", required=True,
                    help="JSON file with model params")
    ap.add_argument("--selected-features", default=None,
                    help="JSON file with selected feature column names")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-sweep", type=int, default=200,
                    help="Number of threshold points")
    ap.add_argument("--min-bets", type=int, default=10,
                    help="Minimum n_bets for valid threshold")
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    t_total = time.time()

    # Determine featured data path
    if args.featured:
        featured_path = args.featured
    elif os.path.isfile(FEATURED_REL_EXT_PATH):
        featured_path = FEATURED_REL_EXT_PATH
        print(f"Using rel_ext features: {featured_path}")
    elif os.path.isfile(FEATURED_BASELINE_PATH):
        featured_path = FEATURED_BASELINE_PATH
        print(f"Using baseline features: {featured_path}")
    else:
        print("ERROR: No featured_data found!", file=sys.stderr)
        return 1

    out_dir = args.out_dir or f"models/{pd.Timestamp.now().strftime('%Y%m%d')}/p14"
    os.makedirs(out_dir, exist_ok=True)

    with open(args.params, "r", encoding="utf-8") as f:
        best_params = json.load(f)

    print(f"\n{'#'*70}")
    print(f"# P14: 異なるテスト期間での検証")
    print(f"# featured: {featured_path}")
    print(f"# params: {args.params}")
    print(f"# selected_features: {args.selected_features or 'None'}")
    print(f"# out_dir: {out_dir}")
    print(f"# test_periods: {[p[1] for p in DEFAULT_TEST_PERIODS]}")
    print(f"{'#'*70}")

    # ================================================================
    # Load data
    # ================================================================
    print(f"\nLoading featured_data: {featured_path}")
    featured_data = pd.read_pickle(featured_path)
    print(f"  shape (raw): {featured_data.shape}")

    if args.selected_features:
        with open(args.selected_features, "r", encoding="utf-8") as f:
            sel_cols = json.load(f)
        keep_always = {'rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS,
                       ResultsCols.UMABAN, 'race_id', 'horse_id'}
        keep = list(keep_always | set(sel_cols))
        keep = [c for c in keep if c in featured_data.columns]
        featured_data = featured_data[keep]
        n_feat = len([c for c in sel_cols if c in featured_data.columns])
        print(f"  Selected features: {n_feat} cols")

    print(f"  shape: {featured_data.shape}")
    dates = pd.to_datetime(featured_data["date"])
    print(f"  date range: {dates.min().strftime('%Y-%m-%d')} ~ {dates.max().strftime('%Y-%m-%d')}")

    # Simulator
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # ================================================================
    # Evaluate each test period
    # ================================================================
    all_results = []

    for test_start, label in DEFAULT_TEST_PERIODS:
        print(f"\n{'='*60}")
        print(f"Period: {label}  (test_start={test_start})")
        print(f"{'='*60}")

        result = evaluate_period(
            featured_data=featured_data,
            best_params=best_params,
            test_start=test_start,
            test_end=args.test_end,
            sim=sim,
            n_sweep=args.n_sweep,
            min_bets=args.min_bets,
            n_boot=args.n_boot,
            valid_ratio=args.valid_ratio,
            seed=args.seed,
            out_dir=out_dir,
            label=label,
        )
        all_results.append(result)

    # ================================================================
    # Summary
    # ================================================================
    elapsed = time.time() - t_total

    print(f"\n{'='*70}")
    print(f"P14 FINAL SUMMARY — 異なるテスト期間比較")
    print(f"{'='*70}")
    print(f"  Model params: {args.params}")
    print(f"  Features: {args.selected_features or 'all'}")
    print()

    # Header
    print(f"  {'Period':<10} {'n_train':>10} {'n_test':>10} {'AUC_test':>9} {'gap':>6} "
          f"{'Cal':<10} {'Th':>6} {'RR':>8} {'n_bets':>7} "
          f"{'CI_low':>8} {'CI_high':>8} {'P(RR<1)':>9} {'Prof_mo':>8}")
    print(f"  {'-'*115}")

    for r in all_results:
        if r.get("skip"):
            print(f"  {r['label']:<10} [SKIPPED]")
            continue
        if r.get("best_cal") is None:
            print(f"  {r['label']:<10} {r['n_train']:>10,} {r['n_test']:>10,} "
                  f"{r['auc_test']:>9.3f} {r['auc_gap']:>6.3f} --- no valid threshold ---")
            continue

        marker = " ★" if r["label"] == "2024-07" else ""
        print(f"  {r['label']:<10} {r['n_train']:>10,} {r['n_test']:>10,} "
              f"{r['auc_test']:>9.3f} {r['auc_gap']:>6.3f} "
              f"{r['best_cal']:<10} {r['best_th']:>6.3f} {r['best_rr']:>8.4f} {r['n_bets']:>7} "
              f"{r['ci_lower']:>8.4f} {r['ci_upper']:>8.4f} {r['p_rr_lt_1']:>9.4f} "
              f"{r['profit_months']:>8}{marker}")

    # Save consolidated results
    summary_rows = [r for r in all_results if not r.get("skip")]
    if summary_rows:
        # Remove non-serializable fields
        for row in summary_rows:
            row.pop("skip", None)
        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(os.path.join(out_dir, "period_comparison.csv"), index=False)

    # Analysis
    print()
    rr_values = [r["best_rr"] for r in all_results if not r.get("skip") and r.get("best_rr", 0) > 0]
    if rr_values:
        rr_above_1 = sum(1 for rr in rr_values if rr > 1.0)
        print(f"  Periods with RR>1.0: {rr_above_1}/{len(rr_values)}")

        p_values = [r["p_rr_lt_1"] for r in all_results if not r.get("skip") and r.get("best_cal")]
        sig_count = sum(1 for p in p_values if p < 0.05)
        print(f"  Statistically significant (P<0.05): {sig_count}/{len(p_values)}")

        # 2024-07 reference
        ref = next((r for r in all_results if r["label"] == "2024-07"), None)
        if ref and ref.get("best_rr"):
            print(f"\n  Reference period (2024-07): RR={ref['best_rr']:.4f}, P(RR<1)={ref['p_rr_lt_1']:.4f}")

        # Check if edge is period-dependent
        if rr_above_1 >= 3:
            print(f"  → エッジは複数期間で確認 — 期間依存性は低い")
        elif rr_above_1 <= 1:
            print(f"  → エッジは特定期間に限定 — 期間依存性が高い（過適合の可能性）")
        else:
            print(f"  → エッジは一部の期間で確認 — 部分的な期間依存性")

    print(f"\n  Total time: {elapsed:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
