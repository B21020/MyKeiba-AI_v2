"""P9-5: ワイド BOX threshold sweep + Bootstrap CI.

P11-ext best params でモデルを学習し、テスト期間で
ワイド BOX の z-score threshold sweep + Bootstrap CI を実行する。

Usage:
    python tools/run_p9_5_wide_sweep.py \
        --params models/20260212/p11_ext/optuna_best_params.json \
        --selected-features models/20260210/p11/selected_positive_perm.json \
        --out-dir models/20260213/p9_5
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


def _build_score_table(keiba_ai, X, score_policy):
    """KeibaAI.calc_score wrapper."""
    return keiba_ai.calc_score(X, score_policy)


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
# Wide BOX sweep
# ===========================================================================

def wide_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    min_bets: int = 10,
) -> list[dict]:
    """ワイド BOX threshold sweep → results list."""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyWideBox.judge(
            score_table_std, threshold=float(th),
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        results.append(ret)
    return results


def fukusho_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    max_bets_per_race: int = 3,
    min_bets: int = 10,
) -> list[dict]:
    """複勝 threshold sweep (comparison baseline)."""
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
# Bootstrap CI (wide)
# ===========================================================================

def bootstrap_wide_ci(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    n_boot: int,
    seed: int = 42,
) -> dict:
    """レース単位 Bootstrap で ワイド BOX RR の CI を推定。"""
    actions = policies.BetPolicyWideBox.judge(
        score_table_std, threshold=float(threshold),
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
# Monthly + Venue breakdown (generic)
# ===========================================================================

def monthly_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    test_data: pd.DataFrame,
    bet_type: str = "wide",
) -> pd.DataFrame:
    """月別の成績。bet_type: 'wide' or 'fukusho'."""
    if bet_type == "wide":
        actions = policies.BetPolicyWideBox.judge(
            score_table_std, threshold=float(threshold),
        )
    else:
        actions = policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(threshold),
            max_bets_per_race=3,
        )

    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    # Attach month
    if "race_id" in test_data.columns:
        date_map = test_data.groupby("race_id")["date"].first()
    else:
        date_map = test_data.groupby(level=0)["date"].first()

    per_race["month"] = pd.to_datetime(date_map.reindex(per_race.index)).dt.to_period("M").astype(str)

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
    bet_type: str = "wide",
) -> pd.DataFrame:
    """場別の成績。"""
    if bet_type == "wide":
        actions = policies.BetPolicyWideBox.judge(
            score_table_std, threshold=float(threshold),
        )
    else:
        actions = policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(threshold),
            max_bets_per_race=3,
        )

    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    # venue_code from race_id
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
# Main
# ===========================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="P9-5: ワイド BOX threshold sweep")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--params", required=True,
                    help="JSON file with model params (e.g., P11-ext optuna_best_params.json)")
    ap.add_argument("--selected-features", default=None,
                    help="JSON file with selected feature column names")
    ap.add_argument("--test-start", default="2024-07-01")
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

    # Output dir
    out_dir = args.out_dir or f"models/{pd.Timestamp.now().strftime('%Y%m%d')}/p9_5"
    os.makedirs(out_dir, exist_ok=True)

    # Load params
    with open(args.params, "r", encoding="utf-8") as f:
        best_params = json.load(f)

    print(f"\n{'#'*70}")
    print(f"# P9-5: ワイド BOX threshold sweep")
    print(f"# featured: {featured_path}")
    print(f"# params: {args.params}")
    print(f"# selected_features: {args.selected_features or 'None'}")
    print(f"# out_dir: {out_dir}")
    print(f"{'#'*70}")

    # ================================================================
    # Load data
    # ================================================================
    print(f"\nLoading featured_data: {featured_path}")
    featured_data = pd.read_pickle(featured_path)
    print(f"  shape (raw): {featured_data.shape}")

    # Selected features filtering
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

    # Train/test split
    test_start = args.test_start
    test_end = args.test_end
    data_min_date = dates.min().strftime("%Y-%m-%d")

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, data_min_date, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]
    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    # Simulator
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # ================================================================
    # Phase 1: Train model with given params
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 1: Train model with P11-ext params")
    print(f"{'='*60}")

    ds = DataSplitter(combined.copy(), test_size=test_ratio, valid_size=args.valid_ratio)
    keiba_ai = training.KeibaAI(ds)
    keiba_ai.set_params(_convert_to_sklearn_params(best_params))
    keiba_ai.train_without_tuning()

    # AUC
    X_test = ds.X_test
    y_test = ds.y_test
    raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)
    try:
        auc_train = roc_auc_score(
            ds.y_train,
            keiba_ai.calc_score(ds.X_train, policies.BasicScorePolicy)["score"].astype(float)
        )
    except Exception:
        auc_train = float("nan")
    try:
        auc_test = roc_auc_score(y_test, raw_scores)
    except Exception:
        auc_test = float("nan")

    print(f"  AUC: {auc_train:.3f}(train), {auc_test:.3f}(test)")
    print(f"  AUC gap: {auc_train - auc_test:.3f}")
    print(f"  Model params: {json.dumps({k: v for k, v in best_params.items()}, indent=2)}")

    # Calibration
    calibrations = {"none": raw_scores.copy()}
    for cal_name in ["platt", "isotonic"]:
        try:
            X_valid = ds.X_valid
            y_valid = ds.y_valid
            valid_score = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
            valid_raw = pd.to_numeric(valid_score["score"], errors="coerce").astype(float)
            calibrator = fit_calibrator(cal_name, y_valid.values, valid_raw.values)
            calibrations[cal_name] = pd.Series(calibrator(raw_scores.values), index=raw_scores.index)
        except Exception as e:
            print(f"  calibration {cal_name} failed: {e}")

    test_data = ds.test_data

    # ================================================================
    # Phase 2: Wide BOX sweep + Fukusho sweep (comparison)
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 2: Threshold sweep (ワイド BOX + 複勝 comparison)")
    print(f"{'='*60}")

    thresholds = _linspace_inclusive(0.0, 4.0, args.n_sweep)

    overall_best_rr = -1
    overall_best = None

    for cal_name, cal_scores in calibrations.items():
        cal_score_table = raw_score_table.copy()
        cal_score_table["score"] = cal_scores.values
        score_std = _standardize_within_race(cal_score_table)

        # --- Wide BOX sweep ---
        wide_results = wide_sweep(score_std, sim, thresholds, min_bets=args.min_bets)
        wide_df = pd.DataFrame(wide_results)

        valid_wide = wide_df[wide_df["n_bets"] >= args.min_bets]
        if len(valid_wide) > 0:
            best_idx = valid_wide["return_rate"].idxmax()
            best_row = valid_wide.loc[best_idx]
            w_th = best_row["threshold"]
            w_rr = best_row["return_rate"]
            w_n = best_row["n_bets"]
            w_races = best_row.get("n_races", 0)
        else:
            w_th, w_rr, w_n, w_races = None, 0.0, 0, 0

        print(f"  [{cal_name}] wide_box:   th={w_th:.3f}  RR={w_rr:.4f}  n_bets={w_n:.0f}  n_races={w_races:.0f}" if w_th else f"  [{cal_name}] wide_box: no valid threshold")

        # --- Fukusho sweep (comparison) ---
        fk_results = fukusho_sweep(score_std, sim, thresholds, max_bets_per_race=3, min_bets=args.min_bets)
        fk_df = pd.DataFrame(fk_results)

        valid_fk = fk_df[fk_df["n_bets"] >= args.min_bets]
        if len(valid_fk) > 0:
            best_idx = valid_fk["return_rate"].idxmax()
            best_row = valid_fk.loc[best_idx]
            f_th = best_row["threshold"]
            f_rr = best_row["return_rate"]
            f_n = best_row["n_bets"]
        else:
            f_th, f_rr, f_n = None, 0.0, 0

        print(f"  [{cal_name}] fukusho:    th={f_th:.3f}  RR={f_rr:.4f}  n_bets={f_n:.0f}" if f_th else f"  [{cal_name}] fukusho: no valid threshold")

        # Save sweep data
        wide_df.to_csv(os.path.join(out_dir, f"wide_sweep_{cal_name}.csv"), index=False)
        fk_df.to_csv(os.path.join(out_dir, f"fukusho_sweep_{cal_name}.csv"), index=False)

        # Track overall best (wide)
        if w_rr > overall_best_rr and w_th is not None:
            overall_best_rr = w_rr
            overall_best = {
                "cal": cal_name, "threshold": w_th, "rr": w_rr,
                "n_bets": w_n, "n_races": w_races,
                "score_std": score_std,
            }

    if overall_best is None:
        print("\n  No valid wide threshold found. Aborting.")
        return 1

    best_cal = overall_best["cal"]
    best_th = overall_best["threshold"]
    best_rr = overall_best["rr"]
    best_n = overall_best["n_bets"]
    best_score_std = overall_best["score_std"]

    print(f"\n  Overall best (wide): [{best_cal}] th={best_th:.3f}  RR={best_rr:.4f}  n={best_n:.0f}")

    # ================================================================
    # Phase 3: Bootstrap CI (wide BOX)
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 3: Bootstrap CI ({args.n_boot} resamples, th={best_th:.3f})")
    print(f"{'='*60}")

    t0 = time.time()
    boot = bootstrap_wide_ci(
        best_score_std, sim, threshold=best_th,
        n_boot=args.n_boot, seed=args.seed,
    )
    boot_time = time.time() - t0

    print(f"  Point RR:   {boot['point_rr']:.4f}")
    print(f"  n_bets:     {boot['n_bets']}")
    print(f"  n_hits:     {boot['n_hits']}  (hit rate: {boot['n_hits']/boot['n_races']*100:.1f}%)" if boot["n_races"] > 0 else "")
    print(f"  95% CI:     [{boot['ci_lower']:.4f}, {boot['ci_upper']:.4f}]")
    print(f"  P(RR<1):    {boot['p_value_lt_1']:.4f}")
    print(f"  Boot mean:  {boot['mean']:.4f}")
    print(f"  Boot time:  {boot_time:.1f}s")

    # Save bootstrap
    ci_df = pd.DataFrame([{
        "threshold": best_th,
        "calibration": best_cal,
        "point_rr": boot["point_rr"],
        "n_bets": boot["n_bets"],
        "n_races": boot["n_races"],
        "n_hits": boot["n_hits"],
        "ci_lower": boot["ci_lower"],
        "ci_upper": boot["ci_upper"],
        "p_rr_lt_1": boot["p_value_lt_1"],
        "boot_mean": boot["mean"],
    }])
    ci_df.to_csv(os.path.join(out_dir, "bootstrap_ci.csv"), index=False)

    # Multi-threshold bootstrap
    print(f"\n  Multi-threshold bootstrap:")
    multi_ths = _linspace_inclusive(max(best_th - 0.5, 0.5), min(best_th + 0.5, 4.0), 7)
    for th in multi_ths:
        mb = bootstrap_wide_ci(best_score_std, sim, threshold=th, n_boot=args.n_boot, seed=args.seed)
        marker = "(*)" if mb["point_rr"] >= 1.0 else ""
        sig = "[OK]" if mb["p_value_lt_1"] < 0.05 else "[NG]"
        print(f"    th={th:.3f}: RR={mb['point_rr']:.4f} n={mb['n_bets']} "
              f"CI=[{mb['ci_lower']:.4f},{mb['ci_upper']:.4f}] "
              f"P(RR<1)={mb['p_value_lt_1']:.3f} hits={mb['n_hits']} {marker} {sig}")

    # ================================================================
    # Phase 4: Monthly + Venue breakdown
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 4: Monthly + Venue breakdown (wide, th={best_th:.3f})")
    print(f"{'='*60}")

    month_df = monthly_breakdown(best_score_std, sim, best_th, test_data, bet_type="wide")
    if not month_df.empty:
        print(f"\n  Monthly breakdown:")
        print(month_df.to_string(max_cols=10))
        profit_months = (month_df["profit"] > 0).sum()
        total_months = len(month_df)
        print(f"\n  Profit months: {profit_months}/{total_months}  "
              f"Loss months: {total_months - profit_months}/{total_months}")
        month_df.to_csv(os.path.join(out_dir, "monthly_breakdown.csv"))

    venue_df = venue_breakdown(best_score_std, sim, best_th, test_data, bet_type="wide")
    if not venue_df.empty:
        print(f"\n  Venue breakdown:")
        print(venue_df.to_string(max_cols=10))
        venue_df.to_csv(os.path.join(out_dir, "venue_breakdown.csv"))

    # ================================================================
    # Summary
    # ================================================================
    elapsed = time.time() - t_total
    print(f"\n{'='*70}")
    print(f"P9-5 SUMMARY")
    print(f"{'='*70}")
    print(f"  Model params: {args.params}")
    print(f"  Features: {args.selected_features or 'all'}")
    print(f"  AUC: {auc_train:.3f}(train), {auc_test:.3f}(test), gap={auc_train-auc_test:.3f}")
    print(f"")
    print(f"  --- Wide BOX best ({best_cal}) ---")
    print(f"  Threshold:   {best_th:.3f}")
    print(f"  RR:          {best_rr:.4f}")
    print(f"  n_bets:      {best_n:.0f}")
    print(f"  95% CI:      [{boot['ci_lower']:.4f}, {boot['ci_upper']:.4f}]")
    print(f"  P(RR<1):     {boot['p_value_lt_1']:.4f}")
    print(f"")
    significant = boot["p_value_lt_1"] < 0.05
    if significant:
        print(f"  [OK] CONCLUSION: P(RR<1)={boot['p_value_lt_1']:.4f} < 0.05 -> Significant!")
    else:
        print(f"  [NG] CONCLUSION: P(RR<1)={boot['p_value_lt_1']:.4f} >= 0.05 -> Not significant")
    print(f"")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
