"""P13: アンサンブルモデル — 複数 seed の予測平均でノイズ低減。

P11-ext best params + pos-perm 145 cols を固定し、
LightGBM の random_state を変えて N 個のモデルを学習。
予測確率を平均し、ノイズを低減してエッジの安定性を向上させる。

出力:
  1. 個別 seed ごとの sweep 結果 → 分散の把握
  2. アンサンブル (平均) の sweep + bootstrap CI
  3. 個別最良 vs アンサンブルの比較

Usage:
    python tools/run_p13_ensemble.py \
        --params models/20260212/p11_ext/optuna_best_params.json \
        --selected-features models/20260210/p11/selected_positive_perm.json \
        --out-dir models/20260213/p13 \
        --n-models 10 --n-boot 2000
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
DEFAULT_TEST_START = "2024-07-01"


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


def _convert_to_sklearn_params(params: dict, seed: int | None = None) -> dict:
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
    if seed is not None:
        out["random_state"] = seed
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
# Train single model and get raw predictions
# ===========================================================================

def train_single_model(
    ds: DataSplitter,
    best_params: dict,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    """1つの seed でモデルを学習し、test/valid/train の raw 予測を返す。

    Returns:
        (test_preds, valid_preds, train_preds, auc_train, auc_test)
    """
    keiba_ai = training.KeibaAI(ds)
    sklearn_params = _convert_to_sklearn_params(best_params, seed=seed)
    keiba_ai.set_params(sklearn_params)
    keiba_ai.train_without_tuning()

    # Test predictions
    raw_score_table = keiba_ai.calc_score(ds.X_test, policies.BasicScorePolicy)
    test_preds = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float).values

    # Valid predictions (for calibration)
    valid_score_table = keiba_ai.calc_score(ds.X_valid, policies.BasicScorePolicy)
    valid_preds = pd.to_numeric(valid_score_table["score"], errors="coerce").astype(float).values

    # Train predictions (for AUC)
    train_score_table = keiba_ai.calc_score(ds.X_train, policies.BasicScorePolicy)
    train_preds = pd.to_numeric(train_score_table["score"], errors="coerce").astype(float).values

    # AUC
    try:
        auc_train = roc_auc_score(ds.y_train, train_preds)
    except Exception:
        auc_train = float("nan")
    try:
        auc_test = roc_auc_score(ds.y_test, test_preds)
    except Exception:
        auc_test = float("nan")

    return test_preds, valid_preds, train_preds, auc_train, auc_test


# ===========================================================================
# Evaluate a set of predictions (single or ensemble)
# ===========================================================================

def evaluate_predictions(
    test_scores: np.ndarray,
    valid_scores: np.ndarray | None,
    ds: DataSplitter,
    sim: simulation.Simulator,
    thresholds: list[float],
    min_bets: int,
    n_boot: int,
    boot_seed: int,
    label: str,
    out_dir: str,
    do_breakdown: bool = True,
) -> dict:
    """予測スコアを受け取り、calibration sweep + bootstrap CI を実行。"""
    os.makedirs(out_dir, exist_ok=True)

    # Build score table (same structure as keiba_ai.calc_score output)
    X_test = ds.X_test
    raw_score_table = pd.DataFrame(index=X_test.index)
    raw_score_table["score"] = test_scores

    # Restore race_id and umaban
    if "race_id" in ds.test_data.columns:
        raw_score_table["race_id"] = ds.test_data.loc[X_test.index, "race_id"].values
    elif X_test.index.name == "race_id" or (hasattr(X_test.index, "names") and "race_id" in X_test.index.names):
        pass  # race_id is in the index
    else:
        raw_score_table["race_id"] = X_test.index.get_level_values(0)

    if ResultsCols.UMABAN in ds.test_data.columns:
        raw_score_table[ResultsCols.UMABAN] = ds.test_data.loc[X_test.index, ResultsCols.UMABAN].values

    test_data = ds.test_data

    # --- Calibrations ---
    raw_scores_series = pd.Series(test_scores, index=X_test.index)
    calibrations = {"none": raw_scores_series.copy()}

    if valid_scores is not None:
        for cal_name in ["platt", "isotonic"]:
            try:
                calibrator = fit_calibrator(
                    cal_name,
                    ds.y_valid.values,
                    valid_scores,
                )
                calibrations[cal_name] = pd.Series(
                    calibrator(test_scores), index=X_test.index,
                )
            except Exception as e:
                print(f"    calibration {cal_name} failed: {e}")

    # --- Sweep ---
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
            print(f"    [{cal_name}] th={th:.3f}  RR={rr:.4f}  n={n:.0f}")

            if rr > overall_best_rr:
                overall_best_rr = rr
                overall_best = {
                    "cal": cal_name, "threshold": th, "rr": rr,
                    "n_bets": n, "score_std": score_std,
                }
        else:
            print(f"    [{cal_name}] no valid threshold")

        fk_df.to_csv(os.path.join(out_dir, f"fukusho_sweep_{cal_name}.csv"), index=False)

    if overall_best is None:
        return {
            "label": label, "best_cal": None, "best_th": None,
            "best_rr": 0, "n_bets": 0,
            "ci_lower": 0, "ci_upper": 0, "p_rr_lt_1": 1.0,
        }

    best_cal = overall_best["cal"]
    best_th = overall_best["threshold"]
    best_rr = overall_best["rr"]
    best_n = overall_best["n_bets"]
    best_score_std = overall_best["score_std"]

    # --- Bootstrap CI ---
    boot = bootstrap_ci(
        best_score_std, sim, threshold=best_th,
        n_boot=n_boot, seed=boot_seed, max_bets_per_race=3,
    )

    print(f"    Best: [{best_cal}] th={best_th:.3f}  RR={best_rr:.4f}  n={best_n:.0f}")
    print(f"    CI: [{boot['ci_lower']:.4f}, {boot['ci_upper']:.4f}]  "
          f"P(RR<1)={boot['p_value_lt_1']:.4f}")

    # Save bootstrap
    ci_row = {
        "label": label, "calibration": best_cal, "threshold": best_th,
        "point_rr": boot["point_rr"], "n_bets": boot["n_bets"],
        "n_races": boot["n_races"], "n_hits": boot["n_hits"],
        "ci_lower": boot["ci_lower"], "ci_upper": boot["ci_upper"],
        "p_rr_lt_1": boot["p_value_lt_1"], "boot_mean": boot["mean"],
    }
    pd.DataFrame([ci_row]).to_csv(os.path.join(out_dir, "bootstrap_ci.csv"), index=False)

    # --- Breakdown ---
    profit_months_str = "N/A"
    if do_breakdown:
        month_df = monthly_breakdown(best_score_std, sim, best_th, test_data)
        if not month_df.empty:
            month_df.to_csv(os.path.join(out_dir, "monthly.csv"))
            profit_months = (month_df["profit"] > 0).sum()
            total_months = len(month_df)
            profit_months_str = f"{profit_months}/{total_months}"
            print(f"    Profit months: {profit_months_str}")

        venue_df = venue_breakdown(best_score_std, sim, best_th, test_data)
        if not venue_df.empty:
            venue_df.to_csv(os.path.join(out_dir, "venue.csv"))

    return {
        "label": label, "best_cal": best_cal, "best_th": best_th,
        "best_rr": best_rr, "n_bets": int(best_n),
        "n_races": boot["n_races"], "n_hits": boot["n_hits"],
        "ci_lower": boot["ci_lower"], "ci_upper": boot["ci_upper"],
        "p_rr_lt_1": boot["p_value_lt_1"],
        "boot_mean": boot["mean"],
        "profit_months": profit_months_str,
    }


# ===========================================================================
# Main
# ===========================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="P13: アンサンブルモデル (seed平均)")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--params", required=True,
                    help="JSON file with model params (P11-ext best)")
    ap.add_argument("--selected-features", default=None,
                    help="JSON file with selected feature column names")
    ap.add_argument("--test-start", default=DEFAULT_TEST_START)
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-models", type=int, default=10,
                    help="Number of models (different seeds) to train")
    ap.add_argument("--n-sweep", type=int, default=200,
                    help="Number of threshold points")
    ap.add_argument("--min-bets", type=int, default=10,
                    help="Minimum n_bets for valid threshold")
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--boot-seed", type=int, default=42)
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

    out_dir = args.out_dir or f"models/{pd.Timestamp.now().strftime('%Y%m%d')}/p13"
    os.makedirs(out_dir, exist_ok=True)

    with open(args.params, "r", encoding="utf-8") as f:
        best_params = json.load(f)

    # Seeds for ensemble (0, 1, ..., N-1)
    seeds = list(range(args.n_models))

    print(f"\n{'#'*70}")
    print(f"# P13: アンサンブルモデル (seed平均)")
    print(f"# featured: {featured_path}")
    print(f"# params: {args.params}")
    print(f"# selected_features: {args.selected_features or 'None'}")
    print(f"# n_models: {args.n_models}")
    print(f"# seeds: {seeds}")
    print(f"# test_start: {args.test_start}")
    print(f"# out_dir: {out_dir}")
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

    # ================================================================
    # Split data (once — deterministic)
    # ================================================================
    test_start = args.test_start
    test_end = args.test_end

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, dates.min().strftime("%Y-%m-%d"), test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    print(f"\n  train: {len(train_df):,} rows, test: {len(test_df):,} rows")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    # Create DataSplitter (same for all models)
    ds = DataSplitter(combined.copy(), test_size=test_ratio, valid_size=args.valid_ratio)

    # Simulator
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    thresholds = _linspace_inclusive(0.0, 4.0, args.n_sweep)

    # ================================================================
    # Phase 1: Train N models with different seeds
    # ================================================================
    print(f"\n{'='*70}")
    print(f"Phase 1: N={args.n_models} 個のモデルを学習")
    print(f"{'='*70}")

    all_test_preds = []   # shape: (n_models, n_test_samples)
    all_valid_preds = []  # shape: (n_models, n_valid_samples)
    seed_aucs = []

    for i, seed in enumerate(seeds):
        t_seed = time.time()
        print(f"\n  --- Model {i+1}/{args.n_models} (seed={seed}) ---")

        test_preds, valid_preds, train_preds, auc_train, auc_test = \
            train_single_model(ds, best_params, seed)

        all_test_preds.append(test_preds)
        all_valid_preds.append(valid_preds)
        seed_aucs.append({
            "seed": seed,
            "auc_train": auc_train,
            "auc_test": auc_test,
            "auc_gap": auc_train - auc_test,
        })

        elapsed_seed = time.time() - t_seed
        print(f"  AUC: {auc_train:.4f}(train), {auc_test:.4f}(test), "
              f"gap={auc_train-auc_test:.3f}, time={elapsed_seed:.1f}s")

    # AUC summary
    auc_df = pd.DataFrame(seed_aucs)
    auc_df.to_csv(os.path.join(out_dir, "seed_aucs.csv"), index=False)
    print(f"\n  AUC(test) stats: mean={auc_df['auc_test'].mean():.4f}, "
          f"std={auc_df['auc_test'].std():.4f}, "
          f"min={auc_df['auc_test'].min():.4f}, max={auc_df['auc_test'].max():.4f}")
    print(f"  AUC gap stats:   mean={auc_df['auc_gap'].mean():.4f}, "
          f"std={auc_df['auc_gap'].std():.4f}")

    # ================================================================
    # Phase 2: 個別 seed の sweep 結果 (分散把握)
    # ================================================================
    print(f"\n{'='*70}")
    print(f"Phase 2: 個別 seed ごとの sweep (簡易)")
    print(f"{'='*70}")

    individual_results = []
    for i, seed in enumerate(seeds):
        seed_dir = os.path.join(out_dir, f"seed_{seed}")
        print(f"\n  --- Seed {seed} ---")
        result = evaluate_predictions(
            test_scores=all_test_preds[i],
            valid_scores=all_valid_preds[i],
            ds=ds, sim=sim,
            thresholds=thresholds,
            min_bets=args.min_bets,
            n_boot=args.n_boot,
            boot_seed=args.boot_seed,
            label=f"seed_{seed}",
            out_dir=seed_dir,
            do_breakdown=False,  # 個別では省略
        )
        result["auc_test"] = seed_aucs[i]["auc_test"]
        result["auc_gap"] = seed_aucs[i]["auc_gap"]
        individual_results.append(result)

    # Individual summary
    ind_df = pd.DataFrame(individual_results)
    ind_df.to_csv(os.path.join(out_dir, "individual_seeds.csv"), index=False)

    print(f"\n  Individual seed results:")
    print(f"  {'Seed':<10} {'Cal':<10} {'Th':>6} {'RR':>8} {'n_bets':>7} "
          f"{'CI_low':>8} {'CI_high':>8} {'P(RR<1)':>9}")
    print(f"  {'-'*70}")
    for r in individual_results:
        if r["best_cal"] is None:
            print(f"  {r['label']:<10} --- no valid threshold ---")
            continue
        print(f"  {r['label']:<10} {r['best_cal']:<10} {r['best_th']:>6.3f} "
              f"{r['best_rr']:>8.4f} {r['n_bets']:>7} "
              f"{r['ci_lower']:>8.4f} {r['ci_upper']:>8.4f} {r['p_rr_lt_1']:>9.4f}")

    valid_ind = [r for r in individual_results if r["best_cal"] is not None]
    if valid_ind:
        ind_rrs = [r["best_rr"] for r in valid_ind]
        print(f"\n  RR stats across seeds: mean={np.mean(ind_rrs):.4f}, "
              f"std={np.std(ind_rrs):.4f}, min={np.min(ind_rrs):.4f}, max={np.max(ind_rrs):.4f}")
        rr_gt_1 = sum(1 for rr in ind_rrs if rr > 1.0)
        print(f"  Seeds with RR>1.0: {rr_gt_1}/{len(ind_rrs)}")

    # ================================================================
    # Phase 3: アンサンブル (予測平均)
    # ================================================================
    print(f"\n{'='*70}")
    print(f"Phase 3: アンサンブル (N={args.n_models} 予測平均)")
    print(f"{'='*70}")

    ensemble_test_preds = np.mean(all_test_preds, axis=0)
    ensemble_valid_preds = np.mean(all_valid_preds, axis=0)

    # Ensemble AUC
    try:
        ens_auc_test = roc_auc_score(ds.y_test, ensemble_test_preds)
    except Exception:
        ens_auc_test = float("nan")
    print(f"\n  Ensemble AUC(test): {ens_auc_test:.4f}  "
          f"(single mean: {auc_df['auc_test'].mean():.4f})")

    # Prediction correlation between seeds
    pred_corrs = []
    for i in range(len(seeds)):
        for j in range(i + 1, len(seeds)):
            corr = np.corrcoef(all_test_preds[i], all_test_preds[j])[0, 1]
            pred_corrs.append(corr)
    mean_corr = np.mean(pred_corrs) if pred_corrs else 1.0
    print(f"  Mean pairwise correlation between seeds: {mean_corr:.6f}")
    print(f"  Prediction diversity (1 - corr): {1 - mean_corr:.6f}")

    # Prediction std per sample
    pred_std = np.std(all_test_preds, axis=0)
    print(f"  Per-sample prediction std: mean={pred_std.mean():.6f}, "
          f"max={pred_std.max():.6f}")

    # Evaluate ensemble
    ens_dir = os.path.join(out_dir, "ensemble")
    print(f"\n  --- Ensemble sweep ---")
    ens_result = evaluate_predictions(
        test_scores=ensemble_test_preds,
        valid_scores=ensemble_valid_preds,
        ds=ds, sim=sim,
        thresholds=thresholds,
        min_bets=args.min_bets,
        n_boot=args.n_boot,
        boot_seed=args.boot_seed,
        label="ensemble",
        out_dir=ens_dir,
        do_breakdown=True,
    )
    ens_result["auc_test"] = ens_auc_test

    # ================================================================
    # Phase 4: サブアンサンブル (3, 5, 7 models) — スケーリング効果
    # ================================================================
    print(f"\n{'='*70}")
    print(f"Phase 4: サブアンサンブル (モデル数のスケーリング効果)")
    print(f"{'='*70}")

    sub_sizes = [3, 5, 7] if args.n_models >= 7 else [3, 5] if args.n_models >= 5 else []
    sub_results = []

    for n_sub in sub_sizes:
        sub_test = np.mean(all_test_preds[:n_sub], axis=0)
        sub_valid = np.mean(all_valid_preds[:n_sub], axis=0)

        try:
            sub_auc = roc_auc_score(ds.y_test, sub_test)
        except Exception:
            sub_auc = float("nan")

        sub_dir = os.path.join(out_dir, f"sub_{n_sub}")
        print(f"\n  --- Sub-ensemble (N={n_sub}) AUC={sub_auc:.4f} ---")
        sub_res = evaluate_predictions(
            test_scores=sub_test,
            valid_scores=sub_valid,
            ds=ds, sim=sim,
            thresholds=thresholds,
            min_bets=args.min_bets,
            n_boot=args.n_boot,
            boot_seed=args.boot_seed,
            label=f"sub_{n_sub}",
            out_dir=sub_dir,
            do_breakdown=False,
        )
        sub_res["n_models"] = n_sub
        sub_res["auc_test"] = sub_auc
        sub_results.append(sub_res)

    # ================================================================
    # Final Summary
    # ================================================================
    elapsed = time.time() - t_total

    print(f"\n{'='*70}")
    print(f"P13 FINAL SUMMARY — アンサンブルモデル")
    print(f"{'='*70}")
    print(f"  Model params: {args.params}")
    print(f"  Features: {args.selected_features or 'all'}")
    print(f"  N models: {args.n_models}")
    print()

    # --- Individual seed stats ---
    print(f"  [Individual Seeds]")
    if valid_ind:
        ind_rrs = [r["best_rr"] for r in valid_ind]
        ind_p = [r["p_rr_lt_1"] for r in valid_ind]
        print(f"  RR: mean={np.mean(ind_rrs):.4f}, std={np.std(ind_rrs):.4f}, "
              f"range=[{np.min(ind_rrs):.4f}, {np.max(ind_rrs):.4f}]")
        print(f"  P(RR<1): mean={np.mean(ind_p):.4f}, "
              f"range=[{np.min(ind_p):.4f}, {np.max(ind_p):.4f}]")
        print(f"  Seeds with RR>1.0: {sum(1 for rr in ind_rrs if rr > 1.0)}/{len(ind_rrs)}")

    # --- Ensemble result ---
    print(f"\n  [Ensemble (N={args.n_models})]")
    if ens_result["best_cal"] is not None:
        print(f"  AUC(test): {ens_result['auc_test']:.4f}")
        print(f"  Cal: {ens_result['best_cal']}, Th: {ens_result['best_th']:.3f}")
        print(f"  RR: {ens_result['best_rr']:.4f}, n_bets: {ens_result['n_bets']}")
        print(f"  CI: [{ens_result['ci_lower']:.4f}, {ens_result['ci_upper']:.4f}]")
        print(f"  P(RR<1): {ens_result['p_rr_lt_1']:.4f}")
        print(f"  Profit months: {ens_result.get('profit_months', 'N/A')}")
    else:
        print(f"  No valid threshold found")

    # --- Sub-ensemble scaling ---
    if sub_results:
        print(f"\n  [Scaling Effect]")
        print(f"  {'N':>5} {'AUC':>8} {'Cal':<10} {'Th':>6} {'RR':>8} "
              f"{'n_bets':>7} {'P(RR<1)':>9}")
        print(f"  {'-'*60}")
        for sr in sub_results:
            if sr["best_cal"]:
                print(f"  {sr['n_models']:>5} {sr['auc_test']:>8.4f} "
                      f"{sr['best_cal']:<10} {sr['best_th']:>6.3f} "
                      f"{sr['best_rr']:>8.4f} {sr['n_bets']:>7} {sr['p_rr_lt_1']:>9.4f}")
            else:
                print(f"  {sr['n_models']:>5} {sr['auc_test']:>8.4f} --- no valid ---")

        if ens_result["best_cal"]:
            print(f"  {args.n_models:>5} {ens_result['auc_test']:>8.4f} "
                  f"{ens_result['best_cal']:<10} {ens_result['best_th']:>6.3f} "
                  f"{ens_result['best_rr']:>8.4f} {ens_result['n_bets']:>7} "
                  f"{ens_result['p_rr_lt_1']:>9.4f}")

    # --- Comparison with P11-ext reference ---
    print(f"\n  [P11-ext Reference Comparison]")
    print(f"  P11-ext (single): RR=1.094, n=66, P(RR<1)=0.298, Cal=none, Th=3.136")
    if ens_result["best_cal"]:
        delta_rr = ens_result["best_rr"] - 1.094
        delta_p = ens_result["p_rr_lt_1"] - 0.298
        print(f"  Ensemble:         RR={ens_result['best_rr']:.4f} ({delta_rr:+.4f}), "
              f"n={ens_result['n_bets']}, "
              f"P(RR<1)={ens_result['p_rr_lt_1']:.4f} ({delta_p:+.4f})")

        if ens_result["best_rr"] > 1.094:
            print(f"\n  ★ アンサンブルが P11-ext を上回った!")
        elif ens_result["best_rr"] > 1.0:
            print(f"\n  ○ RR>1.0 だが P11-ext には及ばない")
        else:
            print(f"\n  ✗ アンサンブルでもエッジなし")

    # --- Diversity analysis ---
    print(f"\n  [Diversity Analysis]")
    print(f"  Mean pairwise prediction correlation: {mean_corr:.6f}")
    print(f"  Diversity (1 - corr): {1 - mean_corr:.6f}")
    if mean_corr > 0.999:
        print(f"  → 極めて高い相関: seed 変更だけでは予測の多様性がほぼ無い")
        print(f"    → アンサンブルによるノイズ低減効果は限定的")
    elif mean_corr > 0.99:
        print(f"  → 高い相関だが一定の多様性あり: 微小なノイズ低減が期待できる")
    else:
        print(f"  → 中程度の多様性: アンサンブルの効果が見込める")

    # Save final summary
    summary = {
        "n_models": args.n_models,
        "seeds": seeds,
        "ensemble_auc_test": ens_auc_test,
        "mean_single_auc_test": float(auc_df["auc_test"].mean()),
        "mean_pred_corr": float(mean_corr),
        "pred_diversity": float(1 - mean_corr),
        "ensemble_result": {k: v for k, v in ens_result.items()
                           if not isinstance(v, (np.ndarray,))},
        "individual_rr_mean": float(np.mean(ind_rrs)) if valid_ind else None,
        "individual_rr_std": float(np.std(ind_rrs)) if valid_ind else None,
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n  Total time: {elapsed:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
