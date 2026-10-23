"""P9-5b: 馬連 BOX / 馬連流し / ワイド流し threshold sweep + Bootstrap CI.

P11-ext best params + pos-perm 145 cols でモデルを学習し、テスト期間で
3券種の threshold sweep + Bootstrap CI を実行する。
複勝も比較ベースラインとして含む。

券種:
  1. 馬連 BOX — BetPolicyUmarenBox (既存)
  2. 馬連流し — BetPolicyUmarenNagashi (新規, 軸=レース内best, 相手=partner_th以上)
  3. ワイド流し — BetPolicyWideNagashi (新規, 同上)
  4. 複勝   — BetPolicyFukusho (比較用)

Usage:
    python tools/run_p9_5b_multi_bet_sweep.py \
        --params models/20260212/p11_ext/optuna_best_params.json \
        --selected-features models/20260210/p11/selected_positive_perm.json \
        --out-dir models/20260213/p9_5b
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
from modules.policies._bet_policy import (
    BetPolicyUmarenNagashi,
    BetPolicyWideNagashi,
)


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
# Sweep functions
# ===========================================================================

def umaren_box_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    min_bets: int = 10,
) -> list[dict]:
    """馬連 BOX threshold sweep。"""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyUmarenBox.judge(
            score_table_std, threshold=float(th),
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        ret["bet_type"] = "umaren_box"
        results.append(ret)
    return results


def umaren_nagashi_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    anchor_thresholds: list[float],
    partner_thresholds: list[float],
    min_bets: int = 10,
) -> list[dict]:
    """馬連流し (anchor=レース内top1) の 2D threshold sweep。"""
    results = []
    for a_th in anchor_thresholds:
        for p_th in partner_thresholds:
            if p_th >= a_th:
                continue  # partner は anchor より低い閾値
            actions = BetPolicyUmarenNagashi.judge(
                score_table_std,
                anchor_threshold=float(a_th),
                partner_threshold=float(p_th),
                anchor_strategy='top1',
            )
            ret = sim.calc_returns(actions)
            ret["anchor_threshold"] = float(a_th)
            ret["partner_threshold"] = float(p_th)
            ret["threshold"] = float(a_th)  # for compatibility
            ret["bet_type"] = "umaren_nagashi"
            results.append(ret)
    return results


def wide_nagashi_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    anchor_thresholds: list[float],
    partner_thresholds: list[float],
    min_bets: int = 10,
) -> list[dict]:
    """ワイド流し (anchor=レース内top1) の 2D threshold sweep。"""
    results = []
    for a_th in anchor_thresholds:
        for p_th in partner_thresholds:
            if p_th >= a_th:
                continue
            actions = BetPolicyWideNagashi.judge(
                score_table_std,
                anchor_threshold=float(a_th),
                partner_threshold=float(p_th),
                anchor_strategy='top1',
            )
            ret = sim.calc_returns(actions)
            ret["anchor_threshold"] = float(a_th)
            ret["partner_threshold"] = float(p_th)
            ret["threshold"] = float(a_th)
            ret["bet_type"] = "wide_nagashi"
            results.append(ret)
    return results


def fukusho_sweep(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    max_bets_per_race: int = 3,
) -> list[dict]:
    """複勝 threshold sweep (comparison baseline)。"""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(th),
            max_bets_per_race=max_bets_per_race,
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        ret["bet_type"] = "fukusho"
        results.append(ret)
    return results


# ===========================================================================
# Bootstrap CI (generic)
# ===========================================================================

def bootstrap_ci(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    bet_type: str,
    threshold: float,
    n_boot: int,
    seed: int = 42,
    anchor_threshold: float | None = None,
    partner_threshold: float | None = None,
) -> dict:
    """レース単位 Bootstrap で RR の CI を推定。"""
    # --- Build actions ---
    if bet_type == "umaren_box":
        actions = policies.BetPolicyUmarenBox.judge(
            score_table_std, threshold=float(threshold),
        )
    elif bet_type == "umaren_nagashi":
        actions = BetPolicyUmarenNagashi.judge(
            score_table_std,
            anchor_threshold=float(anchor_threshold or threshold),
            partner_threshold=float(partner_threshold or 0.0),
            anchor_strategy='top1',
        )
    elif bet_type == "wide_nagashi":
        actions = BetPolicyWideNagashi.judge(
            score_table_std,
            anchor_threshold=float(anchor_threshold or threshold),
            partner_threshold=float(partner_threshold or 0.0),
            anchor_strategy='top1',
        )
    elif bet_type == "fukusho":
        actions = policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(threshold),
            max_bets_per_race=3,
        )
    else:
        raise ValueError(f"Unknown bet_type: {bet_type}")

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
# Monthly / Venue breakdown (generic for all bet types)
# ===========================================================================

def _build_actions(score_table_std, bet_type, threshold, anchor_threshold=None, partner_threshold=None):
    """bet_type に応じた actions dict を生成。"""
    if bet_type == "umaren_box":
        return policies.BetPolicyUmarenBox.judge(score_table_std, threshold=float(threshold))
    elif bet_type == "umaren_nagashi":
        return BetPolicyUmarenNagashi.judge(
            score_table_std,
            anchor_threshold=float(anchor_threshold or threshold),
            partner_threshold=float(partner_threshold or 0.0),
            anchor_strategy='top1',
        )
    elif bet_type == "wide_nagashi":
        return BetPolicyWideNagashi.judge(
            score_table_std,
            anchor_threshold=float(anchor_threshold or threshold),
            partner_threshold=float(partner_threshold or 0.0),
            anchor_strategy='top1',
        )
    elif bet_type == "fukusho":
        return policies.BetPolicyFukusho.judge(
            score_table_std, threshold=float(threshold), max_bets_per_race=3,
        )
    else:
        raise ValueError(f"Unknown bet_type: {bet_type}")


def monthly_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    test_data: pd.DataFrame,
    bet_type: str,
    threshold: float,
    anchor_threshold: float | None = None,
    partner_threshold: float | None = None,
) -> pd.DataFrame:
    """月別の成績。"""
    actions = _build_actions(score_table_std, bet_type, threshold, anchor_threshold, partner_threshold)
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

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
    test_data: pd.DataFrame,
    bet_type: str,
    threshold: float,
    anchor_threshold: float | None = None,
    partner_threshold: float | None = None,
) -> pd.DataFrame:
    """場別の成績。"""
    actions = _build_actions(score_table_std, bet_type, threshold, anchor_threshold, partner_threshold)
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
# Main
# ===========================================================================

def main() -> int:
    ap = argparse.ArgumentParser(description="P9-5b: 馬連BOX/馬連流し/ワイド流し multi-bet sweep")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--params", required=True,
                    help="JSON file with model params")
    ap.add_argument("--selected-features", default=None,
                    help="JSON file with selected feature column names")
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-sweep", type=int, default=100,
                    help="Number of threshold points for BOX sweeps")
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

    out_dir = args.out_dir or f"models/{pd.Timestamp.now().strftime('%Y%m%d')}/p9_5b"
    os.makedirs(out_dir, exist_ok=True)

    with open(args.params, "r", encoding="utf-8") as f:
        best_params = json.load(f)

    print(f"\n{'#'*70}")
    print(f"# P9-5b: Multi-bet sweep (馬連BOX / 馬連流し / ワイド流し)")
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
    # Phase 2: All bet types threshold sweep
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 2: Multi-bet threshold sweep")
    print(f"{'='*60}")

    # BOX thresholds
    box_thresholds = _linspace_inclusive(0.0, 4.0, args.n_sweep)
    # Nagashi thresholds (2D grid)
    anchor_ths = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5]
    partner_ths = [0.0, 0.3, 0.5, 0.8, 1.0, 1.3, 1.5, 2.0, 2.5]

    # Track best results per bet type across all calibrations
    best_results: dict[str, dict] = {}

    for cal_name, cal_scores in calibrations.items():
        cal_score_table = raw_score_table.copy()
        cal_score_table["score"] = cal_scores.values
        score_std = _standardize_within_race(cal_score_table)

        print(f"\n  --- Calibration: {cal_name} ---")

        # --- (1) 馬連 BOX ---
        ub_results = umaren_box_sweep(score_std, sim, box_thresholds, min_bets=args.min_bets)
        ub_df = pd.DataFrame(ub_results)
        valid_ub = ub_df[ub_df["n_bets"] >= args.min_bets]
        if len(valid_ub) > 0:
            best_idx = valid_ub["return_rate"].idxmax()
            r = valid_ub.loc[best_idx]
            print(f"  [{cal_name}] 馬連BOX:   th={r['threshold']:.3f}  RR={r['return_rate']:.4f}  n={r['n_bets']:.0f}")
            key = "umaren_box"
            if key not in best_results or r["return_rate"] > best_results[key]["rr"]:
                best_results[key] = {
                    "cal": cal_name, "threshold": r["threshold"], "rr": r["return_rate"],
                    "n_bets": r["n_bets"], "score_std": score_std,
                    "anchor_threshold": None, "partner_threshold": None,
                }
        else:
            print(f"  [{cal_name}] 馬連BOX:   no valid threshold")
        ub_df.to_csv(os.path.join(out_dir, f"umaren_box_sweep_{cal_name}.csv"), index=False)

        # --- (2) 馬連流し ---
        un_results = umaren_nagashi_sweep(score_std, sim, anchor_ths, partner_ths, min_bets=args.min_bets)
        un_df = pd.DataFrame(un_results)
        valid_un = un_df[un_df["n_bets"] >= args.min_bets]
        if len(valid_un) > 0:
            best_idx = valid_un["return_rate"].idxmax()
            r = valid_un.loc[best_idx]
            print(f"  [{cal_name}] 馬連流し:  a_th={r['anchor_threshold']:.1f}  p_th={r['partner_threshold']:.1f}  RR={r['return_rate']:.4f}  n={r['n_bets']:.0f}")
            key = "umaren_nagashi"
            if key not in best_results or r["return_rate"] > best_results[key]["rr"]:
                best_results[key] = {
                    "cal": cal_name, "threshold": r["anchor_threshold"], "rr": r["return_rate"],
                    "n_bets": r["n_bets"], "score_std": score_std,
                    "anchor_threshold": r["anchor_threshold"],
                    "partner_threshold": r["partner_threshold"],
                }
        else:
            print(f"  [{cal_name}] 馬連流し:  no valid threshold")
        un_df.to_csv(os.path.join(out_dir, f"umaren_nagashi_sweep_{cal_name}.csv"), index=False)

        # --- (3) ワイド流し ---
        wn_results = wide_nagashi_sweep(score_std, sim, anchor_ths, partner_ths, min_bets=args.min_bets)
        wn_df = pd.DataFrame(wn_results)
        valid_wn = wn_df[wn_df["n_bets"] >= args.min_bets]
        if len(valid_wn) > 0:
            best_idx = valid_wn["return_rate"].idxmax()
            r = valid_wn.loc[best_idx]
            print(f"  [{cal_name}] ワイド流し: a_th={r['anchor_threshold']:.1f}  p_th={r['partner_threshold']:.1f}  RR={r['return_rate']:.4f}  n={r['n_bets']:.0f}")
            key = "wide_nagashi"
            if key not in best_results or r["return_rate"] > best_results[key]["rr"]:
                best_results[key] = {
                    "cal": cal_name, "threshold": r["anchor_threshold"], "rr": r["return_rate"],
                    "n_bets": r["n_bets"], "score_std": score_std,
                    "anchor_threshold": r["anchor_threshold"],
                    "partner_threshold": r["partner_threshold"],
                }
        else:
            print(f"  [{cal_name}] ワイド流し: no valid threshold")
        wn_df.to_csv(os.path.join(out_dir, f"wide_nagashi_sweep_{cal_name}.csv"), index=False)

        # --- (4) 複勝 (baseline) ---
        fk_results = fukusho_sweep(score_std, sim, box_thresholds, max_bets_per_race=3)
        fk_df = pd.DataFrame(fk_results)
        valid_fk = fk_df[fk_df["n_bets"] >= args.min_bets]
        if len(valid_fk) > 0:
            best_idx = valid_fk["return_rate"].idxmax()
            r = valid_fk.loc[best_idx]
            print(f"  [{cal_name}] 複勝:     th={r['threshold']:.3f}  RR={r['return_rate']:.4f}  n={r['n_bets']:.0f}")
            key = "fukusho"
            if key not in best_results or r["return_rate"] > best_results[key]["rr"]:
                best_results[key] = {
                    "cal": cal_name, "threshold": r["threshold"], "rr": r["return_rate"],
                    "n_bets": r["n_bets"], "score_std": score_std,
                    "anchor_threshold": None, "partner_threshold": None,
                }
        else:
            print(f"  [{cal_name}] 複勝:     no valid threshold")
        fk_df.to_csv(os.path.join(out_dir, f"fukusho_sweep_{cal_name}.csv"), index=False)

    # ================================================================
    # Print sweep summary
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Sweep Summary (best per bet type)")
    print(f"{'='*60}")
    print(f"  {'Bet Type':<18} {'Cal':<10} {'Threshold':<18} {'RR':>8} {'n_bets':>8}")
    print(f"  {'-'*62}")
    for bt in ["umaren_box", "umaren_nagashi", "wide_nagashi", "fukusho"]:
        if bt in best_results:
            r = best_results[bt]
            if r.get("anchor_threshold") is not None:
                th_str = f"a={r['anchor_threshold']:.1f} p={r['partner_threshold']:.1f}"
            else:
                th_str = f"{r['threshold']:.3f}"
            print(f"  {bt:<18} {r['cal']:<10} {th_str:<18} {r['rr']:>8.4f} {r['n_bets']:>8.0f}")
        else:
            print(f"  {bt:<18} --- no valid result ---")

    # ================================================================
    # Phase 3: Bootstrap CI for each best bet type
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 3: Bootstrap CI ({args.n_boot} resamples)")
    print(f"{'='*60}")

    boot_results: dict[str, dict] = {}
    for bt, info in best_results.items():
        print(f"\n  --- {bt} ---")
        t0 = time.time()
        boot = bootstrap_ci(
            info["score_std"], sim,
            bet_type=bt,
            threshold=info["threshold"],
            n_boot=args.n_boot,
            seed=args.seed,
            anchor_threshold=info.get("anchor_threshold"),
            partner_threshold=info.get("partner_threshold"),
        )
        bt_time = time.time() - t0
        boot_results[bt] = boot

        hit_rate = boot["n_hits"] / boot["n_races"] * 100 if boot["n_races"] > 0 else 0
        print(f"  Point RR:  {boot['point_rr']:.4f}")
        print(f"  n_bets:    {boot['n_bets']}")
        print(f"  n_races:   {boot['n_races']}")
        print(f"  n_hits:    {boot['n_hits']} ({hit_rate:.1f}%)")
        print(f"  95% CI:    [{boot['ci_lower']:.4f}, {boot['ci_upper']:.4f}]")
        print(f"  P(RR<1):   {boot['p_value_lt_1']:.4f}")
        print(f"  Boot mean: {boot['mean']:.4f}")
        print(f"  Time:      {bt_time:.1f}s")

        # Save
        ci_row = {
            "bet_type": bt,
            "calibration": info["cal"],
            "threshold": info["threshold"],
            "anchor_threshold": info.get("anchor_threshold"),
            "partner_threshold": info.get("partner_threshold"),
            "point_rr": boot["point_rr"],
            "n_bets": boot["n_bets"],
            "n_races": boot["n_races"],
            "n_hits": boot["n_hits"],
            "ci_lower": boot["ci_lower"],
            "ci_upper": boot["ci_upper"],
            "p_rr_lt_1": boot["p_value_lt_1"],
            "boot_mean": boot["mean"],
        }
        ci_df = pd.DataFrame([ci_row])
        ci_df.to_csv(os.path.join(out_dir, f"bootstrap_ci_{bt}.csv"), index=False)

    # ================================================================
    # Phase 4: Monthly + Venue breakdown for best bet types
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 4: Monthly + Venue breakdown")
    print(f"{'='*60}")

    for bt, info in best_results.items():
        score_std = info["score_std"]
        a_th = info.get("anchor_threshold")
        p_th = info.get("partner_threshold")
        th = info["threshold"]

        print(f"\n  *** {bt} ***")

        # Monthly
        month_df = monthly_breakdown(score_std, sim, test_data, bt, th, a_th, p_th)
        if not month_df.empty:
            print(f"  Monthly:")
            print(month_df.to_string(max_cols=10))
            profit_months = (month_df["profit"] > 0).sum()
            total_months = len(month_df)
            print(f"  Profit months: {profit_months}/{total_months}")
            month_df.to_csv(os.path.join(out_dir, f"monthly_{bt}.csv"))

        # Venue
        venue_df = venue_breakdown(score_std, sim, test_data, bt, th, a_th, p_th)
        if not venue_df.empty:
            print(f"  Venue:")
            print(venue_df.to_string(max_cols=10))
            venue_df.to_csv(os.path.join(out_dir, f"venue_{bt}.csv"))

    # ================================================================
    # Final Summary
    # ================================================================
    elapsed = time.time() - t_total
    print(f"\n{'='*70}")
    print(f"P9-5b FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"  Model params: {args.params}")
    print(f"  Features: {args.selected_features or 'all'}")
    print(f"  AUC: {auc_train:.3f}(train), {auc_test:.3f}(test), gap={auc_train-auc_test:.3f}")
    print()

    print(f"  {'Bet Type':<18} {'RR':>8} {'n_bets':>8} {'CI_low':>8} {'CI_high':>8} {'P(RR<1)':>9} {'Sig?':>5}")
    print(f"  {'-'*70}")
    for bt in ["umaren_box", "umaren_nagashi", "wide_nagashi", "fukusho"]:
        if bt in boot_results:
            b = boot_results[bt]
            sig = "OK" if b["p_value_lt_1"] < 0.05 else "NG"
            print(f"  {bt:<18} {b['point_rr']:>8.4f} {b['n_bets']:>8} "
                  f"{b['ci_lower']:>8.4f} {b['ci_upper']:>8.4f} "
                  f"{b['p_value_lt_1']:>9.4f} [{sig}]")
        else:
            print(f"  {bt:<18} --- no data ---")

    print()
    # Compare with P11-ext baseline
    print(f"  Reference: P11-ext 複勝 RR=1.094, P(RR<1)=0.298")
    print()

    any_sig = any(b["p_value_lt_1"] < 0.05 for b in boot_results.values())
    any_better = any(b["point_rr"] > 1.094 for b in boot_results.values())
    if any_sig:
        print(f"  [!!] Some bet types show statistical significance!")
    elif any_better:
        print(f"  [OK] Some bet types beat P11-ext RR, but not significant.")
    else:
        print(f"  [NG] No bet type beat P11-ext RR=1.094.")

    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
