"""P15: score_rank × 人気 交差条件の長期バックテスト.

P11-ext best params で学習 → テスト期間で score_rank × 人気 の交差条件を
グリッドサーチし、複勝 RR を評価する。
有望条件は Bootstrap CI で統計的有意性を検定する。

Usage:
  .venv/Scripts/python.exe tools/run_p15_rank_pop_backtest.py
  .venv/Scripts/python.exe tools/run_p15_rank_pop_backtest.py --out-dir models/20260220/p15
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pickle
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

from sklearn.metrics import roc_auc_score

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._data_splitter import DataSplitter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURED_PATH = "data/tmp/featured_data_20260205.pickle"
SELECTED_FEATURES = "models/20260210/p11/selected_positive_perm.json"
BEST_PARAMS_PATH = "models/20260212/p11_ext/optuna_best_params.json"

VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _convert_to_sklearn_params(raw: dict) -> dict:
    """Optuna best params → sklearn-compatible parameter dict."""
    return {
        "num_leaves": int(raw.get("num_leaves", 31)),
        "max_depth": int(raw.get("max_depth", -1)),
        "learning_rate": float(raw.get("learning_rate", 0.1)),
        "n_estimators": int(raw.get("n_estimators", 300)),
        "min_child_samples": int(raw.get("min_child_samples", 20)),
        "subsample": float(raw.get("subsample", 1.0)),
        "subsample_freq": int(raw.get("subsample_freq", 0)),
        "colsample_bytree": float(raw.get("colsample_bytree", 1.0)),
        "reg_alpha": float(raw.get("reg_alpha", 0.0)),
        "reg_lambda": float(raw.get("reg_lambda", 0.0)),
        "min_split_gain": float(raw.get("min_split_gain", 0.0)),
    }


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
    """レース内 z-score 標準化。"""
    st = score_table.copy()
    if "race_id" in st.columns:
        keys = st["race_id"]
    else:
        try:
            keys = st.index.get_level_values(0)
        except Exception:
            keys = None
    s = pd.to_numeric(st["score"], errors="coerce").astype(float)
    if keys is None:
        m, std = float(s.mean()), float(s.std(ddof=0))
        st["score"] = ((s - m) / std).replace([np.inf, -np.inf], 0).fillna(0) if std > 0 else s.fillna(0)
        return st
    g = s.groupby(keys)
    mean_ = g.transform("mean")
    std_ = g.transform("std").replace(0, np.nan)
    z = (s - mean_) / std_
    st["score"] = z.where(np.isfinite(z), s).fillna(0).astype(float)
    return st


def _add_score_rank(score_table: pd.DataFrame) -> pd.DataFrame:
    """レース内 score の降順ランクを付与。"""
    st = score_table.copy()
    if "race_id" in st.columns:
        st["score_rank"] = st.groupby("race_id")["score"].rank(
            ascending=False, method="min"
        ).astype(int)
    else:
        keys = st.index.get_level_values(0)
        st["score_rank"] = pd.to_numeric(st["score"], errors="coerce").groupby(keys).rank(
            ascending=False, method="min"
        ).astype(int)
    return st


def _add_popularity(test_data: pd.DataFrame, score_table: pd.DataFrame) -> pd.DataFrame:
    """test_data の単勝オッズから人気 (popularity) を算出し、score_table に付与。"""
    st = score_table.copy()
    td = test_data.copy()

    # 人気 = race内の 単勝 昇順ランク
    td["人気"] = td.groupby("race_id")[ResultsCols.TANSHO_ODDS].rank(method="min")

    # score_table に人気をマージ
    if "race_id" in st.columns:
        # race_id + 馬番 で結合
        pop_map = td[["race_id", ResultsCols.UMABAN, "人気"]].dropna(subset=["人気"])
        st = st.merge(pop_map, on=["race_id", ResultsCols.UMABAN], how="left")
    else:
        # index ベース
        td_pop = td["人気"]
        st["人気"] = td_pop.reindex(st.index).values

    return st


# ---------------------------------------------------------------------------
# Condition generators
# ---------------------------------------------------------------------------

def _generate_conditions() -> list[dict]:
    """57 条件のリストを生成。"""
    conditions = []

    # --- A. score_rank × 人気 交差条件 (34 conditions) ---
    for max_rank in range(1, 6):
        max_pops = range(1, 9) if max_rank <= 3 else range(1, 6)
        for max_pop in max_pops:
            conditions.append({
                "name": f"rank<={max_rank}_pop<={max_pop}",
                "type": "rank_pop",
                "max_rank": max_rank,
                "max_pop": max_pop,
            })

    # --- A-ext. 穴馬パターン (3 conditions) ---
    conditions.append({
        "name": "rank<=3_pop>=4",
        "type": "rank_pop_reverse",
        "max_rank": 3,
        "min_pop": 4,
    })
    conditions.append({
        "name": "rank<=2_pop>=5",
        "type": "rank_pop_reverse",
        "max_rank": 2,
        "min_pop": 5,
    })
    conditions.append({
        "name": "rank=1_pop>=4",
        "type": "rank_pop_reverse",
        "max_rank": 1,
        "min_pop": 4,
    })

    # --- B. z-score 閾値条件 (16 conditions) ---
    for th in [2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.136, 3.4]:
        conditions.append({
            "name": f"zscore>={th:.3f}_maxbet3",
            "type": "zscore",
            "threshold": th,
            "max_bets": 3,
        })
        conditions.append({
            "name": f"zscore>={th:.3f}_nolimit",
            "type": "zscore",
            "threshold": th,
            "max_bets": None,
        })

    # --- C. ハイブリッド条件 (4 conditions) ---
    hybrid_params = [
        (3, 3, 1.0),
        (3, 3, 1.5),
        (2, 3, 1.0),
        (2, 4, 1.0),
    ]
    for max_rank, max_pop, min_z in hybrid_params:
        conditions.append({
            "name": f"rank<={max_rank}_pop<={max_pop}_z>={min_z}",
            "type": "hybrid",
            "max_rank": max_rank,
            "max_pop": max_pop,
            "min_zscore": min_z,
        })

    return conditions


def _apply_condition(
    score_table_std: pd.DataFrame,
    cond: dict,
) -> dict:
    """条件に従って対象馬をフィルタし、fukusho actions dict を返す。

    Returns:
        actions: {race_id: {'fukusho': [umaban_list]}}
    """
    st = score_table_std

    if cond["type"] == "rank_pop":
        mask = (st["score_rank"] <= cond["max_rank"]) & (st["人気"] <= cond["max_pop"])
        filtered = st[mask]
    elif cond["type"] == "rank_pop_reverse":
        mask = (st["score_rank"] <= cond["max_rank"]) & (st["人気"] >= cond["min_pop"])
        filtered = st[mask]
    elif cond["type"] == "zscore":
        mask = st["score"] >= cond["threshold"]
        filtered = st[mask]
        if cond["max_bets"] is not None:
            k = cond["max_bets"]
            if "race_id" in filtered.columns:
                filtered = (
                    filtered.sort_values(["race_id", "score"], ascending=[True, False])
                    .groupby("race_id", sort=False)
                    .head(k)
                )
            else:
                keys = filtered.index.get_level_values(0)
                filtered = (
                    filtered.assign(_rk=keys)
                    .sort_values(["_rk", "score"], ascending=[True, False])
                    .groupby("_rk", sort=False)
                    .head(k)
                    .drop(columns=["_rk"])
                )
    elif cond["type"] == "hybrid":
        mask = (
            (st["score_rank"] <= cond["max_rank"])
            & (st["人気"] <= cond["max_pop"])
            & (st["score"] >= cond["min_zscore"])
        )
        filtered = st[mask]
    else:
        raise ValueError(f"Unknown condition type: {cond['type']}")

    # Build actions dict: {race_id: {'fukusho': [umaban_list]}}
    if filtered.empty:
        return {}

    if "race_id" in filtered.columns:
        bet_df = filtered.groupby("race_id")[ResultsCols.UMABAN].apply(list)
    else:
        bet_df = filtered.groupby(level=0)[ResultsCols.UMABAN].apply(list)

    actions = {}
    for race_id, umabans in bet_df.items():
        actions[race_id] = {"fukusho": umabans}

    return actions


# ---------------------------------------------------------------------------
# Bootstrap CI
# ---------------------------------------------------------------------------

def _bootstrap_ci(
    per_race: pd.DataFrame,
    n_boot: int = 10000,
    seed: int = 42,
) -> dict:
    """レース単位 Bootstrap で fukusho RR の CI を推定。"""
    if per_race.empty:
        return {
            "point_rr": 0.0, "mean": 0.0, "ci_lower": 0.0, "ci_upper": 0.0,
            "p_value_lt_1": 1.0, "n_races": 0, "n_bets": 0, "n_hits": 0,
        }

    n_races = len(per_race)
    rng = np.random.RandomState(seed)
    rr_dist = np.empty(n_boot)
    bet_arr = per_race["bet_amount"].values
    ret_arr = per_race["return_amount"].values

    for b in range(n_boot):
        idx = rng.choice(n_races, size=n_races, replace=True)
        total_bet = bet_arr[idx].sum()
        total_ret = ret_arr[idx].sum()
        rr_dist[b] = total_ret / total_bet if total_bet > 0 else 0.0

    return {
        "point_rr": float(ret_arr.sum() / bet_arr.sum()) if bet_arr.sum() > 0 else 0.0,
        "mean": float(np.mean(rr_dist)),
        "ci_lower": float(np.percentile(rr_dist, 2.5)),
        "ci_upper": float(np.percentile(rr_dist, 97.5)),
        "p_value_lt_1": float(np.mean(rr_dist < 1.0)),
        "n_races": n_races,
        "n_bets": int(per_race["n_bets"].sum()),
        "n_hits": int(per_race["hit_or_not"].sum()),
    }


# ---------------------------------------------------------------------------
# Monthly / Venue breakdown
# ---------------------------------------------------------------------------

def _monthly_breakdown(
    per_race: pd.DataFrame,
    test_data: pd.DataFrame,
) -> pd.DataFrame:
    """月別成績。"""
    if per_race.empty:
        return pd.DataFrame()
    if "race_id" in test_data.columns:
        date_map = test_data.groupby("race_id")["date"].first()
    else:
        date_map = test_data.groupby(level=0)["date"].first()
    date_map = pd.to_datetime(date_map)

    pr = per_race.copy()
    pr["month"] = date_map.reindex(pr.index).dt.to_period("M").astype(str)
    pr = pr.dropna(subset=["month"])

    monthly = pr.groupby("month").agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    monthly["return_rate"] = monthly["total_return"] / monthly["total_bet"].replace(0, np.nan)
    monthly["profit"] = monthly["total_return"] - monthly["total_bet"]
    return monthly.sort_index()


def _venue_breakdown(per_race: pd.DataFrame) -> pd.DataFrame:
    """場別成績。"""
    if per_race.empty:
        return pd.DataFrame()
    pr = per_race.copy()
    race_ids = pr.index.astype(str)
    pr["venue_code"] = race_ids.str[4:6]
    pr["venue_name"] = pr["venue_code"].map(VENUE_NAMES).fillna("不明")

    venue = pr.groupby(["venue_code", "venue_name"]).agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    venue["return_rate"] = venue["total_return"] / venue["total_bet"].replace(0, np.nan)
    venue["profit"] = venue["total_return"] - venue["total_bet"]
    return venue.sort_index()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="P15: score_rank × 人気 交差条件バックテスト")
    ap.add_argument("--featured", default=None, help="featured_data path")
    ap.add_argument("--selected-features", default=SELECTED_FEATURES)
    ap.add_argument("--best-params", default=BEST_PARAMS_PATH)
    ap.add_argument("--test-size", type=float, default=0.15)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-boot", type=int, default=10000)
    ap.add_argument("--n-boot-top", type=int, default=5,
                    help="Bootstrap CI を実行する上位条件数")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd, "p15")
    os.makedirs(out_dir, exist_ok=True)

    # ================================================================
    # Phase 1: Load data & Train model
    # ================================================================
    featured_path = args.featured or FEATURED_PATH
    print(f"\n{'#'*70}")
    print(f"# P15: score_rank × 人気 交差条件バックテスト")
    print(f"# featured: {featured_path}")
    print(f"# selected_features: {args.selected_features}")
    print(f"# best_params: {args.best_params}")
    print(f"# test_size: {args.test_size}")
    print(f"# out_dir: {out_dir}")
    print(f"{'#'*70}")

    t0 = time.time()

    # Load featured data
    print(f"\nLoading featured_data: {featured_path}")
    featured_data = pd.read_pickle(featured_path)
    print(f"  shape (raw): {featured_data.shape}")

    # Apply selected features filter
    if args.selected_features and os.path.isfile(args.selected_features):
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

    # Load best params
    with open(args.best_params, "r", encoding="utf-8") as f:
        raw_params = json.load(f)
    best_params = _convert_to_sklearn_params(raw_params)
    print(f"  best params loaded: {args.best_params}")

    # DataSplitter
    ds = DataSplitter(featured_data, test_size=args.test_size, valid_size=args.valid_ratio)

    # Train
    print("\nTraining model...")
    keiba_ai = training.KeibaAI(ds)
    keiba_ai.set_params(best_params)
    keiba_ai.train_without_tuning()

    # AUC
    X_test = ds.X_test
    y_test = ds.y_test
    test_data = ds.test_data

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

    print(f"  AUC: {auc_train:.4f}(train), {auc_test:.4f}(test), gap={auc_train - auc_test:.4f}")
    print(f"  Test rows: {len(test_data):,}")
    print(f"  Test races: {test_data['race_id'].nunique():,}")
    print(f"  Test period: {test_data['date'].min()} ~ {test_data['date'].max()}")

    # ================================================================
    # Phase 2: Prepare score_table with score_rank & 人気
    # ================================================================
    print(f"\n{'='*60}")
    print("Phase 2: Prepare score_table")
    print(f"{'='*60}")

    # z-score 標準化
    score_table_std = _standardize_within_race(raw_score_table)

    # score_rank 付与
    score_table_std = _add_score_rank(score_table_std)

    # 人気 付与 (単勝オッズから)
    score_table_std = _add_popularity(test_data, score_table_std)

    # NaN除外 (人気が取得できない行)
    n_before = len(score_table_std)
    score_table_std = score_table_std.dropna(subset=["人気"])
    score_table_std["人気"] = score_table_std["人気"].astype(int)
    n_after = len(score_table_std)
    print(f"  Rows: {n_before} → {n_after} (removed {n_before - n_after} NaN rows)")
    print(f"  score_rank range: {score_table_std['score_rank'].min()} - {score_table_std['score_rank'].max()}")
    print(f"  人気 range: {score_table_std['人気'].min()} - {score_table_std['人気'].max()}")

    # Simulator
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # ================================================================
    # Phase 3: Grid search all conditions
    # ================================================================
    print(f"\n{'='*60}")
    print("Phase 3: Grid search (57 conditions)")
    print(f"{'='*60}")

    conditions = _generate_conditions()
    print(f"  Total conditions: {len(conditions)}")

    results = []
    for i, cond in enumerate(conditions):
        actions = _apply_condition(score_table_std, cond)
        ret = sim.calc_returns(actions)

        hit_rate = ret["n_hits"] / ret["n_bets"] if ret.get("n_bets", 0) > 0 else 0.0
        rr = ret.get("return_rate", 0.0)
        n_bets = ret.get("n_bets", 0)
        n_races = ret.get("n_races", 0)
        n_hits = ret.get("n_hits", 0)

        row = {
            "condition": cond["name"],
            "type": cond["type"],
            "n_bets": n_bets,
            "n_races": n_races,
            "n_hits": n_hits,
            "hit_rate": hit_rate,
            "return_rate": rr,
        }
        results.append(row)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(conditions)}] processed...")

    results_df = pd.DataFrame(results)

    # Sort by return_rate descending
    results_df = results_df.sort_values("return_rate", ascending=False).reset_index(drop=True)

    # Save grid search results
    csv_path = os.path.join(out_dir, "grid_search_results.csv")
    results_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n  Grid search results saved: {csv_path}")

    # Print top 20
    print(f"\n  {'='*80}")
    print(f"  Top 20 conditions (sorted by RR)")
    print(f"  {'='*80}")
    print(f"  {'condition':<35s}  {'type':<15s}  {'n_bets':>6}  {'n_hits':>6}  {'hit%':>6}  {'RR':>8}")
    for _, row in results_df.head(20).iterrows():
        print(
            f"  {row['condition']:<35s}  {row['type']:<15s}  "
            f"{int(row['n_bets']):>6d}  {int(row['n_hits']):>6d}  "
            f"{row['hit_rate']:>5.1%}  {row['return_rate']:>7.4f}"
        )

    # Also print baseline (people's choice only) for comparison
    print(f"\n  --- ベースライン (人気のみ) ---")
    for max_pop in [1, 2, 3]:
        mask = score_table_std["人気"] <= max_pop
        filtered = score_table_std[mask]
        if "race_id" in filtered.columns:
            bet_df = filtered.groupby("race_id")[ResultsCols.UMABAN].apply(list)
        else:
            bet_df = filtered.groupby(level=0)[ResultsCols.UMABAN].apply(list)
        baseline_actions = {rid: {"fukusho": umas} for rid, umas in bet_df.items()}
        br = sim.calc_returns(baseline_actions)
        hit_rate_b = br["n_hits"] / br["n_bets"] if br.get("n_bets", 0) > 0 else 0
        print(f"  人気<={max_pop}:  n_bets={br.get('n_bets', 0):>6d}  hit={hit_rate_b:>5.1%}  RR={br.get('return_rate', 0):>7.4f}")

    # ================================================================
    # Phase 4: Bootstrap CI for top conditions
    # ================================================================
    n_boot_top = args.n_boot_top
    # Pick top conditions that have n_bets >= 10
    valid_results = results_df[results_df["n_bets"] >= 10]
    top_conditions = valid_results.head(n_boot_top)

    # Also always include key conditions of interest
    key_cond_names = [
        "rank<=2_pop<=3", "rank<=3_pop<=3", "rank<=2_pop<=4",
        "rank<=3_pop<=4", "rank<=1_pop<=3",
        "zscore>=3.136_maxbet3",
    ]
    for kname in key_cond_names:
        if kname not in top_conditions["condition"].values:
            row_k = results_df[results_df["condition"] == kname]
            if not row_k.empty:
                top_conditions = pd.concat([top_conditions, row_k]).drop_duplicates(
                    subset=["condition"]
                )

    print(f"\n{'='*60}")
    print(f"Phase 4: Bootstrap CI ({args.n_boot} iterations) for {len(top_conditions)} conditions")
    print(f"{'='*60}")

    bootstrap_results = []
    for _, row in top_conditions.iterrows():
        cname = row["condition"]
        cond = next(c for c in conditions if c["name"] == cname)
        actions = _apply_condition(score_table_std, cond)
        per_race = sim.calc_returns_per_race(actions)

        t1 = time.time()
        ci = _bootstrap_ci(per_race, n_boot=args.n_boot, seed=args.seed)
        elapsed = time.time() - t1

        ci["condition"] = cname
        ci["type"] = cond["type"]
        ci["ci_width"] = ci["ci_upper"] - ci["ci_lower"]
        bootstrap_results.append(ci)

        print(
            f"  {cname:<35s}  RR={ci['point_rr']:.4f}  "
            f"CI=[{ci['ci_lower']:.3f}, {ci['ci_upper']:.3f}]  "
            f"P(RR<1)={ci['p_value_lt_1']:.3f}  n={ci['n_bets']}  ({elapsed:.1f}s)"
        )

    boot_df = pd.DataFrame(bootstrap_results)
    boot_csv = os.path.join(out_dir, "bootstrap_ci.csv")
    boot_df.to_csv(boot_csv, index=False, encoding="utf-8-sig")
    print(f"\n  Bootstrap CI saved: {boot_csv}")

    # Bonferroni correction note
    n_conds = len(conditions)
    bonferroni_alpha = 0.05 / n_conds
    print(f"\n  多重比較: {n_conds} 条件, Bonferroni α = 0.05/{n_conds} = {bonferroni_alpha:.5f}")
    significant = boot_df[boot_df["p_value_lt_1"] < bonferroni_alpha]
    if len(significant) > 0:
        print(f"  Bonferroni補正後に有意な条件: {len(significant)}")
        for _, r in significant.iterrows():
            print(f"    {r['condition']}: P(RR<1)={r['p_value_lt_1']:.5f}")
    else:
        print(f"  Bonferroni補正後に有意な条件: なし")

    # ================================================================
    # Phase 5: Monthly / Venue breakdown for best condition
    # ================================================================
    # Best = highest RR with n_bets >= 30
    best_valid = boot_df[boot_df["n_bets"] >= 30].sort_values("point_rr", ascending=False)
    if len(best_valid) == 0:
        best_valid = boot_df.sort_values("point_rr", ascending=False)

    if len(best_valid) > 0:
        best_cname = best_valid.iloc[0]["condition"]
        best_cond = next(c for c in conditions if c["name"] == best_cname)

        print(f"\n{'='*60}")
        print(f"Phase 5: Monthly / Venue breakdown (best: {best_cname})")
        print(f"{'='*60}")

        best_actions = _apply_condition(score_table_std, best_cond)
        best_per_race = sim.calc_returns_per_race(best_actions)

        # Monthly
        monthly = _monthly_breakdown(best_per_race, test_data)
        if not monthly.empty:
            monthly_csv = os.path.join(out_dir, "monthly_breakdown.csv")
            monthly.to_csv(monthly_csv, encoding="utf-8-sig")
            print(f"\n  Monthly breakdown ({best_cname}):")
            profit_months = (monthly["profit"] > 0).sum()
            total_months = len(monthly)
            print(f"  Profit months: {profit_months}/{total_months} ({profit_months/total_months:.0%})")
            print(monthly.to_string())

        # Venue
        venue = _venue_breakdown(best_per_race)
        if not venue.empty:
            venue_csv = os.path.join(out_dir, "venue_breakdown.csv")
            venue.to_csv(venue_csv, encoding="utf-8-sig")
            print(f"\n  Venue breakdown ({best_cname}):")
            print(venue.to_string())

    # Also breakdown for rank<=3_pop<=3 if it exists and is different from best
    key_cname = "rank<=3_pop<=3"
    if key_cname != best_cname:
        key_cond = next((c for c in conditions if c["name"] == key_cname), None)
        if key_cond:
            print(f"\n  --- Breakdown for {key_cname} ---")
            key_actions = _apply_condition(score_table_std, key_cond)
            key_per_race = sim.calc_returns_per_race(key_actions)
            key_monthly = _monthly_breakdown(key_per_race, test_data)
            if not key_monthly.empty:
                safe_name = key_cname.replace("<=", "le").replace(">=", "ge")
                key_monthly_csv = os.path.join(out_dir, f"monthly_{safe_name}.csv")
                key_monthly.to_csv(key_monthly_csv, encoding="utf-8-sig")
                profit_m = (key_monthly["profit"] > 0).sum()
                total_m = len(key_monthly)
                print(f"  Profit months: {profit_m}/{total_m} ({profit_m/total_m:.0%})")
                print(key_monthly.to_string())

    # ================================================================
    # Phase 6: Comparison summary
    # ================================================================
    print(f"\n{'='*60}")
    print("Phase 6: Final comparison summary")
    print(f"{'='*60}")

    # Compare top rank_pop conditions vs best z-score condition
    rp_top = boot_df[boot_df["type"].isin(["rank_pop", "hybrid"])].sort_values(
        "point_rr", ascending=False
    )
    zs_top = boot_df[boot_df["type"] == "zscore"].sort_values("point_rr", ascending=False)

    print(f"\n  --- rank×人気 conditions (top 5) ---")
    print(f"  {'condition':<35s}  {'RR':>7s}  {'n':>5s}  {'CI_low':>7s}  {'CI_up':>7s}  {'P(RR<1)':>8s}")
    for _, r in rp_top.head(5).iterrows():
        print(
            f"  {r['condition']:<35s}  {r['point_rr']:>7.4f}  {int(r['n_bets']):>5d}  "
            f"{r['ci_lower']:>7.3f}  {r['ci_upper']:>7.3f}  {r['p_value_lt_1']:>8.3f}"
        )

    print(f"\n  --- z-score conditions (top 3) ---")
    for _, r in zs_top.head(3).iterrows():
        print(
            f"  {r['condition']:<35s}  {r['point_rr']:>7.4f}  {int(r['n_bets']):>5d}  "
            f"{r['ci_lower']:>7.3f}  {r['ci_upper']:>7.3f}  {r['p_value_lt_1']:>8.3f}"
        )

    # ================================================================
    # Save summary JSON
    # ================================================================
    elapsed_total = time.time() - t0
    summary = {
        "experiment": "P15_rank_pop_backtest",
        "date": yyyymmdd,
        "featured_data": featured_path,
        "selected_features": args.selected_features,
        "best_params": args.best_params,
        "test_size": args.test_size,
        "test_rows": len(test_data),
        "test_races": int(test_data["race_id"].nunique()),
        "test_period": f"{test_data['date'].min()} ~ {test_data['date'].max()}",
        "auc_train": float(auc_train),
        "auc_test": float(auc_test),
        "n_conditions": len(conditions),
        "n_boot": args.n_boot,
        "elapsed_seconds": elapsed_total,
        "top_5_rank_pop": (
            rp_top.head(5)[["condition", "point_rr", "n_bets", "ci_lower", "ci_upper", "p_value_lt_1"]]
            .to_dict(orient="records")
        ) if len(rp_top) > 0 else [],
        "top_3_zscore": (
            zs_top.head(3)[["condition", "point_rr", "n_bets", "ci_lower", "ci_upper", "p_value_lt_1"]]
            .to_dict(orient="records")
        ) if len(zs_top) > 0 else [],
    }
    summary_path = os.path.join(out_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n  Summary saved: {summary_path}")
    print(f"  Total elapsed: {elapsed_total:.1f}s ({elapsed_total/60:.1f}min)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
