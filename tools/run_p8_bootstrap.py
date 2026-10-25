"""P8-1: Edge RR=1.025 の統計的頑健性の精密評価。

1) Fine-grain edge sweep (th=0.120-0.155, step=0.001, 36点)
2) Bootstrap CI (レース単位リサンプリング, 2000回)
3) 月別 breakdown
4) 場別 breakdown

Usage:
  .venv/Scripts/python.exe tools/run_p8_bootstrap.py
  .venv/Scripts/python.exe tools/run_p8_bootstrap.py --n-boot 2000
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from sklearn.metrics import roc_auc_score

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._data_splitter import DataSplitter
from modules.training._win_data_splitter import WinDataSplitter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

HPO_PARAMS_PATH = "models/20260209/p7/p7_1_win/optuna_best_params.json"
FEATURED_PATH = "data/tmp/featured_data_20260205.pickle"


# ---------------------------------------------------------------------------
# Helpers (from run_p7_hpo.py)
# ---------------------------------------------------------------------------

def _ensure_win_column(fd: pd.DataFrame) -> pd.DataFrame:
    if "win" in fd.columns:
        return fd
    raw_path = os.path.join(str(REPO_ROOT), "data", "raw", "results.pickle.bak")
    if not os.path.isfile(raw_path):
        raise FileNotFoundError(f"raw results not found: {raw_path}")
    print("  Reconstructing 'win' column from raw results...")
    results = pd.read_pickle(raw_path)
    rank_num = pd.to_numeric(results["着順"], errors="coerce")
    umaban_num = pd.to_numeric(results["馬番"], errors="coerce")
    win_df = pd.DataFrame({
        "race_id": results.index,
        "馬番": umaban_num,
        "win": (rank_num == 1).astype("int8"),
    }).dropna(subset=["馬番", "win"])
    win_df["馬番"] = win_df["馬番"].astype(int)
    win_df = win_df.drop_duplicates(subset=["race_id", "馬番"], keep="first")
    if "race_id" in fd.columns:
        win_df["race_id"] = win_df["race_id"].astype(fd["race_id"].dtype)
    if fd["馬番"].dtype != win_df["馬番"].dtype:
        win_df["馬番"] = win_df["馬番"].astype(fd["馬番"].dtype)
    n_before = len(fd)
    fd = fd.merge(win_df[["race_id", "馬番", "win"]], on=["race_id", "馬番"], how="left")
    assert len(fd) == n_before, f"Merge explosion: {n_before} -> {len(fd)}"
    fd["win"] = fd["win"].fillna(0).astype("int8")
    print(f"  win column: 1={int((fd['win']==1).sum()):,}  0={int((fd['win']==0).sum()):,}")
    return fd


def _convert_to_sklearn_params(params: dict) -> dict:
    out = {"objective": "binary"}
    for k in ["num_leaves", "max_depth", "learning_rate", "n_estimators",
              "min_child_samples", "subsample", "subsample_freq",
              "colsample_bytree", "reg_alpha", "reg_lambda", "min_split_gain"]:
        if k in params:
            out[k] = params[k]
    return out


def _judge_edge_tansho(
    score_table: pd.DataFrame,
    tansho_odds: pd.Series,
    edge_threshold: float,
    max_bets_per_race: int = 2,
) -> dict:
    """Edge >= threshold の馬に単勝購入。"""
    table = score_table.copy()
    odds_col = ResultsCols.TANSHO_ODDS
    if odds_col not in table.columns:
        if tansho_odds is not None:
            table[odds_col] = tansho_odds.reindex(table.index)
        else:
            return {}
    table["score"] = pd.to_numeric(table["score"], errors="coerce")
    table[odds_col] = pd.to_numeric(table[odds_col], errors="coerce")
    table[ResultsCols.UMABAN] = pd.to_numeric(table[ResultsCols.UMABAN], errors="coerce")
    table = table.dropna(subset=["score", odds_col, ResultsCols.UMABAN])
    table = table[(table["score"] >= 0) & (table["score"] <= 1) & (table[odds_col] > 0)]
    table["edge"] = table["score"] - (1.0 / table[odds_col])
    table = table[table["edge"] >= edge_threshold]
    if table.empty:
        return {}
    k = int(max_bets_per_race)
    if "race_id" in table.columns:
        table = (
            table.sort_values(["race_id", "edge"], ascending=[True, False])
            .groupby("race_id", sort=False).head(k)
        )
        bet_df = table.groupby("race_id")[ResultsCols.UMABAN].apply(list).to_frame()
    else:
        race_keys = table.index.get_level_values(0)
        table = (
            table.assign(_rk=race_keys)
            .sort_values(["_rk", "edge"], ascending=[True, False])
            .groupby("_rk", sort=False).head(k)
            .drop(columns=["_rk"])
        )
        bet_df = table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

    def _to_int_list(xs):
        s = pd.to_numeric(pd.Series(xs), errors="coerce")
        return s.dropna().astype(int).drop_duplicates().tolist()

    bet_df[ResultsCols.UMABAN] = bet_df[ResultsCols.UMABAN].apply(_to_int_list)
    bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) > 0]
    return bet_df.rename(columns={ResultsCols.UMABAN: "tansho"}).T.to_dict()


# ---------------------------------------------------------------------------
# Core: Per-race returns for a given threshold
# ---------------------------------------------------------------------------

def get_per_race_returns(
    score_table: pd.DataFrame,
    tansho_odds: pd.Series,
    sim: simulation.Simulator,
    edge_threshold: float,
) -> pd.DataFrame:
    """指定 threshold でベットし、レースごとの return を返す。"""
    actions = _judge_edge_tansho(score_table, tansho_odds, edge_threshold)
    if not actions:
        return pd.DataFrame(columns=["n_bets", "bet_amount", "return_amount", "hit_or_not"])
    return sim.calc_returns_per_race(actions)


# ---------------------------------------------------------------------------
# Fine-grain sweep
# ---------------------------------------------------------------------------

def fine_grain_sweep(
    score_table: pd.DataFrame,
    tansho_odds: pd.Series,
    sim: simulation.Simulator,
    th_start: float = 0.120,
    th_end: float = 0.155,
    th_step: float = 0.001,
) -> pd.DataFrame:
    """Fine-grain edge sweep → DataFrame[threshold, RR, n_bets, n_hits, ...]"""
    thresholds = np.arange(th_start, th_end + th_step / 2, th_step)
    rows = []
    for th in thresholds:
        actions = _judge_edge_tansho(score_table, tansho_odds, float(th))
        if not actions:
            rows.append({"threshold": float(th), "n_bets": 0, "n_races": 0,
                         "n_hits": 0, "total_bet": 0, "total_return": 0,
                         "return_rate": 0.0, "profit": 0.0})
            continue
        rpr = sim.calc_returns_per_race(actions)
        if rpr.empty:
            rows.append({"threshold": float(th), "n_bets": 0, "n_races": 0,
                         "n_hits": 0, "total_bet": 0, "total_return": 0,
                         "return_rate": 0.0, "profit": 0.0})
            continue
        tb = rpr["bet_amount"].sum()
        tr = rpr["return_amount"].sum()
        rows.append({
            "threshold": float(th),
            "n_bets": int(rpr["n_bets"].sum()),
            "n_races": int(rpr.index.nunique()),
            "n_hits": int(rpr["hit_or_not"].sum()),
            "total_bet": int(tb),
            "total_return": int(tr),
            "return_rate": tr / tb if tb > 0 else 0.0,
            "profit": int(tr - tb),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Bootstrap CI (race-level resampling)
# ---------------------------------------------------------------------------

def bootstrap_ci(
    per_race_returns: pd.DataFrame,
    n_boot: int = 2000,
    ci_levels: list[float] | None = None,
    seed: int = 42,
) -> dict:
    """レース単位で Bootstrap リサンプリングし、RR の分布を推定。

    Returns:
        dict with keys: mean, median, std, ci_lower, ci_upper, p_value_ge_1,
                        rr_distribution (array)
    """
    if ci_levels is None:
        ci_levels = [0.025, 0.975]

    rng = np.random.RandomState(seed)
    n_races = len(per_race_returns)
    if n_races == 0:
        return {"mean": 0, "median": 0, "std": 0,
                "ci_lower": 0, "ci_upper": 0, "p_value_lt_1": 1.0,
                "rr_distribution": np.array([])}

    bets = per_race_returns["bet_amount"].values
    returns = per_race_returns["return_amount"].values

    boot_rrs = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.randint(0, n_races, size=n_races)
        total_bet = bets[idx].sum()
        total_ret = returns[idx].sum()
        boot_rrs[i] = total_ret / total_bet if total_bet > 0 else 0.0

    ci_lo, ci_hi = np.percentile(boot_rrs, [ci_levels[0] * 100, ci_levels[1] * 100])
    p_lt_1 = (boot_rrs < 1.0).mean()  # P(RR < 1.0)

    return {
        "mean": float(np.mean(boot_rrs)),
        "median": float(np.median(boot_rrs)),
        "std": float(np.std(boot_rrs)),
        "ci_lower": float(ci_lo),
        "ci_upper": float(ci_hi),
        "p_value_lt_1": float(p_lt_1),
        "rr_distribution": boot_rrs,
    }


# ---------------------------------------------------------------------------
# Monthly breakdown
# ---------------------------------------------------------------------------

def monthly_breakdown(
    per_race_returns: pd.DataFrame,
    race_dates: pd.Series,
) -> pd.DataFrame:
    """月別の RR, n_bets, n_hits, profit を集計。"""
    rpr = per_race_returns.copy()
    dates = race_dates.reindex(rpr.index)
    rpr["month"] = pd.to_datetime(dates).dt.to_period("M")

    grouped = rpr.groupby("month").agg(
        n_races=("bet_amount", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    grouped["return_rate"] = grouped["total_return"] / grouped["total_bet"]
    grouped["profit"] = grouped["total_return"] - grouped["total_bet"]
    return grouped.sort_index()


# ---------------------------------------------------------------------------
# Venue breakdown
# ---------------------------------------------------------------------------

def venue_breakdown(
    per_race_returns: pd.DataFrame,
) -> pd.DataFrame:
    """場別の RR, n_bets, n_hits, profit を集計。"""
    rpr = per_race_returns.copy()
    # race_id is in the index
    rpr["venue_code"] = [str(rid)[4:6] for rid in rpr.index]
    rpr["venue_name"] = rpr["venue_code"].map(VENUE_NAMES).fillna("不明")

    grouped = rpr.groupby(["venue_code", "venue_name"]).agg(
        n_races=("bet_amount", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    grouped["return_rate"] = grouped["total_return"] / grouped["total_bet"]
    grouped["profit"] = grouped["total_return"] - grouped["total_bet"]
    return grouped.sort_index()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="P8-1: Bootstrap CI + Fine-grain Edge Sweep")
    ap.add_argument("--hpo-params", default=HPO_PARAMS_PATH)
    ap.add_argument("--featured", default=FEATURED_PATH)
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--th-start", type=float, default=0.120)
    ap.add_argument("--th-end", type=float, default=0.155)
    ap.add_argument("--th-step", type=float, default=0.001)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    import datetime
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd, "p8")
    os.makedirs(out_dir, exist_ok=True)

    t_total = time.time()

    # --- Load HPO params ---
    print(f"Loading HPO params: {args.hpo_params}")
    with open(args.hpo_params) as f:
        hpo_params = json.load(f)
    print(f"  {hpo_params}")

    # --- Load data ---
    print(f"\nLoading featured data: {args.featured}")
    fd = pd.read_pickle(args.featured)
    fd = _ensure_win_column(fd)
    print(f"  shape: {fd.shape}")

    test_start = args.test_start
    dates = pd.to_datetime(fd["date"])
    test_df = fd[dates >= pd.Timestamp(test_start)]
    train_df = fd[dates < pd.Timestamp(test_start)]
    print(f"  train: {len(train_df):,}  test: {len(test_df):,}")

    # --- Train model with HPO params ---
    print("\n" + "=" * 60)
    print("Phase 1: Train win model with HPO params")
    print("=" * 60)
    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)
    ds = WinDataSplitter(combined, test_size=test_ratio, valid_size=args.valid_ratio)

    keiba_ai = training.KeibaAI(ds)
    sklearn_params = _convert_to_sklearn_params(hpo_params)
    keiba_ai.set_params(sklearn_params)
    print("  training...")
    keiba_ai.train_without_tuning()

    # Score on test
    X_test = ds.X_test
    y_test = ds.y_test
    raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)
    auc_test = roc_auc_score(y_test, raw_scores)
    print(f"  AUC(test): {auc_test:.4f}")

    tansho_odds = ds.tansho_odds_test

    # Build return table accessor
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # Build race_id -> date mapping from test data
    test_race_dates = test_df.groupby("race_id")["date"].first()

    # --- Phase 2: Fine-grain sweep ---
    print("\n" + "=" * 60)
    print(f"Phase 2: Fine-grain edge sweep (th={args.th_start:.3f}~{args.th_end:.3f}, step={args.th_step:.3f})")
    print("=" * 60)
    t1 = time.time()
    fg_df = fine_grain_sweep(
        raw_score_table, tansho_odds, sim,
        th_start=args.th_start, th_end=args.th_end, th_step=args.th_step,
    )
    print(f"  sweep time: {time.time()-t1:.1f}s")
    print(f"  {len(fg_df)} thresholds evaluated")

    # Show best region
    valid = fg_df[fg_df["n_bets"] >= 50].copy()
    if len(valid) > 0:
        best_idx = valid["return_rate"].idxmax()
        best = valid.loc[best_idx]
        print(f"\n  Best (n>=50): th={best['threshold']:.3f}  RR={best['return_rate']:.4f}  "
              f"n={int(best['n_bets'])}  hits={int(best['n_hits'])}  profit={int(best['profit'])}円")

    # Show RR>1.0 region
    above1 = valid[valid["return_rate"] >= 1.0]
    if len(above1) > 0:
        print(f"\n  RR >= 1.0 region ({len(above1)} thresholds):")
        for _, row in above1.iterrows():
            print(f"    th={row['threshold']:.3f}  RR={row['return_rate']:.4f}  "
                  f"n={int(row['n_bets'])}  profit={int(row['profit'])}円")
    else:
        print("\n  ⚠ No threshold with RR >= 1.0 (n>=50)")

    # Show th=0.130-0.145 detail
    print(f"\n  Detailed curve (th=0.130~0.145):")
    detail_mask = (fg_df["threshold"] >= 0.130) & (fg_df["threshold"] <= 0.145)
    for _, row in fg_df[detail_mask].iterrows():
        marker = " ★" if row["return_rate"] >= 1.0 and row["n_bets"] >= 50 else ""
        print(f"    th={row['threshold']:.3f}  RR={row['return_rate']:.4f}  "
              f"n={int(row['n_bets'])}  hits={int(row['n_hits'])}  "
              f"profit={int(row['profit'])}円{marker}")

    # Save fine-grain results
    fg_df.to_pickle(os.path.join(out_dir, "fine_grain_edge_sweep.pickle"))
    fg_df.to_csv(os.path.join(out_dir, "fine_grain_edge_sweep.csv"), index=False)

    # --- Phase 3: Bootstrap CI at multiple thresholds ---
    print("\n" + "=" * 60)
    print(f"Phase 3: Bootstrap CI ({args.n_boot} resamples, race-level)")
    print("=" * 60)
    t2 = time.time()

    # Bootstrap at key thresholds
    boot_thresholds = sorted(set(
        list(np.arange(0.130, 0.150 + 0.001, 0.002))  # fine around peak
        + [0.120, 0.125, 0.135, 0.138, 0.140, 0.145, 0.150]
    ))
    boot_results = []

    for th in boot_thresholds:
        rpr = get_per_race_returns(raw_score_table, tansho_odds, sim, float(th))
        if len(rpr) == 0:
            continue
        observed_rr = rpr["return_amount"].sum() / rpr["bet_amount"].sum()
        n_bets = int(rpr["n_bets"].sum())
        n_hits = int(rpr["hit_or_not"].sum())
        n_races = len(rpr)
        profit = int(rpr["return_amount"].sum() - rpr["bet_amount"].sum())

        ci = bootstrap_ci(rpr, n_boot=args.n_boot, seed=args.seed)

        boot_results.append({
            "threshold": float(th),
            "observed_rr": observed_rr,
            "n_bets": n_bets,
            "n_hits": n_hits,
            "n_races": n_races,
            "profit": profit,
            "boot_mean": ci["mean"],
            "boot_median": ci["median"],
            "boot_std": ci["std"],
            "ci_lower_2.5": ci["ci_lower"],
            "ci_upper_97.5": ci["ci_upper"],
            "p_lt_1": ci["p_value_lt_1"],
        })

        sig = "✓" if ci["ci_lower"] > 1.0 else ("~" if ci["p_value_lt_1"] < 0.05 else "✗")
        print(f"  th={th:.3f}: RR={observed_rr:.4f}  n={n_bets}  "
              f"95%CI=[{ci['ci_lower']:.4f}, {ci['ci_upper']:.4f}]  "
              f"P(RR<1)={ci['p_value_lt_1']:.3f}  {sig}")

    boot_df = pd.DataFrame(boot_results)
    print(f"\n  Bootstrap time: {time.time()-t2:.1f}s")

    # Save bootstrap results
    boot_df.to_pickle(os.path.join(out_dir, "bootstrap_ci.pickle"))
    boot_df.to_csv(os.path.join(out_dir, "bootstrap_ci.csv"), index=False)

    # Save full distribution at th=0.138
    rpr_peak = get_per_race_returns(raw_score_table, tansho_odds, sim, 0.138)
    if len(rpr_peak) > 0:
        ci_peak = bootstrap_ci(rpr_peak, n_boot=args.n_boot, seed=args.seed)
        with open(os.path.join(out_dir, "bootstrap_dist_th0.138.pickle"), "wb") as f:
            pickle.dump(ci_peak, f)

    # --- Phase 4: Monthly breakdown ---
    print("\n" + "=" * 60)
    print("Phase 4: Monthly breakdown (th=0.138)")
    print("=" * 60)

    if len(rpr_peak) > 0:
        monthly = monthly_breakdown(rpr_peak, test_race_dates)
        print(monthly.to_string())
        monthly.to_csv(os.path.join(out_dir, "monthly_breakdown.csv"))

        # Consecutive win/loss streaks
        monthly_rr = monthly["return_rate"]
        n_profit_months = (monthly_rr >= 1.0).sum()
        n_loss_months = (monthly_rr < 1.0).sum()
        print(f"\n  Profit months: {n_profit_months}/{len(monthly_rr)}  "
              f"Loss months: {n_loss_months}/{len(monthly_rr)}")
    else:
        print("  No bets at th=0.138")

    # --- Phase 5: Venue breakdown ---
    print("\n" + "=" * 60)
    print("Phase 5: Venue breakdown (th=0.138)")
    print("=" * 60)

    if len(rpr_peak) > 0:
        venue = venue_breakdown(rpr_peak)
        print(venue.to_string())
        venue.to_csv(os.path.join(out_dir, "venue_breakdown.csv"))
    else:
        print("  No bets at th=0.138")

    # --- Phase 6: Additional analysis at plateau region ---
    print("\n" + "=" * 60)
    print("Phase 6: Plateau analysis (th=0.134~0.144)")
    print("=" * 60)

    plateau_ths = np.arange(0.134, 0.145, 0.002)
    for th in plateau_ths:
        rpr_th = get_per_race_returns(raw_score_table, tansho_odds, sim, float(th))
        if len(rpr_th) == 0:
            print(f"  th={th:.3f}: no bets")
            continue
        ci_th = bootstrap_ci(rpr_th, n_boot=args.n_boot, seed=args.seed)
        observed = rpr_th["return_amount"].sum() / rpr_th["bet_amount"].sum()
        n = int(rpr_th["n_bets"].sum())
        # Monthly stability: std of monthly RR
        m_th = monthly_breakdown(rpr_th, test_race_dates)
        monthly_std = m_th["return_rate"].std() if len(m_th) > 1 else float("nan")
        # Venue concentration: max venue share
        v_th = venue_breakdown(rpr_th)
        max_venue_share = v_th["n_bets"].max() / v_th["n_bets"].sum() if len(v_th) > 0 else 0

        print(f"  th={th:.3f}: RR={observed:.4f} n={n} "
              f"CI=[{ci_th['ci_lower']:.4f},{ci_th['ci_upper']:.4f}] "
              f"P(RR<1)={ci_th['p_value_lt_1']:.3f} "
              f"月RR_std={monthly_std:.3f} "
              f"場集中={max_venue_share:.2f}")

    # --- Summary ---
    total_time = time.time() - t_total
    print("\n" + "=" * 70)
    print("P8-1 SUMMARY")
    print("=" * 70)
    print(f"  AUC(test): {auc_test:.4f}")
    print(f"  Fine-grain: {len(fg_df)} thresholds ({args.th_start:.3f}~{args.th_end:.3f})")
    print(f"  Bootstrap:  {args.n_boot} resamples, {len(boot_results)} thresholds")

    if len(boot_df) > 0:
        peak = boot_df.loc[boot_df["observed_rr"].idxmax()]
        print(f"\n  Peak: th={peak['threshold']:.3f}  RR={peak['observed_rr']:.4f}  "
              f"n={int(peak['n_bets'])}")
        print(f"    95% CI: [{peak['ci_lower_2.5']:.4f}, {peak['ci_upper_97.5']:.4f}]")
        print(f"    P(RR<1): {peak['p_lt_1']:.4f}")
        print(f"    Bootstrap mean: {peak['boot_mean']:.4f}")

        # Recommendation
        if peak["ci_lower_2.5"] > 1.0:
            print(f"\n  ✅ CONCLUSION: 95% CI fully above 1.0 → 統計的に有意 (p<0.025)")
            print(f"     → P8-2 (本番組み込み) 推奨")
        elif peak["p_lt_1"] < 0.05:
            print(f"\n  ⚠ CONCLUSION: P(RR<1)={peak['p_lt_1']:.3f} < 0.05 → 有意だが CI下限<1.0")
            print(f"     → P8-2 条件付き推奨 (リスク認識の上で)")
        else:
            print(f"\n  ✗ CONCLUSION: P(RR<1)={peak['p_lt_1']:.3f} >= 0.05 → 統計的に有意でない")
            print(f"     → 本番投入は見送り推奨")

    print(f"\n  Total time: {total_time:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
