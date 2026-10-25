"""校正(Calibration) × 窓長 × ポリシー の総合sweep。

校正手法 (none / platt / isotonic) × 窓長 (5y / 7y / all) の各組み合わせで
単勝EV sweep・複勝 threshold sweep・Edge-based sweep を実行し、
n_bets >= min_bets を満たす範囲でのベスト回収率を比較する。

出力:
  models/YYYYMMDD/calibration_sweep_summary.pickle
  models/YYYYMMDD/calibration_sweep_detail/  (各組み合わせのsweep結果)

Usage:
  .venv/Scripts/python.exe tools/run_calibration_sweep.py
  .venv/Scripts/python.exe tools/run_calibration_sweep.py --min-bets 500
  .venv/Scripts/python.exe tools/run_calibration_sweep.py --windows 5,7,all --calibrations none,platt,isotonic
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._calibration import fit_calibrator
from modules.training._data_splitter import DataSplitter
from modules.training._win_data_splitter import WinDataSplitter


# ---------------------------------------------------------------------------
# Helpers (borrowed from run_window_comparison.py)
# ---------------------------------------------------------------------------

def _linspace_inclusive(start: float, end: float, n: int) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
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


def _filter_by_date(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    dates = pd.to_datetime(df["date"])
    mask = pd.Series(True, index=df.index)
    if start is not None:
        mask &= dates >= pd.Timestamp(start)
    if end is not None:
        mask &= dates <= pd.Timestamp(end)
    return df.loc[mask]


def _compute_train_start(test_start: str, window_years: int) -> str:
    ts = pd.Timestamp(test_start)
    return (ts - pd.DateOffset(years=window_years)).strftime("%Y-%m-%d")


def _ensure_win_column(fd: pd.DataFrame) -> pd.DataFrame:
    """featured_data に ``win`` カラムがなければ raw results から復元する。"""
    if "win" in fd.columns:
        return fd

    raw_path = os.path.join(REPO_ROOT, "data", "raw", "results.pickle.bak")
    if not os.path.isfile(raw_path):
        raise FileNotFoundError(f"raw results not found: {raw_path}")

    print("  Reconstructing 'win' column from raw results...")
    results = pd.read_pickle(raw_path)

    rank_num = pd.to_numeric(results["着順"], errors="coerce")
    umaban_num = pd.to_numeric(results["馬番"], errors="coerce")
    race_ids = results.index

    win_df = pd.DataFrame({
        "race_id": race_ids,
        "馬番": umaban_num,
        "win": (rank_num == 1).astype("int8"),
    }).dropna(subset=["馬番", "win"])
    win_df["馬番"] = win_df["馬番"].astype(int)

    # race_id 列の型を合わせる
    if "race_id" in fd.columns:
        win_df["race_id"] = win_df["race_id"].astype(fd["race_id"].dtype)
    fd_umaban = fd["馬番"]
    if fd_umaban.dtype != win_df["馬番"].dtype:
        win_df["馬番"] = win_df["馬番"].astype(fd_umaban.dtype)

    win_df = win_df.drop_duplicates(subset=["race_id", "馬番"], keep="first")
    n_before = len(fd)
    fd = fd.merge(win_df[["race_id", "馬番", "win"]], on=["race_id", "馬番"], how="left")
    assert len(fd) == n_before, f"Merge row explosion: {n_before} -> {len(fd)}"
    fd["win"] = fd["win"].fillna(0).astype("int8")
    print(f"  win column added: 1={int((fd['win']==1).sum()):,}  0={int((fd['win']==0).sum()):,}")
    return fd


# ---------------------------------------------------------------------------
# Edge-based bet policy (P3 新規)
# ---------------------------------------------------------------------------

def _judge_edge_tansho(
    score_table: pd.DataFrame,
    tansho_odds: pd.Series,
    edge_threshold: float,
    max_bets_per_race: int = 2,
    min_odds: float | None = None,
) -> dict:
    """Edge = calibrated_p - (1/odds) >= edge_threshold の馬に単勝購入。

    calibrated_p はスコアテーブルのscoreをそのまま（校正後の確率として）使用。
    """
    table = score_table.copy()

    # odds をセット
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

    if min_odds is not None:
        table = table[table[odds_col] >= float(min_odds)]

    # Edge = p - 1/odds
    table["edge"] = table["score"] - (1.0 / table[odds_col])
    table = table[table["edge"] >= edge_threshold]

    if table.empty:
        return {}

    # レース単位で上位K頭に制限
    if max_bets_per_race is not None:
        k = int(max_bets_per_race)
        if "race_id" in table.columns:
            table = (
                table.sort_values(["race_id", "edge"], ascending=[True, False])
                .groupby("race_id", sort=False)
                .head(k)
            )
        else:
            race_keys = table.index.get_level_values(0)
            table = (
                table.assign(_rk=race_keys)
                .sort_values(["_rk", "edge"], ascending=[True, False])
                .groupby("_rk", sort=False)
                .head(k)
                .drop(columns=["_rk"])
            )

    if "race_id" in table.columns:
        bet_df = table.groupby("race_id")[ResultsCols.UMABAN].apply(list).to_frame()
    else:
        bet_df = table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

    def _to_int_list(xs):
        s = pd.to_numeric(pd.Series(xs), errors="coerce")
        return s.dropna().astype(int).drop_duplicates().tolist()

    bet_df[ResultsCols.UMABAN] = bet_df[ResultsCols.UMABAN].apply(_to_int_list)
    bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) > 0]
    return bet_df.rename(columns={ResultsCols.UMABAN: "tansho"}).T.to_dict()


# ---------------------------------------------------------------------------
# Single combination runner
# ---------------------------------------------------------------------------

def run_combination(
    featured_data: pd.DataFrame,
    train_start: str | None,
    test_start: str,
    test_end: str | None,
    valid_ratio: float,
    base_params: dict | None,
    window_label: str,
    calibration: str,  # "none", "platt", "isotonic"
    out_dir: str,
    max_bets_per_race_tansho: int = 2,
    cap_fukusho: int = 3,
    n_sweep: int = 100,
    min_bets: int = 500,
    target: str = "rank",  # "rank" (3着以内) or "win" (1着)
) -> dict:
    """1つの (窓長, 校正) 組み合わせについて全sweep実行。"""

    combo_label = f"{window_label}_{calibration}"
    print(f"\n{'='*60}")
    print(f"Combo: {combo_label}  (train_start={train_start}, calibration={calibration})")
    print(f"{'='*60}")

    t0 = time.time()

    # --- 1) train/test split ---
    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, train_start, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")
    if len(train_df) == 0 or len(test_df) == 0:
        print("  SKIP: no data")
        return {"combo": combo_label, "error": "no data"}

    # --- 2) DataSplitter + train ---
    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    if target == "win":
        ds = WinDataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)
    else:
        ds = DataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)
    keiba_ai = training.KeibaAI(ds)
    if base_params is not None:
        keiba_ai.set_params(base_params)

    print("  training...")
    keiba_ai.train_without_tuning()

    # --- 3) Basic score (raw probability) ---
    from sklearn.metrics import roc_auc_score

    X_test = ds.X_test
    y_test = ds.y_test
    raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)

    try:
        auc_test = roc_auc_score(y_test, raw_scores)
    except Exception:
        auc_test = float("nan")
    print(f"  AUC test={auc_test:.4f}")

    # --- 4) Calibration ---
    calibrated_scores = raw_scores.copy()
    if calibration != "none":
        print(f"  fitting calibrator: {calibration} (on validation set)...")
        # validation set で calibrator を fit する (public property で取得)
        X_valid = ds.X_valid
        y_valid = ds.y_valid

        valid_score_table = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
        valid_raw = pd.to_numeric(valid_score_table["score"], errors="coerce").astype(float)

        calibrator = fit_calibrator(calibration, y_valid.values, valid_raw.values)
        calibrated_scores = pd.Series(
            calibrator(raw_scores.values), index=raw_scores.index
        )

        # calibrated AUC
        try:
            auc_calibrated = roc_auc_score(y_test, calibrated_scores)
        except Exception:
            auc_calibrated = float("nan")
        print(f"  AUC calibrated={auc_calibrated:.4f}")
    else:
        auc_calibrated = auc_test

    # スコアテーブルを校正後に更新
    cal_score_table = raw_score_table.copy()
    cal_score_table["score"] = calibrated_scores.values

    # --- 5) Tansho odds ---
    tansho_odds = None
    try:
        test_data = ds.test_data
        if ResultsCols.TANSHO_ODDS in test_data.columns:
            tansho_odds = test_data[ResultsCols.TANSHO_ODDS]
    except Exception:
        pass
    if tansho_odds is None:
        tansho_odds = ds.tansho_odds_test

    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # --- 6a) 単勝EV sweep (z-score on calibrated) ---
    print("  tansho EV sweep...")
    score_std = _standardize_within_race(cal_score_table)
    margins = _linspace_inclusive(0.0, 0.5, n_sweep)
    tansho_ev_results = {}
    for margin in margins:
        actions = keiba_ai.decide_action(
            score_std, policies.BetPolicyEVTansho,
            margin=float(margin), tansho_odds=tansho_odds,
            max_bets_per_race=max_bets_per_race_tansho,
        )
        tansho_ev_results[float(margin)] = sim.calc_returns(actions)

    tansho_ev_df = pd.DataFrame.from_dict(tansho_ev_results, orient="index").sort_index()
    tansho_ev_df.index.name = "margin"

    # --- 6b) 複勝 threshold sweep (z-score on calibrated) ---
    print("  fukusho threshold sweep...")
    thresholds = _linspace_inclusive(0.0, 3.5, n_sweep)
    fukusho_results = {}
    for th in thresholds:
        actions = keiba_ai.decide_action(
            score_std, policies.BetPolicyFukusho,
            threshold=float(th), max_bets_per_race=cap_fukusho,
        )
        fukusho_results[float(th)] = sim.calc_returns(actions)

    fukusho_df = pd.DataFrame.from_dict(fukusho_results, orient="index").sort_index()
    fukusho_df.index.name = "threshold"

    # --- 6c) Edge-based tansho sweep (raw calibrated probability) ---
    print("  edge tansho sweep...")
    edge_thresholds = _linspace_inclusive(-0.05, 0.15, n_sweep)
    edge_results = {}
    for edge_th in edge_thresholds:
        actions = _judge_edge_tansho(
            cal_score_table, tansho_odds,
            edge_threshold=float(edge_th),
            max_bets_per_race=max_bets_per_race_tansho,
        )
        edge_results[float(edge_th)] = sim.calc_returns(actions)

    edge_df = pd.DataFrame.from_dict(edge_results, orient="index").sort_index()
    edge_df.index.name = "edge_threshold"

    # --- 6d) 複勝EV sweep (score * tansho_odds >= threshold, bet fukusho) ---
    print("  fukusho EV sweep...")
    fukusho_ev_thresholds = _linspace_inclusive(0.5, 5.0, n_sweep)
    fukusho_ev_results = {}
    for th in fukusho_ev_thresholds:
        actions = keiba_ai.decide_action(
            cal_score_table, policies.BetPolicyEVFukusho,
            threshold=float(th), tansho_odds=tansho_odds,
            max_bets_per_race=cap_fukusho,
        )
        fukusho_ev_results[float(th)] = sim.calc_returns(actions)

    fukusho_ev_df = pd.DataFrame.from_dict(fukusho_ev_results, orient="index").sort_index()
    fukusho_ev_df.index.name = "threshold"

    train_time = time.time() - t0

    # --- 7) Save detail pickles ---
    detail_dir = os.path.join(out_dir, "calibration_sweep_detail")
    os.makedirs(detail_dir, exist_ok=True)
    tansho_ev_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_tansho_ev.pickle"))
    fukusho_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_fukusho.pickle"))
    edge_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_edge.pickle"))
    fukusho_ev_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_fukusho_ev.pickle"))

    # --- 8) Best within n_bets >= min_bets ---
    def _pick_best(df, min_n):
        if "n_bets" in df.columns:
            reliable = df[df["n_bets"] >= min_n]
        else:
            reliable = df
        if len(reliable) == 0:
            return float("nan"), float("nan"), 0, float("nan")
        idx = reliable["return_rate"].idxmax()
        row = reliable.loc[idx]
        return idx, row["return_rate"], row.get("n_bets", 0), row.get("std", float("nan"))

    te_param, te_rr, te_n, te_std = _pick_best(tansho_ev_df, min_bets)
    fk_param, fk_rr, fk_n, fk_std = _pick_best(fukusho_df, min_bets)
    ed_param, ed_rr, ed_n, ed_std = _pick_best(edge_df, min_bets)
    fe_param, fe_rr, fe_n, fe_std = _pick_best(fukusho_ev_df, min_bets)

    print(f"  tansho_ev best  (n>={min_bets}): margin={te_param}  RR={te_rr:.4f}  n={te_n}")
    print(f"  fukusho best    (n>={min_bets}): th={fk_param}  RR={fk_rr:.4f}  n={fk_n}")
    print(f"  fukusho_ev best (n>={min_bets}): th={fe_param}  RR={fe_rr:.4f}  n={fe_n}")
    print(f"  edge best       (n>={min_bets}): edge_th={ed_param}  RR={ed_rr:.4f}  n={ed_n}")
    print(f"  total time: {train_time:.1f}s")

    return {
        "combo": combo_label,
        "window": window_label,
        "calibration": calibration,
        "train_start": train_start,
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "auc_test": auc_test,
        "auc_calibrated": auc_calibrated,
        # tansho EV
        "tansho_ev_margin": te_param,
        "tansho_ev_rr": te_rr,
        "tansho_ev_n_bets": te_n,
        "tansho_ev_std": te_std,
        # fukusho
        "fukusho_th": fk_param,
        "fukusho_rr": fk_rr,
        "fukusho_n_bets": fk_n,
        "fukusho_std": fk_std,
        # fukusho EV
        "fukusho_ev_th": fe_param,
        "fukusho_ev_rr": fe_rr,
        "fukusho_ev_n_bets": fe_n,
        "fukusho_ev_std": fe_std,
        # edge
        "edge_th": ed_param,
        "edge_rr": ed_rr,
        "edge_n_bets": ed_n,
        "edge_std": ed_std,
        # meta
        "target": target,
        "train_time_sec": train_time,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="校正×窓長 総合sweep")
    ap.add_argument("--featured", default="data/tmp/featured_data_20260205.pickle")
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--windows", default="5,7,all",
                    help="比較窓長 (カンマ区切り, 'all'=全期間)")
    ap.add_argument("--calibrations", default="none,platt,isotonic",
                    help="校正手法 (カンマ区切り)")
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--base-model", default=None)
    ap.add_argument("--max-bets-tansho", type=int, default=2)
    ap.add_argument("--cap-fukusho", type=int, default=3)
    ap.add_argument("--n-sweep", type=int, default=100)
    ap.add_argument("--min-bets", type=int, default=500)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--target", default="rank", choices=["rank", "win"],
                    help="Target variable: 'rank' (3着以内) or 'win' (1着)")
    args = ap.parse_args()

    # Load data
    print("loading featured_data:", args.featured)
    featured_data = pd.read_pickle(args.featured)
    print(f"  shape: {featured_data.shape}")
    dates = pd.to_datetime(featured_data["date"])
    data_min_date = dates.min().strftime("%Y-%m-%d")
    print(f"  date range: {data_min_date} ~ {dates.max().strftime('%Y-%m-%d')}")

    # win column reconstruction (for --target win)
    if args.target == "win":
        featured_data = _ensure_win_column(featured_data)

    # Base params
    base_params = None
    if args.base_model and os.path.isfile(args.base_model):
        try:
            tmp = training.KeibaAIFactory.load(args.base_model)
            base_params = tmp.get_params()
            print(f"  base params loaded from: {args.base_model}")
        except Exception as e:
            print(f"  base params load failed: {e}")

    # Output dir
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd)
    os.makedirs(out_dir, exist_ok=True)

    # Parse windows
    windows_raw = [w.strip() for w in args.windows.split(",")]
    window_specs = []
    for w in windows_raw:
        if w.lower() == "all":
            window_specs.append(("all", data_min_date))
        else:
            years = int(w)
            ts = _compute_train_start(args.test_start, years)
            if pd.Timestamp(ts) < pd.Timestamp(data_min_date):
                ts = data_min_date
            window_specs.append((f"{years}y", ts))

    calibrations = [c.strip() for c in args.calibrations.split(",")]

    total = len(window_specs) * len(calibrations)
    print(f"\nWindows: {[w[0] for w in window_specs]}")
    print(f"Calibrations: {calibrations}")
    print(f"Target: {args.target}")
    print(f"Total combos: {total}")
    print(f"min_bets filter: {args.min_bets}")

    # Run all combinations
    summaries = []
    idx = 0
    for window_label, train_start in window_specs:
        for cal in calibrations:
            idx += 1
            print(f"\n[{idx}/{total}]")
            result = run_combination(
                featured_data=featured_data,
                train_start=train_start,
                test_start=args.test_start,
                test_end=args.test_end,
                valid_ratio=args.valid_ratio,
                base_params=base_params,
                window_label=window_label,
                calibration=cal,
                out_dir=out_dir,
                max_bets_per_race_tansho=args.max_bets_tansho,
                cap_fukusho=args.cap_fukusho,
                n_sweep=args.n_sweep,
                min_bets=args.min_bets,
                target=args.target,
            )
            summaries.append(result)

    # Summary table
    summary_df = pd.DataFrame(summaries)
    summary_path = os.path.join(out_dir, "calibration_sweep_summary.pickle")
    summary_df.to_pickle(summary_path)

    print(f"\n{'='*80}")
    print(f"SUMMARY saved: {summary_path}")
    print(f"{'='*80}")

    show_cols = [
        "combo", "auc_test", "auc_calibrated",
        "tansho_ev_rr", "tansho_ev_n_bets", "tansho_ev_std",
        "fukusho_rr", "fukusho_n_bets", "fukusho_std",
        "edge_rr", "edge_n_bets", "edge_std",
        "train_time_sec",
    ]
    show_cols = [c for c in show_cols if c in summary_df.columns]
    print(summary_df[show_cols].to_string(index=False))

    # Overall best
    print(f"\n--- Overall Best (n_bets >= {args.min_bets}) ---")
    for policy in ["tansho_ev", "fukusho", "fukusho_ev", "edge"]:
        rr_col = f"{policy}_rr"
        n_col = f"{policy}_n_bets"
        if rr_col not in summary_df.columns:
            continue
        valid = summary_df[summary_df[n_col] >= args.min_bets]
        if len(valid) == 0:
            print(f"  {policy}: no combo with n_bets >= {args.min_bets}")
            continue
        best_idx = valid[rr_col].idxmax()
        best = valid.loc[best_idx]
        print(f"  {policy}: {best['combo']}  RR={best[rr_col]:.4f}  n={best[n_col]:.0f}  std={best.get(f'{policy}_std', float('nan')):.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
