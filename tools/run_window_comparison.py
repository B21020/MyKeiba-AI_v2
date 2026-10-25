"""学習窓長（5/7/10年/全期間）を比較するスクリプト。

同一テスト期間を固定し、学習データの窓長だけを変えて
AUC・単勝EV回収率・複勝回収率を比較する。

出力:
  models/YYYYMMDD/window_comparison_summary.pickle
  models/YYYYMMDD/window_<N>y_tansho_ev.pickle  (各窓長の単勝EV sweep)
  models/YYYYMMDD/window_<N>y_fukusho_th.pickle  (各窓長の複勝threshold sweep)

Usage:
  .venv/Scripts/python.exe tools/run_window_comparison.py
  .venv/Scripts/python.exe tools/run_window_comparison.py --test-start 2024-07-01
  .venv/Scripts/python.exe tools/run_window_comparison.py --windows 5,7,10,all
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
from modules.training._data_splitter import DataSplitter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _linspace_inclusive(start: float, end: float, n: int) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
    """StdScorePolicy 相当のレース内 z-score 標準化。"""
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


def _attach_win_label(df: pd.DataFrame, results_raw: pd.DataFrame) -> pd.DataFrame:
    """featured_data に win ラベル(1着=1)を付与する。"""
    if "win" in df.columns:
        return df

    rp = preprocessing.ResultsProcessor(filepath=None, raw_data=results_raw)
    results = rp.preprocessed_data
    needed = ["race_id", ResultsCols.UMABAN, "win"]
    missing = [c for c in needed if c not in results.columns]
    if missing:
        print(f"WARNING: results missing {missing}, skipping win label")
        return df

    res = results.loc[:, needed].copy()
    df2 = df.copy()
    if "race_id" not in df2.columns:
        df2["race_id"] = df2.index.astype(str)

    df2["race_id"] = df2["race_id"].astype(str)
    res["race_id"] = res["race_id"].astype(str)
    df2[ResultsCols.UMABAN] = pd.to_numeric(df2[ResultsCols.UMABAN], errors="coerce")
    res[ResultsCols.UMABAN] = pd.to_numeric(res[ResultsCols.UMABAN], errors="coerce")

    merged = df2.merge(res, on=["race_id", ResultsCols.UMABAN], how="left")
    merged["win"] = pd.to_numeric(merged["win"], errors="coerce").fillna(0).astype(int)
    return merged


def _filter_by_date(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    """date 列で行をフィルタする（start/end は inclusive）。"""
    dates = pd.to_datetime(df["date"])
    mask = pd.Series(True, index=df.index)
    if start is not None:
        mask &= dates >= pd.Timestamp(start)
    if end is not None:
        mask &= dates <= pd.Timestamp(end)
    return df.loc[mask]


def _compute_train_start(test_start: str, window_years: int) -> str:
    """テスト開始日から window_years 年遡った日付を返す。"""
    ts = pd.Timestamp(test_start)
    return (ts - pd.DateOffset(years=window_years)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# 窓長別の学習・評価
# ---------------------------------------------------------------------------

def run_single_window(
    featured_data: pd.DataFrame,
    train_start: str | None,
    test_start: str,
    test_end: str | None,
    valid_ratio: float,
    base_params: dict | None,
    label: str,
    out_dir: str,
    max_bets_per_race_tansho: int = 2,
    cap_fukusho: int = 3,
    margin_start: float = 0.0,
    margin_end: float = 0.5,
    n_margins: int = 100,
) -> dict:
    """1 つの窓長で学習→EV sweep→複勝 sweep を実行し、結果 dict を返す。"""

    print(f"\n{'='*60}")
    print(f"Window: {label}  (train_start={train_start}, test_start={test_start})")
    print(f"{'='*60}")

    t0 = time.time()

    # --- 1) train / test 分割 ---
    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, train_start, test_start)
    # test_start 当日は test に含める、train からは除外
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")
    if len(train_df) == 0 or len(test_df) == 0:
        print("  SKIP: no data")
        return {"label": label, "error": "no data"}

    # train の年分布
    train_years = pd.to_datetime(train_df["date"]).dt.year.value_counts().sort_index()
    print(f"  train years: {train_years.index.min()}..{train_years.index.max()} ({len(train_years)} years)")
    test_years = pd.to_datetime(test_df["date"]).dt.year.value_counts().sort_index()
    print(f"  test years: {test_years.index.min()}..{test_years.index.max()}")

    # --- 2) 結合して DataSplitter に渡す ---
    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)
    print(f"  test_ratio: {test_ratio:.4f}  valid_ratio: {valid_ratio}")

    ds = DataSplitter(
        combined,
        test_size=test_ratio,
        valid_size=valid_ratio,
    )
    keiba_ai = training.KeibaAI(ds)

    # パラメータ固定
    if base_params is not None:
        keiba_ai.set_params(base_params)

    # --- 3) 学習 ---
    print("  training...")
    keiba_ai.train_without_tuning()
    train_time = time.time() - t0

    # --- 4) AUC ---
    from sklearn.metrics import roc_auc_score

    y_test = ds.y_test
    X_test = ds.X_test
    score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    # rank は既に binary (0/1: 3着以内=1) なのでそのまま使う
    try:
        auc_test = roc_auc_score(y_test, score_table["score"])
    except Exception:
        auc_test = float("nan")

    y_train = ds.y_train
    X_train = ds.X_train
    score_train = keiba_ai.calc_score(X_train, policies.BasicScorePolicy)
    try:
        auc_train = roc_auc_score(y_train, score_train["score"])
    except Exception:
        auc_train = float("nan")

    print(f"  AUC train={auc_train:.4f}  test={auc_test:.4f}  time={train_time:.1f}s")

    # --- 5) 単勝 EV sweep ---
    print("  running tansho EV sweep...")
    score_table_std = _standardize_within_race(score_table)

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

    margins = _linspace_inclusive(margin_start, margin_end, n_margins)
    tansho_results: dict[float, dict] = {}
    for margin in margins:
        actions = keiba_ai.decide_action(
            score_table_std,
            policies.BetPolicyEVTansho,
            margin=float(margin),
            tansho_odds=tansho_odds,
            max_bets_per_race=max_bets_per_race_tansho,
        )
        tansho_results[float(margin)] = sim.calc_returns(actions)

    tansho_df = pd.DataFrame.from_dict(tansho_results, orient="index").sort_index()
    tansho_df.index.name = "margin"
    tansho_path = os.path.join(out_dir, f"window_{label}_tansho_ev.pickle")
    tansho_df.to_pickle(tansho_path)

    best_tansho = tansho_df.sort_values("return_rate", ascending=False).iloc[0]
    best_tansho_margin = tansho_df.sort_values("return_rate", ascending=False).index[0]
    print(f"  tansho best: margin={best_tansho_margin:.4f}  return_rate={best_tansho['return_rate']:.4f}  n_bets={best_tansho.get('n_bets', 'N/A')}")

    # --- 6) 複勝 threshold sweep ---
    print("  running fukusho threshold sweep...")
    thresholds = _linspace_inclusive(0.0, 3.5, n_margins)
    fukusho_results: dict[float, dict] = {}
    for th in thresholds:
        actions = keiba_ai.decide_action(
            score_table_std,
            policies.BetPolicyFukusho,
            threshold=float(th),
            max_bets_per_race=cap_fukusho,
        )
        fukusho_results[float(th)] = sim.calc_returns(actions)

    fukusho_df = pd.DataFrame.from_dict(fukusho_results, orient="index").sort_index()
    fukusho_df.index.name = "threshold"
    fukusho_path = os.path.join(out_dir, f"window_{label}_fukusho_th.pickle")
    fukusho_df.to_pickle(fukusho_path)

    best_fukusho = fukusho_df.sort_values("return_rate", ascending=False).iloc[0]
    best_fukusho_th = fukusho_df.sort_values("return_rate", ascending=False).index[0]
    print(f"  fukusho best: threshold={best_fukusho_th:.4f}  return_rate={best_fukusho['return_rate']:.4f}  n_bets={best_fukusho.get('n_bets', 'N/A')}")

    total_time = time.time() - t0
    print(f"  total time: {total_time:.1f}s")

    return {
        "label": label,
        "train_start": train_start,
        "test_start": test_start,
        "test_end": test_end,
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "train_races": train_df["race_id"].nunique() if "race_id" in train_df.columns else None,
        "test_races": test_df["race_id"].nunique() if "race_id" in test_df.columns else None,
        "train_years": f"{train_years.index.min()}-{train_years.index.max()}",
        "auc_train": auc_train,
        "auc_test": auc_test,
        "best_tansho_margin": best_tansho_margin,
        "best_tansho_return_rate": best_tansho["return_rate"],
        "best_tansho_n_bets": best_tansho.get("n_bets", None),
        "best_tansho_std": best_tansho.get("std", None),
        "best_fukusho_threshold": best_fukusho_th,
        "best_fukusho_return_rate": best_fukusho["return_rate"],
        "best_fukusho_n_bets": best_fukusho.get("n_bets", None),
        "best_fukusho_std": best_fukusho.get("std", None),
        "train_time_sec": total_time,
        "tansho_pickle": tansho_path,
        "fukusho_pickle": fukusho_path,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="学習窓長比較（5/7/10/all）")
    ap.add_argument(
        "--featured",
        default="data/tmp/featured_data_20260205.pickle",
        help="featured_data pickle path",
    )
    ap.add_argument(
        "--test-start",
        default="2024-07-01",
        help="テスト期間の開始日（inclusive）",
    )
    ap.add_argument(
        "--test-end",
        default=None,
        help="テスト期間の終了日（inclusive, デフォルト: データ末尾）",
    )
    ap.add_argument(
        "--windows",
        default="5,7,10,all",
        help="比較する窓長（年数, カンマ区切り, 'all' で全期間）",
    )
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--base-model", default=None, help="パラメータ流用元 KeibaAI pickle")
    ap.add_argument("--max-bets-tansho", type=int, default=2)
    ap.add_argument("--cap-fukusho", type=int, default=3)
    ap.add_argument("--n-margins", type=int, default=100)
    ap.add_argument("--out-dir", default=None, help="出力ディレクトリ（デフォルト: models/YYYYMMDD/）")
    args = ap.parse_args()

    # --- Load featured_data ---
    print("loading featured_data:", args.featured)
    if not os.path.isfile(args.featured):
        raise FileNotFoundError(f"not found: {args.featured}")
    featured_data = pd.read_pickle(args.featured)
    print(f"  shape: {featured_data.shape}")

    dates = pd.to_datetime(featured_data["date"])
    data_min_date = dates.min().strftime("%Y-%m-%d")
    data_max_date = dates.max().strftime("%Y-%m-%d")
    print(f"  date range: {data_min_date} ~ {data_max_date}")

    # --- Load base params ---
    base_params = None
    if args.base_model and os.path.isfile(args.base_model):
        try:
            tmp = training.KeibaAIFactory.load(args.base_model)
            base_params = tmp.get_params()
            print(f"  base params loaded from: {args.base_model}")
        except Exception as e:
            print(f"  base params load failed: {e}")

    # --- Output dir ---
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd)
    os.makedirs(out_dir, exist_ok=True)

    # --- Parse windows ---
    windows_raw = [w.strip() for w in args.windows.split(",")]
    window_specs: list[tuple[str, str | None]] = []  # (label, train_start)
    for w in windows_raw:
        if w.lower() == "all":
            window_specs.append(("all", data_min_date))
        else:
            years = int(w)
            train_start = _compute_train_start(args.test_start, years)
            # clamp to data min
            if pd.Timestamp(train_start) < pd.Timestamp(data_min_date):
                print(f"  WARNING: {years}y window start ({train_start}) < data min ({data_min_date}), clamping")
                train_start = data_min_date
            window_specs.append((f"{years}y", train_start))

    print(f"\nWindows to compare: {[w[0] for w in window_specs]}")
    print(f"Test period: {args.test_start} ~ {args.test_end or data_max_date}")

    # --- Run each window ---
    summaries = []
    for label, train_start in window_specs:
        result = run_single_window(
            featured_data=featured_data,
            train_start=train_start,
            test_start=args.test_start,
            test_end=args.test_end,
            valid_ratio=args.valid_ratio,
            base_params=base_params,
            label=label,
            out_dir=out_dir,
            max_bets_per_race_tansho=args.max_bets_tansho,
            cap_fukusho=args.cap_fukusho,
            n_margins=args.n_margins,
        )
        summaries.append(result)

    # --- Summary ---
    summary_df = pd.DataFrame(summaries)
    summary_path = os.path.join(out_dir, "window_comparison_summary.pickle")
    summary_df.to_pickle(summary_path)
    print(f"\n{'='*60}")
    print("Summary saved:", summary_path)
    print(f"{'='*60}")

    # display
    show_cols = [
        "label", "train_years", "train_rows", "test_rows",
        "auc_train", "auc_test",
        "best_tansho_margin", "best_tansho_return_rate", "best_tansho_n_bets", "best_tansho_std",
        "best_fukusho_threshold", "best_fukusho_return_rate", "best_fukusho_n_bets", "best_fukusho_std",
        "train_time_sec",
    ]
    show_cols = [c for c in show_cols if c in summary_df.columns]
    print(summary_df[show_cols].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
