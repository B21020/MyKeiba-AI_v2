"""複勝（threshold）戦略の sweep を実行して結果pickleを生成する。

Notebook編集が反映されない/カーネル状態が不安定な場合の迂回用。

出力（例）:
- models/20260208/fukusho_threshold.pickle

Usage (PowerShell):
  .venv/Scripts/python.exe tools/run_fukusho_sweep.py

Options:
  --model    KeibaAI pickle path
  --out      output pickle path
  --t-start  threshold start (default 0.5)
  --t-end    threshold end   (default 3.5)
  --n        number of samples (default 100)
    --top-k    1レースあたり購入頭数の上限（デフォルト: 3）
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._calibration import fit_calibrator


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
    """StdScorePolicy相当のレース内z-score標準化（簡易版）。"""
    st = score_table.copy()
    if 'race_id' in st.columns:
        keys = st['race_id']
    else:
        try:
            keys = st.index.get_level_values(0)
        except Exception:
            keys = None

    s = pd.to_numeric(st['score'], errors='coerce').astype(float)
    if keys is None:
        m = float(s.mean())
        std = float(s.std(ddof=0))
        if np.isfinite(std) and std > 0:
            st['score'] = ((s - m) / std).replace([np.inf, -np.inf], 0).fillna(0)
        else:
            st['score'] = s.replace([np.inf, -np.inf], 0).fillna(0)
        return st

    g = s.groupby(keys)
    mean_ = g.transform('mean')
    std_ = g.transform('std')
    std_ = std_.replace(0, np.nan)
    z = (s - mean_) / std_
    st['score'] = z.where(np.isfinite(z), s).fillna(0).astype(float)
    return st


def _linspace_inclusive(start: float, end: float, n: int) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def _build_score_table_for_test(keiba_ai) -> pd.DataFrame:
    """旧pickle互換を考慮して、race_id/馬番をなるべく保持してscore_tableを作る。"""
    X_test = keiba_ai.datasets.X_test
    X_for_score = X_test
    try:
        ds = keiba_ai.datasets
        test_data = getattr(ds, "test_data", None)
        if test_data is not None and hasattr(test_data, "empty") and not test_data.empty:
            cols = list(X_test.columns)
            for c in ["race_id", ResultsCols.UMABAN]:
                if c in test_data.columns and c not in cols:
                    cols.append(c)
            X_for_score = test_data.loc[:, cols]
    except Exception:
        X_for_score = X_test

    score_table = keiba_ai.calc_score(X_for_score, policies.BasicScorePolicy)
    return score_table


def _actions_fukusho_from_score_table(
    score_table: pd.DataFrame,
    *,
    threshold: float,
    top_k: int | None,
) -> dict:
    table = score_table
    table = table.copy()

    # numeric化（安全弁）
    table["score"] = pd.to_numeric(table["score"], errors="coerce")
    if ResultsCols.UMABAN in table.columns:
        table[ResultsCols.UMABAN] = pd.to_numeric(table[ResultsCols.UMABAN], errors="coerce")

    table = table.dropna(subset=["score", ResultsCols.UMABAN])
    table = table[table["score"] >= float(threshold)]
    if table.empty:
        return {}

    # cap: レースごとに上位k頭
    if top_k is not None:
        k = int(top_k)
        if k <= 0:
            raise ValueError("top_k must be a positive int or None")
        if "race_id" in table.columns:
            table = (
                table.sort_values(["race_id", "score"], ascending=[True, False])
                .groupby("race_id", sort=False)
                .head(k)
            )
        else:
            race_keys = table.index.get_level_values(0)
            table = (
                table.assign(_race_key=race_keys)
                .sort_values(["_race_key", "score"], ascending=[True, False])
                .groupby("_race_key", sort=False)
                .head(k)
                .drop(columns=["_race_key"])
            )

    # actions形式へ
    if "race_id" in table.columns:
        bet_df = table.groupby("race_id")[ResultsCols.UMABAN].apply(list).to_frame()
    else:
        bet_df = table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

    def _to_int_list(xs):
        s = pd.to_numeric(pd.Series(xs), errors="coerce")
        return s.dropna().astype(int).drop_duplicates().tolist()

    bet_df[ResultsCols.UMABAN] = bet_df[ResultsCols.UMABAN].apply(_to_int_list)
    bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) > 0]
    bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: "fukusho"}).T.to_dict()
    return bet_dict


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--model",
        default="models/20260208/basemodel_2020_2026.pickle",
        help="KeibaAI model pickle path",
    )
    ap.add_argument(
        "--out",
        default="models/20260208/fukusho_threshold.pickle",
        help="output pickle path",
    )
    ap.add_argument("--t-start", type=float, default=0.5)
    ap.add_argument("--t-end", type=float, default=3.5)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--top-k", type=int, default=3)
    ap.add_argument(
        "--score-policy",
        choices=["std", "basic"],
        default="std",
        help="score definition: std=z-score within race, basic=raw proba",
    )
    ap.add_argument(
        "--calibration",
        choices=["none", "platt", "isotonic"],
        default="none",
        help="fit calibrator on valid and apply to test probabilities",
    )
    args = ap.parse_args()

    model_path = args.model
    out_path = args.out

    print("loading model:", model_path)
    keiba_ai = training.KeibaAIFactory.load(model_path)
    keiba_ai.set_params(keiba_ai.get_params())

    calibrator = None
    if args.calibration != "none":
        print("fitting calibrator on valid:", args.calibration)
        X_valid = keiba_ai.datasets.X_valid
        y_valid = keiba_ai.datasets.y_valid
        valid_score_table = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
        calibrator = fit_calibrator(args.calibration, y_valid, valid_score_table["score"].to_numpy())

    print("building score_table (test)...")
    score_table = _build_score_table_for_test(keiba_ai)
    if calibrator is not None:
        score_table = score_table.copy()
        score_table["score"] = calibrator(score_table["score"].to_numpy())
    if args.score_policy == "std":
        score_table = _standardize_within_race(score_table)

    print("loading return tables + simulator...")
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    thresholds = _linspace_inclusive(float(args.t_start), float(args.t_end), int(args.n))

    returns: dict[float, dict] = {}
    for i, thr in enumerate(thresholds, start=1):
        actions = _actions_fukusho_from_score_table(score_table, threshold=float(thr), top_k=args.top_k)
        returns[float(thr)] = sim.calc_returns(actions)

        if i % 10 == 0 or i == 1 or i == len(thresholds):
            rr = returns[float(thr)].get("return_rate", None)
            n_bets = returns[float(thr)].get("n_bets", None)
            print(f"[{i}/{len(thresholds)}] threshold={thr:.4f} return_rate={rr} n_bets={n_bets}")

    returns_df = pd.DataFrame.from_dict(returns, orient="index").sort_index()
    returns_df.index.name = "threshold"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    returns_df.to_pickle(out_path)
    print("saved:", out_path)

    try:
        best = returns_df.sort_values("return_rate", ascending=False).head(5)
        print("top5 by return_rate:")
        print(best[["return_rate", "n_bets", "n_races", "n_hits", "total_bet_amount", "std"]])
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
