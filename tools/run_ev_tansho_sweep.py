"""単勝EV（score * odds）戦略の margin sweep を実行して結果pickleを生成する。

Notebook編集が反映されない/カーネル状態が不安定な場合の迂回用。

出力:
- models/20260208/tansho_ev_margin.pickle（デフォルト）

Usage (PowerShell):
  .venv/Scripts/python.exe tools/run_ev_tansho_sweep.py

Options:
  --model    KeibaAI pickle path
  --out      output pickle path
  --m-start  margin start (default 0.0)
  --m-end    margin end   (default 0.5)
  --n        number of samples (default 100)
  --max-bets-per-race  raceごとの最大購入点数（指定しない場合は無制限）
  --min-odds           購入対象の最低オッズ
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# tools/ 配下から実行した場合でも、プロジェクト直下(modules/ 等)を import できるようにする
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths


def _linspace_inclusive(start: float, end: float, n: int) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--model",
        default="models/20260208/basemodel_2020_2026.pickle",
        help="KeibaAI model pickle path",
    )
    ap.add_argument(
        "--out",
        default="models/20260208/tansho_ev_margin.pickle",
        help="output pickle path",
    )
    ap.add_argument("--m-start", type=float, default=0.0)
    ap.add_argument("--m-end", type=float, default=0.5)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--max-bets-per-race", type=int, default=None)
    ap.add_argument("--min-odds", type=float, default=None)
    args = ap.parse_args()

    model_path = args.model
    out_path = args.out

    print("loading model:", model_path)
    keiba_ai = training.KeibaAIFactory.load(model_path)
    keiba_ai.set_params(keiba_ai.get_params())

    print("building score_table (test)...")
    X_test = keiba_ai.datasets.X_test
    score_table = keiba_ai.calc_score(X_test, policies.StdScorePolicy)

    print("loading odds (test)...")
    tansho_odds = keiba_ai.datasets.tansho_odds_test

    print("loading return tables + simulator...")
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    margins = _linspace_inclusive(float(args.m_start), float(args.m_end), int(args.n))

    returns_ev: dict[float, dict] = {}
    for i, margin in enumerate(margins, start=1):
        actions = keiba_ai.decide_action(
            score_table,
            policies.BetPolicyEVTansho,
            margin=float(margin),
            tansho_odds=tansho_odds,
            max_bets_per_race=args.max_bets_per_race,
            min_odds=args.min_odds,
        )
        returns_ev[float(margin)] = sim.calc_returns(actions)

        if i % 10 == 0 or i == 1 or i == len(margins):
            rr = returns_ev[float(margin)].get("return_rate", None)
            n_bets = returns_ev[float(margin)].get("n_bets", None)
            print(f"[{i}/{len(margins)}] margin={margin:.4f} return_rate={rr} n_bets={n_bets}")

    returns_ev_df = pd.DataFrame.from_dict(returns_ev, orient="index").sort_index()
    returns_ev_df.index.name = "margin"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    returns_ev_df.to_pickle(out_path)
    print("saved:", out_path)

    # 参考: best margin を表示（return_rate最大）
    try:
        best = returns_ev_df.sort_values("return_rate", ascending=False).head(5)
        print("top5 by return_rate:")
        print(best[["return_rate", "n_bets", "n_races", "n_hits", "total_bet_amount", "std"]])
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
