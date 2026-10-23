"""win(1着)目的のモデルを学習してpickle保存する。

目的:
- 既存の featured_data.pickle（placeラベルrankはあるがwinが無いことがある）に対し、
  results（レース結果）から win=(着順==1) を付与して学習できるようにする。

使い方例:
  python tools/train_win_model.py \
    --featured data/tmp/featured_data_20260205.pickle \
    --results data/raw/results.pickle.bak \
    --version win_2020_2026 \
    --test-size 0.3 --valid-size 0.3

出力:
- models/YYYYMMDD/<version>.pickle
- （任意）data/tmp/<out-featured> に win 付き featured_data を保存
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# tools/ 配下から実行した場合でも、プロジェクト直下(modules/ 等)を import できるようにする
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules.constants import LocalPaths, ResultsCols
from modules.preprocessing import ResultsProcessor
from modules.training import KeibaAIFactory


def _default_results_path() -> str:
    candidates = [
        LocalPaths.RAW_RESULTS_PATH,
        os.path.join(LocalPaths.RAW_DIR, "results.pickle.bak"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return candidates[-1]


def _ensure_race_id_column(df: pd.DataFrame) -> pd.DataFrame:
    if "race_id" in df.columns:
        return df
    out = df.copy()
    out["race_id"] = out.index.astype(str)
    return out


def _attach_win_label(featured: pd.DataFrame, results_raw: pd.DataFrame) -> pd.DataFrame:
    if "win" in featured.columns:
        return featured

    if ResultsCols.UMABAN not in featured.columns:
        raise ValueError(f"featured_data must contain '{ResultsCols.UMABAN}' column to attach win label")

    featured2 = _ensure_race_id_column(featured)

    # results -> preprocess -> win/rank/race_id/馬番 を取得
    rp = ResultsProcessor(filepath=None, raw_data=results_raw)
    results = rp.preprocessed_data

    needed = ["race_id", ResultsCols.UMABAN, "win"]
    missing = [c for c in needed if c not in results.columns]
    if missing:
        raise ValueError(f"results(preprocessed) missing columns: {missing}")

    res = results.loc[:, needed].copy()

    # join key の型を寄せる
    featured2["race_id"] = featured2["race_id"].astype(str)
    res["race_id"] = res["race_id"].astype(str)

    # UMABAN が数値/文字列混在でも merge できるように一旦 numeric へ
    featured2[ResultsCols.UMABAN] = pd.to_numeric(featured2[ResultsCols.UMABAN], errors="coerce")
    res[ResultsCols.UMABAN] = pd.to_numeric(res[ResultsCols.UMABAN], errors="coerce")

    merged = featured2.merge(res, on=["race_id", ResultsCols.UMABAN], how="left")

    # 未結合（欠損）は0扱い（取消等も含め、学習上は非勝利として扱う）
    merged["win"] = pd.to_numeric(merged["win"], errors="coerce").fillna(0).astype(int)

    return merged


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--featured", default=os.path.join(LocalPaths.TMP_DIR, "featured_data_20260205.pickle"))
    parser.add_argument("--results", default=_default_results_path())
    parser.add_argument("--version", default="win_model")
    parser.add_argument("--test-size", type=float, default=0.3)
    parser.add_argument("--valid-size", type=float, default=0.3)
    parser.add_argument("--out-featured", default="", help="win付与後のfeatured_dataを保存する場合の出力パス")
    args = parser.parse_args()

    if not os.path.isfile(args.featured):
        raise FileNotFoundError(f"featured pickle not found: {args.featured}")
    if not os.path.isfile(args.results):
        raise FileNotFoundError(f"results pickle not found: {args.results}")

    print(f"load featured: {args.featured}")
    featured = pd.read_pickle(args.featured)
    if not isinstance(featured, pd.DataFrame):
        raise TypeError("featured must be a pandas DataFrame")

    print(f"load results: {args.results}")
    results_raw = pd.read_pickle(args.results)
    if not isinstance(results_raw, pd.DataFrame):
        raise TypeError("results must be a pandas DataFrame")

    if "win" not in featured.columns:
        print("attach win label from results")
        featured = _attach_win_label(featured, results_raw)

    if args.out_featured:
        out_path = args.out_featured
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        featured.to_pickle(out_path)
        print(f"saved featured with win: {out_path}")

    # 学習
    print("train win model")
    keiba_ai = KeibaAIFactory.create_win(featured, test_size=args.test_size, valid_size=args.valid_size)
    keiba_ai.train_without_tuning()

    KeibaAIFactory.save(keiba_ai, args.version)
    print(f"saved model version={args.version}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
