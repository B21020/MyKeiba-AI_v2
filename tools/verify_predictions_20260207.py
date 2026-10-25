"""2026/02/07 の全レースを base_model / longshot_model で推論してCSV出力する。

出力:
- data/score_20260207/all_races.csv
- data/score_20260207/{race_id}.csv

メモ:
- スクレイピング負荷軽減のため、未取得レースの取得前に time.sleep(1) を入れる。
- 出馬表pickleは data/tmp/shutuba_20260207/ にキャッシュして再実行を高速化する。
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

import pandas as pd

# tools/ 配下から実行した場合でも、プロジェクト直下(modules/ 等)を import できるようにする
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules import policies, preparing, preprocessing, training
from modules.constants import HorseResultsCols, LocalPaths, ResultsCols


def _upsert_front_columns(df: pd.DataFrame, values_by_col: dict) -> pd.DataFrame:
    out = df.copy()
    for col, value in values_by_col.items():
        if col in out.columns:
            out[col] = value
    missing = [col for col in values_by_col.keys() if col not in out.columns]
    if not missing:
        return out
    front = pd.DataFrame({col: [values_by_col[col]] * len(out) for col in missing})
    return pd.concat([front, out], axis=1)


def _default_target_cols() -> List[str]:
    return [
        HorseResultsCols.RANK,
        HorseResultsCols.PRIZE,
        HorseResultsCols.RANK_DIFF,
        "first_corner",
        "final_corner",
        "first_to_rank",
        "first_to_final",
        "final_to_rank",
        "time_seconds",
    ]


def _default_group_cols() -> List[str]:
    return [
        "course_len",
        "race_type",
        HorseResultsCols.PLACE,
    ]


def _race_list(yyyymmdd: str) -> Tuple[List[str], List[str]]:
    race_id_list, race_time_list = preparing.scrape_race_id_race_time_list(yyyymmdd)
    race_data_sorted = sorted(zip(race_id_list, race_time_list), key=lambda x: x[1])
    return [r for r, _ in race_data_sorted], [t for _, t in race_data_sorted]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--yyyymmdd", default="20260207")
    parser.add_argument("--date-slash", default="2026/02/07", help="yyyy/mm/dd")
    parser.add_argument("--base-model", default="models/20260207/basemodel_2020_2026.pickle")
    parser.add_argument("--longshot-model", default="models/20260207/longshot/longshot_2020_2026.pickle")
    parser.add_argument("--sleep-sec", type=float, default=1.0)
    parser.add_argument("--skip-scrape", action="store_true", help="出馬表pickle取得をスキップ（既存キャッシュ前提）")
    args = parser.parse_args()

    yyyymmdd = args.yyyymmdd
    date_slash = args.date_slash

    out_dir = os.path.join(LocalPaths.DATA_DIR, f"score_{yyyymmdd}")
    os.makedirs(out_dir, exist_ok=True)

    shutuba_cache_dir = os.path.join(LocalPaths.TMP_DIR, f"shutuba_{yyyymmdd}")
    os.makedirs(shutuba_cache_dir, exist_ok=True)

    def shutuba_pickle_path(race_id: str) -> str:
        return os.path.join(shutuba_cache_dir, f"shutuba_{yyyymmdd}_{race_id}.pickle")

    target_cols = _default_target_cols()
    group_cols = _default_group_cols()

    print("[1/5] fetch race list")
    race_id_list, race_time_list = _race_list(yyyymmdd)
    print(f"n_races={len(race_id_list)}")

    if not args.skip_scrape:
        print("[2/5] scrape shutuba tables (missing only)")
        failed = []
        for i, (race_id, race_time) in enumerate(zip(race_id_list, race_time_list), start=1):
            path = shutuba_pickle_path(race_id)
            if os.path.isfile(path):
                continue
            print(f"[{i}/{len(race_id_list)}] scrape {race_id} ({race_time})")
            if args.sleep_sec:
                time.sleep(float(args.sleep_sec))
            try:
                preparing.scrape_shutuba_table(race_id, date_slash, path)
            except Exception as e:
                print(f"[ERROR] {race_id}: {e}")
                failed.append(race_id)
        print("failed_scrape:", len(failed))

    print("[3/5] collect horse_id list")
    horse_id_set = set()
    for race_id in race_id_list:
        path = shutuba_pickle_path(race_id)
        if not os.path.isfile(path):
            continue
        try:
            shutuba_raw = pd.read_pickle(path)
            if isinstance(shutuba_raw, pd.DataFrame) and "horse_id" in shutuba_raw.columns:
                horse_id_set.update(shutuba_raw["horse_id"].astype(str).dropna().tolist())
        except Exception:
            continue
    horse_id_list = sorted(horse_id_set)
    print(f"n_horses={len(horse_id_list)}")

    print("[4/5] load processors/models (horse_id filtered)")
    # horse_results は非常に巨大なため、検証日に出走する馬だけに絞ってから前処理する
    horse_results_raw = pd.read_pickle(LocalPaths.RAW_HORSE_RESULTS_PATH)
    try:
        hr_index = horse_results_raw.index.astype(str)
    except Exception:
        hr_index = horse_results_raw.index
    filtered_horse_results = horse_results_raw.loc[hr_index.isin(horse_id_set)].copy()
    del horse_results_raw
    gc.collect()

    horse_info_raw = pd.read_pickle(LocalPaths.RAW_HORSE_INFO_PATH)
    try:
        hi_index = horse_info_raw.index.astype(str)
    except Exception:
        hi_index = horse_info_raw.index
    filtered_horse_info = horse_info_raw.loc[hi_index.isin(horse_id_set)].copy()
    del horse_info_raw
    gc.collect()

    peds_raw = pd.read_pickle(LocalPaths.RAW_PEDS_PATH)
    try:
        peds_index = peds_raw.index.astype(str)
    except Exception:
        peds_index = peds_raw.index
    filtered_peds = peds_raw.loc[peds_index.isin(horse_id_set)].copy()
    del peds_raw
    gc.collect()

    horse_results_processor = preprocessing.HorseResultsProcessor(
        filepath=LocalPaths.RAW_HORSE_RESULTS_PATH,
        raw_data=filtered_horse_results,
    )
    horse_info_processor = preprocessing.HorseInfoProcessor(
        filepath=LocalPaths.RAW_HORSE_INFO_PATH,
        raw_data=filtered_horse_info,
    )
    peds_processor = preprocessing.PedsProcessor(
        filepath=LocalPaths.RAW_PEDS_PATH,
        raw_data=filtered_peds,
    )

    keiba_ai_base = training.KeibaAIFactory.load(args.base_model)
    keiba_ai_base.set_params(keiba_ai_base.get_params())

    keiba_ai_long = training.KeibaAIFactory.load(args.longshot_model)
    keiba_ai_long.set_params(keiba_ai_long.get_params())

    print("[5/5] build features -> predict -> save csv")
    all_rows = []
    for i, (race_id, race_time) in enumerate(zip(race_id_list, race_time_list), start=1):
        path = shutuba_pickle_path(race_id)
        if not os.path.isfile(path):
            print(f"[SKIP] shutuba pickle not found: {race_id}")
            continue

        try:
            shutuba_table_processor = preprocessing.ShutubaTableProcessor(path)

            shutuba_data_merger = preprocessing.ShutubaDataMerger(
                shutuba_table_processor,
                horse_results_processor,
                horse_info_processor,
                peds_processor,
                target_cols=target_cols,
                group_cols=group_cols,
            )
            shutuba_data_merger.merge()

            feature_engineering = (
                preprocessing.FeatureEngineering(shutuba_data_merger)
                .add_interval()
                .add_agedays()
                .dumminize_ground_state()
                .dumminize_race_type()
                .dumminize_sex()
                .dumminize_weather()
                .encode_horse_id()
                .encode_jockey_id()
                .encode_trainer_id()
                .encode_owner_id()
                .encode_breeder_id()
                .dumminize_kaisai()
                .dumminize_around()
                .dumminize_race_class()
            )

            X = feature_engineering.featured_data.drop(["date"], axis=1, errors="ignore")
            if "race_id" not in X.columns:
                X = X.copy()
                X["race_id"] = race_id

            score_base = keiba_ai_base.calc_score(X, policies.StdScorePolicy)
            score_long = keiba_ai_long.calc_score(X, policies.StdScorePolicy)

            if "score" in score_base.columns:
                score_base = score_base.rename(columns={"score": "score_base"})
            if "score" in score_long.columns:
                score_long = score_long.rename(columns={"score": "score_longshot"})

            key_cols = ["race_id"]
            if ResultsCols.UMABAN in score_base.columns and ResultsCols.UMABAN in score_long.columns:
                key_cols.append(ResultsCols.UMABAN)

            score_long_rest = [c for c in score_long.columns if c not in key_cols]
            merged = score_base.merge(score_long[key_cols + score_long_rest], on=key_cols, how="left")
            merged = _upsert_front_columns(merged, {"race_id": race_id, "race_time": race_time})

            for col in ["actual_rank", "hit_manual_base", "hit_manual_longshot"]:
                if col not in merged.columns:
                    merged[col] = pd.NA

            merged.to_csv(os.path.join(out_dir, f"{race_id}.csv"), index=False, encoding="utf-8-sig")
            all_rows.append(merged)

            if i % 5 == 0:
                print(f"done {i}/{len(race_id_list)}")

        except Exception as e:
            print(f"[ERROR] {race_id}: {e}")
            continue

    if not all_rows:
        raise RuntimeError("予測結果が1件も作れませんでした")

    all_races = pd.concat(all_rows, axis=0, ignore_index=True)
    sort_cols = ["race_time", "race_id"]
    if ResultsCols.UMABAN in all_races.columns:
        sort_cols.append(ResultsCols.UMABAN)
    all_races = all_races.sort_values(sort_cols, na_position="last")

    all_path = os.path.join(out_dir, "all_races.csv")
    all_races.to_csv(all_path, index=False, encoding="utf-8-sig")

    print("saved:")
    print("-", all_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
