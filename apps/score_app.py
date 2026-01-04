import datetime
import os
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
import streamlit as st

from modules import policies, preparing, preprocessing, training
from modules.constants import HorseResultsCols, LocalPaths, Master


TARGET_COLS = [
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

GROUP_COLS = [
    "course_len",
    "race_type",
    HorseResultsCols.PLACE,
]


def _find_default_model_path() -> Optional[str]:
    models_dir = Path("models")
    if not models_dir.exists():
        return None

    candidates = list(models_dir.glob("**/*.pickle"))
    if not candidates:
        return None

    def sort_key(p: Path):
        # 1) yyyymmddディレクトリを優先的に評価 2) 更新日時
        try:
            yyyymmdd = int(p.parent.name)
        except ValueError:
            yyyymmdd = 0
        return (yyyymmdd, p.stat().st_mtime)

    best = max(candidates, key=sort_key)
    return str(best).replace("\\", "/")


def _race_label(race_id: str, race_time: str) -> str:
    place_code = race_id[4:6]
    round_str = race_id[10:12]

    place_name = place_code
    for name, code in Master.PLACE_DICT.items():
        if code == place_code:
            place_name = name
            break

    return f"{place_name}{round_str}R {race_time}発走"


def _safe_first(series: pd.Series, default: str = "") -> str:
    if series is None or series.empty:
        return default
    v = series.iloc[0]
    if pd.isna(v):
        return default
    return str(v)


def _race_conditions_label(race_id: str, race_time: str, shutuba_raw: pd.DataFrame) -> str:
    """Notebook出力（セル146-147）と同等の情報を組み立てる。"""
    base = _race_label(race_id, race_time)
    race_type = _safe_first(shutuba_raw.get("race_type"))
    course_len = _safe_first(shutuba_raw.get("course_len"))
    around = _safe_first(shutuba_raw.get("around"))
    ground_state = _safe_first(shutuba_raw.get("ground_state"))
    weather = _safe_first(shutuba_raw.get("weather"))

    parts = []
    if race_type:
        parts.append(race_type)
    if course_len and course_len != "0":
        parts.append(f"{course_len}m")
    if around:
        parts.append(around)
    if ground_state:
        parts.append(ground_state)
    if weather:
        parts.append(weather)

    if parts:
        return f"{base}  " + " ".join(parts)
    return base


@st.cache_resource
def _load_processors():
    horse_info_processor = preprocessing.HorseInfoProcessor(filepath=LocalPaths.RAW_HORSE_INFO_PATH)
    horse_results_processor = preprocessing.HorseResultsProcessor(filepath=LocalPaths.RAW_HORSE_RESULTS_PATH)
    peds_processor = preprocessing.PedsProcessor(filepath=LocalPaths.RAW_PEDS_PATH)
    return horse_results_processor, horse_info_processor, peds_processor


@st.cache_resource
def _load_model(model_path: str):
    keiba_ai = training.KeibaAIFactory.load(model_path)
    # Notebookと同様、読み込み直後にパラメータ再セット（互換維持）
    keiba_ai.set_params(keiba_ai.get_params())
    return keiba_ai


def main():
    st.set_page_config(page_title="MyKeiba-AI 当日スコア予測", layout="wide")
    st.title("当日レース スコア予測")

    default_model = _find_default_model_path() or ""
    model_path = st.text_input("モデルファイル(.pickle)のパス", value=default_model)

    col1, col2 = st.columns(2)
    with col1:
        minus_time = st.number_input(
            "馬体重発表の判定（レース時刻から何分前か）",
            min_value=-180,
            max_value=-1,
            value=-50,
            step=1,
            help="Notebookの create_active_race_id_list(minus_time=...) に渡されます",
        )
    with col2:
        sleep_sec = st.number_input(
            "スクレイピング間隔(秒)",
            min_value=0.0,
            max_value=10.0,
            value=1.0,
            step=0.5,
            help="サーバー負荷軽減のため、各レース取得前に待機します",
        )

    if model_path and not Path(model_path).exists():
        st.warning("モデルファイルが見つかりません（パスを確認してください）")

    run = st.button("当日レース取得 → 予測実行", type="primary", disabled=not bool(model_path))

    if not run:
        st.caption("実行すると、このPC上でスクレイピング（Selenium）と予測を行い、結果を表示します。")
        return

    os.makedirs("data/tmp", exist_ok=True)
    tmp_shutuba_path = "data/tmp/shutuba.pickle"

    today = datetime.datetime.now().date().strftime("%Y/%m/%d")

    try:
        st.info("対象レース（馬体重発表済み）を取得中...")
        target_race_id_list, target_race_time_list = preparing.create_active_race_id_list(minus_time=int(minus_time))

        race_data_sorted = sorted(zip(target_race_id_list, target_race_time_list), key=lambda x: x[1])
        target_race_id_list = [race_id for race_id, _ in race_data_sorted]
        target_race_time_list = [race_time for _, race_time in race_data_sorted]

        if not target_race_id_list:
            st.warning("対象レースが見つかりませんでした（時間帯/開催日をご確認ください）")
            return

        st.success(f"対象レース数: {len(target_race_id_list)}")

        horse_results_processor, horse_info_processor, peds_processor = _load_processors()
        keiba_ai = _load_model(model_path)

        all_rows: List[pd.DataFrame] = []

        for idx, (race_id, race_time) in enumerate(zip(target_race_id_list, target_race_time_list), 1):
            st.write(f"{idx}/{len(target_race_id_list)}: {_race_label(race_id, race_time)}")

            try:
                if sleep_sec:
                    time.sleep(float(sleep_sec))

                preparing.scrape_shutuba_table(race_id, today, tmp_shutuba_path)

                shutuba_table_processor = preprocessing.ShutubaTableProcessor(tmp_shutuba_path)

                # Notebookと同様、前処理前テーブルからレース条件を表示
                try:
                    st.caption(_race_conditions_label(race_id, race_time, shutuba_table_processor.raw_data[:1]))
                except Exception:
                    # 表示は補助情報なので、失敗しても予測は継続
                    pass

                shutuba_data_merger = preprocessing.ShutubaDataMerger(
                    shutuba_table_processor,
                    horse_results_processor,
                    horse_info_processor,
                    peds_processor,
                    target_cols=TARGET_COLS,
                    group_cols=GROUP_COLS,
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
                score_df = keiba_ai.calc_score(X, policies.StdScorePolicy).sort_values("score", ascending=False)

                score_df = score_df.copy()
                score_df.insert(0, "race_id", race_id)
                score_df.insert(1, "race_time", race_time)

                st.dataframe(score_df, use_container_width=True)

                all_rows.append(score_df)

            except Exception as e:
                st.error(f"{race_id} の処理でエラー: {e}")
                continue

        if all_rows:
            combined = pd.concat(all_rows, axis=0, ignore_index=True)
            csv = combined.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="結果CSVをダウンロード",
                data=csv,
                file_name=f"scores_{datetime.date.today().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.warning("予測に成功したレースがありませんでした")

    except Exception as e:
        st.error(f"実行中にエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
