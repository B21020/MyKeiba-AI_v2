import datetime
import os
import pickle
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from modules import policies, preparing, preprocessing, training
from modules.constants import HorseResultsCols, LocalPaths, Master, ResultsCols


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

    # 1) models直下の最新日付フォルダ（YYYYMMDD想定）
    date_dirs: List[Path] = []
    for child in models_dir.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if len(name) != 8 or not name.isdigit():
            continue
        date_dirs.append(child)

    latest_dir: Optional[Path] = max(date_dirs, key=lambda p: int(p.name)) if date_dirs else None

    # 2) そのフォルダ内の basemodel_ から始まるpickleを優先
    if latest_dir is not None:
        base_candidates = list(latest_dir.glob("basemodel_*.pickle"))
        if base_candidates:
            best = max(base_candidates, key=lambda p: p.stat().st_mtime)
            return str(best).replace("\\", "/")

        # basemodel_が無い場合は、そのフォルダ内の最新pickleへフォールバック
        any_candidates = list(latest_dir.glob("*.pickle"))
        if any_candidates:
            best = max(any_candidates, key=lambda p: p.stat().st_mtime)
            return str(best).replace("\\", "/")

    # 最終フォールバック（従来互換）: models配下から最新っぽいpickle
    candidates = list(models_dir.glob("**/*.pickle"))
    if not candidates:
        return None

    def sort_key(p: Path):
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


def _upsert_front_columns(df: pd.DataFrame, values_by_col: dict) -> pd.DataFrame:
    """列があれば上書き、なければ先頭側へ追加する。

    `DataFrame.insert()` は同名列があると例外になるため、
    `calc_score()` の戻りDFに列が含まれる場合でも安全に付与できるようにする。
    """
    out = df.copy()
    # 既存列は上書き
    for col, value in values_by_col.items():
        if col in out.columns:
            out[col] = value

    # 無い列だけを順序維持で前に入れる
    missing = [col for col in values_by_col.keys() if col not in out.columns]
    if not missing:
        return out

    front = pd.DataFrame({col: [values_by_col[col]] * len(out) for col in missing})
    return pd.concat([front, out], axis=1)


def _sort_score_df(df: pd.DataFrame, sort_mode: str) -> pd.DataFrame:
    """予測結果テーブルの表示順を切り替える。

    - スコア順: score 降順（同点時は馬番昇順）
    - 馬番順: 馬番 昇順（同点時は score 降順）
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df

    out = df.copy()

    has_score = "score" in out.columns
    has_umaban = ResultsCols.UMABAN in out.columns

    if has_umaban:
        out[ResultsCols.UMABAN] = pd.to_numeric(out[ResultsCols.UMABAN], errors="coerce")

    if sort_mode == "馬番順（昇順）" and has_umaban:
        if has_score:
            return out.sort_values([ResultsCols.UMABAN, "score"], ascending=[True, False], na_position="last")
        return out.sort_values([ResultsCols.UMABAN], ascending=[True], na_position="last")

    # デフォルト: スコア順（降順）
    if has_score and has_umaban:
        return out.sort_values(["score", ResultsCols.UMABAN], ascending=[False, True], na_position="last")
    if has_score:
        return out.sort_values(["score"], ascending=[False], na_position="last")
    return out


def _cache_file_path(today_yyyymmdd: str) -> str:
    return f"data/tmp/score_cache_{today_yyyymmdd}.pkl"


def _cache_key(
    *,
    today_yyyymmdd: str,
    model_path: str,
    minus_time: int,
    race_id: str,
) -> str:
    # 端末差やOS差で揺れないように正規化
    mp = (model_path or "").replace("\\", "/")
    return f"{today_yyyymmdd}|{minus_time}|{mp}|{race_id}"


def _load_cache_from_disk(path: str) -> Dict[str, Dict[str, Any]]:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "rb") as f:
            obj = pickle.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return {}
    return {}


def _save_cache_to_disk(path: str, cache: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "wb") as f:
        pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp_path, path)


def _init_prediction_cache(today_yyyymmdd: str) -> None:
    """セッションキャッシュを初期化し、ディスクキャッシュがあれば読み込む。"""
    if "prediction_cache" in st.session_state:
        return
    cache_path = _cache_file_path(today_yyyymmdd)
    st.session_state["prediction_cache"] = _load_cache_from_disk(cache_path)


def _reset_prediction_cache(today_yyyymmdd: str) -> None:
    st.session_state["prediction_cache"] = {}
    cache_path = _cache_file_path(today_yyyymmdd)
    try:
        if os.path.isfile(cache_path):
            os.remove(cache_path)
    except Exception:
        pass


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

    today_date = datetime.datetime.now().date()
    today_yyyymmdd = today_date.strftime("%Y%m%d")
    _init_prediction_cache(today_yyyymmdd)

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

    sort_mode = st.radio(
        "予測結果の表示順",
        options=["スコア順（降順）", "馬番順（昇順）"],
        horizontal=True,
        index=0,
    )

    if model_path and not Path(model_path).exists():
        st.warning("モデルファイルが見つかりません（パスを確認してください）")

    with st.expander("予測済みキャッシュ", expanded=True):
        use_cache = st.checkbox(
            "予測済み結果を使い回す（誤ってページ更新しても再スクレイピングを避ける）",
            value=True,
        )
        persist_cache = st.checkbox(
            "キャッシュをディスクに保存する（再読込/再接続でも復元）",
            value=True,
            help="data/tmp に当日分のキャッシュファイルを保存します",
        )
        force_recompute = st.checkbox(
            "キャッシュがあっても再予測する（騎手変更などの反映用）",
            value=False,
        )
        cache_count = len(st.session_state.get("prediction_cache", {}))
        st.caption(f"キャッシュ件数: {cache_count}")
        if st.button("予測済みリストをリセット（全削除）", type="secondary"):
            _reset_prediction_cache(today_yyyymmdd)
            st.success("予測済みキャッシュをリセットしました")
            st.stop()

    run = st.button("当日レース取得 → 予測実行", type="primary", disabled=not bool(model_path))

    if not run:
        st.caption("実行すると、このPC上でスクレイピング（Selenium）と予測を行い、結果を表示します。")
        return

    os.makedirs("data/tmp", exist_ok=True)
    today = today_date.strftime("%Y/%m/%d")

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

        progress = st.progress(0)
        status = st.empty()

        for idx, (race_id, race_time) in enumerate(zip(target_race_id_list, target_race_time_list), 1):
            progress.progress(int((idx - 1) / max(len(target_race_id_list), 1) * 100))
            status.write(f"進捗: {idx}/{len(target_race_id_list)}")
            st.write(f"{idx}/{len(target_race_id_list)}: {_race_label(race_id, race_time)}")

            cache_path = _cache_file_path(today_yyyymmdd)
            key = _cache_key(
                today_yyyymmdd=today_yyyymmdd,
                model_path=model_path,
                minus_time=int(minus_time),
                race_id=race_id,
            )
            cache: Dict[str, Dict[str, Any]] = st.session_state.get("prediction_cache", {})

            if use_cache and (not force_recompute) and key in cache:
                cached = cache[key]
                cached_at = cached.get("cached_at", "")
                if cached_at:
                    st.caption(f"キャッシュ使用: {cached_at}")
                score_df_base = cached.get("score_df_base")
                if score_df_base is None:
                    score_df_base = cached.get("score_df")  # 旧キャッシュ互換
                if isinstance(score_df_base, pd.DataFrame):
                    score_df_view = _sort_score_df(score_df_base, sort_mode)
                    st.dataframe(score_df_view, use_container_width=True)
                    all_rows.append(score_df_view)
                    continue

            try:
                if sleep_sec:
                    time.sleep(float(sleep_sec))

                # 出馬表もレースごとに保存しておく（同一セッション内での再実行を高速化）
                tmp_shutuba_path = f"data/tmp/shutuba_{today_yyyymmdd}_{race_id}.pickle"
                if force_recompute or (not use_cache) or (not os.path.isfile(tmp_shutuba_path)):
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
                score_df_base = keiba_ai.calc_score(X, policies.StdScorePolicy)

                score_df_base = _upsert_front_columns(
                    score_df_base,
                    {
                        "race_id": race_id,
                        "race_time": race_time,
                    },
                )

                score_df_view = _sort_score_df(score_df_base, sort_mode)

                # 予測済みキャッシュに保存
                if use_cache:
                    cache[key] = {
                        "score_df_base": score_df_base,
                        "cached_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    st.session_state["prediction_cache"] = cache
                    if persist_cache:
                        _save_cache_to_disk(cache_path, cache)

                st.dataframe(score_df_view, use_container_width=True)

                all_rows.append(score_df_view)

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

        progress.progress(100)
        status.write("完了")

    except Exception as e:
        st.error(f"実行中にエラーが発生しました: {e}")


if __name__ == "__main__":
    main()
