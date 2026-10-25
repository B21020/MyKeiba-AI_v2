"""古いパイプライン（orig＝9863b64、feb＝eee8b68）を、その worktree の modules のまま動かし、fold ごとに学習して予測スコアを書き出す。

- 作業ディレクトリを worktree に移してから、その worktree の modules を import する（現在の modules は使わない）
- 特徴量は各パイプラインの手順で全期間を1回だけ作る（main.ipynb の前処理 → DataMerger → FeatureEngineering）
  - feb は騎手成績（JockeyStatsProcessor）を先に作る（2月のセル39と同じ順）
- fold ごとに cutoff より前のデータだけで、各パイプライン自身の手順で学習する
  - orig：KeibaAIFactory.create(既定 test 0.3 / valid 0.3) → train_with_tuning()
  - feb ：P11 の145列（存在するもの）＋システム列 → 20260212 のパラメータ → train_without_tuning()（投票用モデルと同じ手順）
- 予測は cutoff 〜 cutoff+4か月（確率補正＋評価の区間）。orig は学習時と同じ `.values` で予測する（既知の表現の食い違いを避ける）
- 出力：<out>/scores_<pipeline>.csv（race_id, 馬番, date, cutoff, score）と <out>/train_log_<pipeline>.json

使い方（worktree ごとに、その環境の python で）:
  <venv_orig>/python tools/pipeline_compare/build_and_train.py --pipeline orig --worktree <wt_orig> --out <dir>
  .venv/Scripts/python.exe tools/pipeline_compare/build_and_train.py --pipeline feb --worktree <wt_feb> --out <dir>
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
CUTOFFS = ['2025-11-01', '2026-01-01', '2026-03-01']
HORIZON_MONTHS = 4
FINAL_PERIOD_START = pd.Timestamp('2026-07-01')
FEB_SELECTED = REPO / 'models/20260210/p11/selected_positive_perm.json'
FEB_PARAMS = REPO / 'models/20260212/p11_ext/optuna_best_params.json'
FEB_KEEP_ALWAYS = {'rank', 'win', 'longshot', 'date', '単勝', '馬番', 'race_id', 'horse_id'}
ORIG_DROP = ['rank', 'date', '単勝']


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build_features(pipeline: str, preprocessing, LocalPaths, HorseResultsCols) -> pd.DataFrame:
    rp = preprocessing.ResultsProcessor(filepath=LocalPaths.RAW_RESULTS_PATH)
    rip = preprocessing.RaceInfoProcessor(filepath=LocalPaths.RAW_RACE_INFO_PATH)
    hip = preprocessing.HorseInfoProcessor(filepath=LocalPaths.RAW_HORSE_INFO_PATH)
    hrp = preprocessing.HorseResultsProcessor(filepath=LocalPaths.RAW_HORSE_RESULTS_PATH)
    pp = preprocessing.PedsProcessor(filepath=LocalPaths.RAW_PEDS_PATH)
    if pipeline == 'feb':
        os.makedirs(LocalPaths.TMP_DIR, exist_ok=True)
        preprocessing.JockeyStatsProcessor(filepath=LocalPaths.RAW_HORSE_RESULTS_PATH) \
            .preprocessed_data.to_pickle(LocalPaths.JOCKEY_STATS_PATH)
    target_cols = [HorseResultsCols.RANK, HorseResultsCols.PRIZE, HorseResultsCols.RANK_DIFF,
                   'first_corner', 'final_corner', 'first_to_rank', 'first_to_final', 'final_to_rank', 'time_seconds']
    group_cols = ['course_len', 'race_type', HorseResultsCols.PLACE]
    dm = preprocessing.DataMerger(rp, rip, hrp, hip, pp, target_cols=target_cols, group_cols=group_cols)
    dm.merge()
    fe = preprocessing.FeatureEngineering(dm)\
        .add_interval()\
        .add_agedays()\
        .dumminize_ground_state()\
        .dumminize_race_type()\
        .dumminize_sex()\
        .dumminize_weather()\
        .encode_horse_id()\
        .encode_jockey_id()\
        .encode_trainer_id()\
        .encode_owner_id()\
        .encode_breeder_id()\
        .dumminize_kaisai()\
        .dumminize_around()\
        .dumminize_race_class()
    return fe.featured_data


def race_ids(df: pd.DataFrame) -> pd.Series:
    if 'race_id' in df.columns:
        return df['race_id'].astype(str)
    return pd.Series(df.index.astype(str), index=df.index)


def train_and_score(pipeline: str, training, fd: pd.DataFrame, cutoff: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    dates = pd.to_datetime(fd['date'])
    train = fd[dates < cutoff]
    end = cutoff + pd.DateOffset(months=HORIZON_MONTHS)
    assert end <= FINAL_PERIOD_START
    pred = fd[(dates >= cutoff) & (dates < end)]
    info = {'cutoff': str(cutoff.date()), 'train_rows': len(train), 'train_max_date': str(pd.to_datetime(train['date']).max().date()),
            'pred_rows': len(pred)}
    assert pd.to_datetime(train['date']).max() < cutoff
    if pipeline == 'orig':
        ai = training.KeibaAIFactory.create(train)
        ai.train_with_tuning()
        model = ai._KeibaAI__model_wrapper.lgb_model
        X = pred.drop(ORIG_DROP, axis=1)
        assert list(X.columns) == list(ai.datasets.X_train.columns)
        score = model.predict_proba(X.values)[:, 1]
        info['n_features'] = X.shape[1]
        info['params'] = {k: v for k, v in model.get_params().items() if isinstance(v, (int, float, str, type(None)))}
    else:
        selected = set(json.loads(FEB_SELECTED.read_text(encoding='utf-8')))
        keep = [c for c in fd.columns if c in (selected | FEB_KEEP_ALWAYS)]
        ai = training.KeibaAIFactory.create(train[keep])
        ai.set_params(json.loads(FEB_PARAMS.read_text(encoding='utf-8')))
        ai.train_without_tuning()
        model = ai._KeibaAI__model_wrapper.lgb_model
        cols = list(ai.datasets.X_train.columns)
        X = pred[keep].reindex(columns=cols)
        score = model.predict_proba(X)[:, 1]
        info['n_features'] = len(cols)
        info['features_missing_from_p11'] = len(selected - set(fd.columns))
    out = pd.DataFrame({'race_id': race_ids(pred).to_numpy(), '馬番': pd.to_numeric(pred['馬番']).to_numpy(),
                        'date': pd.to_datetime(pred['date']).dt.date.astype(str).to_numpy(),
                        'cutoff': info['cutoff'], 'score': score})
    return out, info


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--pipeline', choices=['orig', 'feb'], required=True)
    ap.add_argument('--worktree', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--cutoffs', nargs='*', default=CUTOFFS)
    args = ap.parse_args()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    wt = Path(args.worktree).resolve()
    os.chdir(wt)                      # LocalPaths は import 時の作業ディレクトリを基準にする
    sys.path.insert(0, str(wt))
    from modules import preprocessing, training  # noqa: E402  (worktree の modules)
    from modules.constants import HorseResultsCols, LocalPaths  # noqa: E402
    assert Path(preprocessing.__file__).resolve().is_relative_to(wt), 'modules must come from the worktree'

    cache = out / f'featured_{args.pipeline}.pickle'
    if cache.exists():
        fd = pd.read_pickle(cache)
        log(f'loaded cached features {fd.shape}')
    else:
        log('building features')
        fd = build_features(args.pipeline, preprocessing, LocalPaths, HorseResultsCols)
        fd.to_pickle(cache)
        log(f'features {fd.shape}')
    assert pd.to_datetime(fd['date']).max() < FINAL_PERIOD_START
    scores, infos = [], []
    for c in args.cutoffs:
        log(f'training cutoff {c}')
        s, info = train_and_score(args.pipeline, training, fd, pd.Timestamp(c))
        scores.append(s)
        infos.append(info)
        log(json.dumps({k: v for k, v in info.items() if k != 'params'}, ensure_ascii=False))
    pd.concat(scores).to_csv(out / f'scores_{args.pipeline}.csv', index=False, encoding='utf-8')
    (out / f'train_log_{args.pipeline}.json').write_text(
        json.dumps({'pipeline': args.pipeline, 'worktree': str(wt), 'feature_shape': list(fd.shape), 'folds': infos},
                   ensure_ascii=False, indent=2, default=str), encoding='utf-8')


if __name__ == '__main__':
    main()
