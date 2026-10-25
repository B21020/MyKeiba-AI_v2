"""特徴量を1つの手順で作り直す（再開計画 段階1）。

main.ipynb のセル38〜44（前処理 → DataMerger → FeatureEngineering）と同じ手順に、
これまで別スクリプトでだけ追加していたレース内相対特徴量（_zR / _rnkR）を加える。

- 入力の対象レースは段階0監査の race_inclusion.csv の include_train（障害を除く平地、データ欠落なし）
- 馬戦績は tools/prepare_horse_results.py で作った重複除去・差分合成済みの中間ファイル
- 出力: 特徴量 pickle、feature_schema.json（学習に使う列の定義）、manifest.json（入力のハッシュ・件数・検証結果）
- 既存の data/tmp/featured_data_*.pickle や data/tmp/jockey_stats.pickle は上書きしない

使い方:
  .venv/Scripts/python.exe tools/build_features.py --tag 20261024_stage1
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules import preprocessing  # noqa: E402
from modules.constants import HorseResultsCols, ResultsCols  # noqa: E402
from modules.training import build_schema, save_schema  # noqa: E402

# main.ipynb セル42 と同じ
TARGET_COLS = [
    HorseResultsCols.RANK, HorseResultsCols.PRIZE, HorseResultsCols.RANK_DIFF,
    'first_corner', 'final_corner', 'first_to_rank', 'first_to_final', 'final_to_rank', 'time_seconds',
]
GROUP_COLS = ['course_len', 'race_type', HorseResultsCols.PLACE]
# DataSplitter が学習の特徴量から除く列
NON_FEATURE_COLS = ['rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']
# 段階0で欠損100%だった年があった列。作り直し後の欠損率を確認する
CHECK_MISSING = [ResultsCols.TANSHO_ODDS, ResultsCols.WAKUBAN, '体重', '体重変化']
MAX_MISSING_PCT = 2.0


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    return h.hexdigest()


def rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def build(args) -> dict:
    inputs = {
        'results': ROOT / 'data/raw/results.pickle',
        'race_info': ROOT / 'data/raw/race_info.pickle',
        'horse_info': ROOT / 'data/raw/horse_info.pickle',
        'peds': ROOT / 'data/raw/peds.pickle',
        'horse_results': ROOT / args.horse_results,
        'race_inclusion': ROOT / args.race_inclusion,
    }
    manifest ={'tag': args.tag, 'started_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
                'git_head': subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True).stdout.strip(),
                'inputs': {k: {'path': rel(p), 'sha256': sha256(p)} for k, p in inputs.items()}}

    inc = pd.read_csv(inputs['race_inclusion'], dtype={'race_id': str})
    sel = inc['include_train']
    if args.date_from:
        sel &= inc['date'] >= args.date_from
    if args.date_to:
        sel &= inc['date'] <= args.date_to
    train_ids = set(inc.loc[sel, 'race_id'])
    manifest['races_include_train'] = len(train_ids)

    log('loading results')
    raw_results = pd.read_pickle(inputs['results'])
    raw_results = raw_results[raw_results.index.astype(str).isin(train_ids)]
    horse_results_raw = pd.read_pickle(inputs['horse_results'])

    log('preprocessing')
    results_p = preprocessing.ResultsProcessor(raw_data=raw_results)
    race_info_p = preprocessing.RaceInfoProcessor(filepath=str(inputs['race_info']))
    horse_results_p = preprocessing.HorseResultsProcessor(filepath=None, raw_data=horse_results_raw)
    horse_info_p = preprocessing.HorseInfoProcessor(filepath=str(inputs['horse_info']))
    peds_p = preprocessing.PedsProcessor(filepath=str(inputs['peds']))
    log('jockey stats')
    jockey_stats = preprocessing.JockeyStatsProcessor(raw_data=horse_results_raw).preprocessed_data
    del horse_results_raw, raw_results
    n_results = len(results_p.preprocessed_data)
    manifest['result_rows_after_preprocess'] = n_results

    log('merging (per race date)')
    merger = preprocessing.DataMerger(results_p, race_info_p, horse_results_p, horse_info_p, peds_p,
                                      target_cols=TARGET_COLS, group_cols=GROUP_COLS, jockey_stats=jockey_stats)
    merger.merge()
    if len(merger.merged_data) != n_results:
        raise RuntimeError(f'row count changed in merge: {n_results} -> {len(merger.merged_data)}')

    log('feature engineering')
    fe = preprocessing.FeatureEngineering(merger)\
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
        .dumminize_race_class()\
        .add_race_relative_features()
    fd = fe.featured_data
    return validate_and_save(fd, manifest, args)


def validate_and_save(fd: pd.DataFrame, manifest: dict, args) -> dict:
    checks = {}
    fd['date'] = pd.to_datetime(fd['date'])
    checks['rows'] = len(fd)
    checks['races'] = int(fd['race_id'].nunique())
    checks['date_min'] = str(fd['date'].min().date())
    checks['date_max'] = str(fd['date'].max().date())
    checks['duplicate_race_umaban'] = int(fd.duplicated(['race_id', ResultsCols.UMABAN]).sum())
    year = fd['date'].dt.year
    miss = fd.groupby(year)[[c for c in CHECK_MISSING if c in fd.columns]].apply(lambda x: (x.isna().mean() * 100).round(2))
    checks['missing_pct_by_year'] = {str(y): row.to_dict() for y, row in miss.iterrows()}
    checks['missing_ok'] = bool((miss <= MAX_MISSING_PCT).all().all())
    p11 = json.loads((ROOT / 'models/20260210/p11/selected_positive_perm.json').read_text(encoding='utf-8'))
    checks['p11_positive_perm_missing'] = [c for c in p11 if c not in fd.columns]
    checks['labels'] = {'rank': fd['rank'].value_counts().to_dict(), 'win': fd['win'].value_counts().to_dict()}
    manifest['checks'] = checks
    problems = []
    if checks['duplicate_race_umaban']:
        problems.append('duplicate (race_id, 馬番)')
    if not checks['missing_ok']:
        problems.append(f'missing rate above {MAX_MISSING_PCT}% in some year')
    if checks['p11_positive_perm_missing']:
        problems.append(f"{len(checks['p11_positive_perm_missing'])} P11 columns missing")
    manifest['problems'] = problems

    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    feature_cols = [c for c in fd.columns if c not in NON_FEATURE_COLS]
    schema = build_schema(fd[feature_cols])
    schema['non_feature_columns'] = [c for c in NON_FEATURE_COLS if c in fd.columns]
    save_schema(schema, out_dir / 'feature_schema.json')
    featured_path = ROOT / args.featured_out.format(tag=args.tag)
    fd.to_pickle(featured_path)
    manifest['outputs'] = {'featured': rel(featured_path), 'sha256': sha256(featured_path),
                           'columns': fd.shape[1], 'feature_columns': len(feature_cols),
                           'schema_fingerprint': schema['fingerprint']}
    manifest['finished_at'] = time.strftime('%Y-%m-%dT%H:%M:%S')
    (out_dir / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    log(f"saved {featured_path.name}: {fd.shape}, problems={problems}")
    return manifest


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--tag', required=True)
    ap.add_argument('--horse-results', default='data/interim/horse_results_merged.pickle')
    ap.add_argument('--race-inclusion', default='reproducibility/20261024_stage0_audit/race_inclusion.csv')
    ap.add_argument('--out-dir', default='reproducibility/20261024_stage1')
    ap.add_argument('--featured-out', default='data/tmp/featured_data_{tag}.pickle')
    ap.add_argument('--date-from', default=None, help='動作確認用: この日付以降のレースだけ作る (YYYY-MM-DD)')
    ap.add_argument('--date-to', default=None, help='動作確認用: この日付以前のレースだけ作る (YYYY-MM-DD)')
    args = ap.parse_args()
    manifest = build(args)
    print(json.dumps(manifest['checks'], ensure_ascii=False, indent=2, default=str))
    sys.exit(1 if manifest['problems'] else 0)


if __name__ == '__main__':
    main()
