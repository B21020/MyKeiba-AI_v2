"""調教データの特徴量が、発走6分前の市場に情報を足すかを比べる（開発期間のみ）。

調教データ（JV-Link の通常データ）は 2025-10-21 以降しか無いため、能力モデルの学習には入れず、
Benter の第2段（レース内の条件付きロジット）に変数として足す:
  M0: log q6                          （発走6分前の市場）
  M1: log q6 + log F                  （市場 + 能力モデル base）
  M2: M1 + 調教変数
  M3: M0 + 調教変数
fold は tools/run_stage2_t6.py の --rolling と同じ3つ（2か月で推定 → 次の2か月で評価、評価は 2026年1〜6月）。
最終確認期間（2026-07-01 以降）のレースは読まない。

使い方:
  .venv/Scripts/python.exe tools/run_training_features_eval.py --out models/20261024/training_features
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START, ConditionalLogit, paired_delta_r2  # noqa: E402
from modules.preprocessing._training_features import load_workouts, training_features  # noqa: E402
from modules.training import align_to_schema  # noqa: E402
from tools.analyze_stage2_ability import define_groups, fit_model, sub_schema  # noqa: E402
from tools.run_measure_d_speed_index import raw_horse_ids  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import rolling_splits, seg_mask, t6_odds  # noqa: E402

TR_VARS = ['hill_best_fast', 'hill_last_lap1_fast', 'wood_last_fast', 'wood_last_lap1_fast',
           'has_hill', 'has_wood', 'log_tr_n14']
MODELS = {'M0': ['log_q6'], 'M1': ['log_q6', 'log_pF'], 'M2': ['log_q6', 'log_pF', *TR_VARS], 'M3': ['log_q6', *TR_VARS]}
COMPARE = {'M2_vs_M1': ('M2', 'M1'), 'M3_vs_M0': ('M3', 'M0'), 'M1_vs_M0': ('M1', 'M0')}
# 探索的な確認：調教変数を1つずつ市場に足す（7比較。結果を見てから選んだ変数は最終確認の候補にしない）
for _v in ('hill_best_fast', 'hill_last_lap1_fast', 'wood_last_fast', 'wood_last_lap1_fast', 'has_hill', 'has_wood', 'log_tr_n14'):
    MODELS[f'S_{_v}'] = ['log_q6', _v]
    COMPARE[f'S_{_v}_vs_M0'] = (f'S_{_v}', 'M0')


def add_training_vars(df: pd.DataFrame, workouts: pd.DataFrame) -> pd.DataFrame:
    f = training_features(workouts, df[['horse_raw', 'date']].rename(columns={'horse_raw': 'horse_id'}))
    out = pd.concat([df, f], axis=1)
    out['has_hill'] = (out['hill_n14'] > 0).astype(float)
    out['has_wood'] = (out['wood_n14'] > 0).astype(float)
    out['log_tr_n14'] = np.log1p(out['tr_n14'])
    for c in ('hill_best_fast', 'hill_last_lap1_fast', 'wood_last_fast', 'wood_last_lap1_fast'):
        out[c] = out[c].fillna(0.0).clip(-4, 4)
    return out


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out_dir = ROOT / a.out
    out_dir.mkdir(parents=True, exist_ok=True)
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, schema = load_inputs(args)
    assert fd['date'].max() < FINAL_PERIOD_START
    fd['horse_raw'] = raw_horse_ids(fd)
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp('2025-11-01')) & fd['is_eval'], 'race_id'])
    o6 = t6_odds(dev_ids)
    d = fd.merge(o6, on=['race_id', '馬番'], how='left')
    ok = d[d['race_id'].isin(dev_ids)].groupby('race_id')['odds_t6'].apply(lambda s: s.notna().all() and (s > 0).all())
    usable = set(ok[ok].index)
    inv = 1.0 / d['odds_t6']
    d['log_q6'] = np.log(inv / inv.groupby(d['race_id']).transform('sum'))

    hill = pd.read_csv(ROOT / 'data/jv_training/training_SLOP.csv', dtype={'horse_id': str, 'make_date': str, 'time': str, 'tresen': str})
    wood = pd.read_csv(ROOT / 'data/jv_training/training_WOOD.csv', dtype={'horse_id': str, 'make_date': str, 'time': str,
                                                                          'tresen': str, 'course': str})
    workouts = load_workouts(hill, wood)

    groups = define_groups(schema['columns'])
    sch = sub_schema(schema, groups['form'] + groups['position'] + groups['cond_history'] + groups['jockey_stats'])
    summary = {'models': MODELS, 'training_vars': TR_VARS, 'workouts': len(workouts),
               'workout_range': [str(workouts['date'].min().date()), str(workouts['date'].max().date())], 'folds': {}}
    pooled = []
    for sp in rolling_splits():
        seg = {s: d[seg_mask(d['date'], s, sp)] for s in ('fit', 'select')}
        cal = d[seg_mask(d['date'], 'calib', sp) & d['race_id'].isin(usable)].copy()
        out = d[seg_mask(d['date'], 'outer', sp) & d['race_id'].isin(usable)].copy()
        model = fit_model({s: align_to_schema(seg[s], sch) for s in seg}, seg)
        for df in (cal, out):
            df['log_pF'] = np.log(np.clip(model.predict_proba(align_to_schema(df, sch))[:, 1], 1e-9, 1))
        cal, out = add_training_vars(cal, workouts), add_training_vars(out, workouts)
        probs, coefs = {}, {}
        for name, cols in MODELS.items():
            cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['win'])
            coefs[name] = dict(zip(cols, map(float, cl.coef_)))
            probs[name] = cl.predict_proba(out[cols], out['race_id'])
        res = {k: paired_delta_r2(probs[x], probs[y], out['race_id'], out['win']) for k, (x, y) in COMPARE.items()}
        cover = {'any_workout_14d': float((out['tr_n14'] > 0).mean()), 'hill': float(out['has_hill'].mean()),
                 'wood': float(out['has_wood'].mean())}
        key = f"outer_{sp['outer'][0]}"
        summary['folds'][key] = {'split': sp, 'calib_races': int(cal['race_id'].nunique()),
                                 'outer_races': int(out['race_id'].nunique()), 'coverage_outer': cover,
                                 'coefs': coefs, 'delta_r2': res}
        pooled.append(pd.DataFrame({'race_id': out['race_id'].to_numpy(), 'win': out['win'].to_numpy(), **probs}))
        print(f"{key}: " + ' | '.join(f"{k}={v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}" for k, v in res.items())
              + f" | coverage {cover}", flush=True)
    P = pd.concat(pooled)
    summary['pooled'] = {k: paired_delta_r2(P[x], P[y], P['race_id'], P['win']) for k, (x, y) in COMPARE.items()}
    print('pooled: ' + ' | '.join(f"{k}={v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}" for k, v in summary['pooled'].items()))
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    for k, f in summary['folds'].items():
        print(k, 'M3 coefs', {c: round(v, 3) for c, v in f['coefs']['M3'].items()})


if __name__ == '__main__':
    main()
