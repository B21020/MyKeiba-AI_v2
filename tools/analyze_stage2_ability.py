"""段階2の原因分析：能力モデルが確定オッズの市場基準に上積みできない理由を、特徴量群ごとに調べる。

すべて tools/run_stage2_market_baseline.py と同じ fold・同じ評価対象・同じ市場基準（calib で推定した log q の条件付きロジット）を使う。

1. ID系カテゴリ列の扱い（all / noid / keep_jt / capped_ids）
2. 特徴量群だけで学習した能力モデル F_G を市場と結合したときの上積み（群ごとの ΔR²）
3. 市場オッズを初期値（オフセット）にして残差だけを学習するモデル：早期終了の本数と、どの群で分割したか（gain）
ΔR² は外側期間で、結合モデル − 市場基準。fold別と全体に、レース単位の対の再標本化で95%区間を付ける。
最終確認期間（2026-07-01 以降）は読まない。

使い方:
  .venv/Scripts/python.exe tools/analyze_stage2_ability.py --out models/20261024/stage2_ability_analysis
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import ConditionalLogit, make_rolling_folds, paired_delta_r2  # noqa: E402
from modules.training import align_to_schema  # noqa: E402
from tools.run_stage2_market_baseline import DEFAULT_OUTER, LGB_PARAMS, load_inputs  # noqa: E402

ID_COLS = ['horse_id', 'jockey_id', 'trainer_id', 'owner_id', 'breeder_id']
COND_RE = re.compile(r'_(course_len|race_type|開催)_(\d+R|allR)$')
MIN_ID_COUNT = 50


def define_groups(columns: list[str]) -> dict[str, list[str]]:
    g = {k: [] for k in ('id_horse', 'id_connections', 'peds', 'relative', 'cond_history', 'form', 'time',
                         'position', 'jockey_stats', 'horse_state', 'race_context')}
    for c in columns:
        if c == 'horse_id':
            g['id_horse'].append(c)
        elif c in ID_COLS:
            g['id_connections'].append(c)
        elif c.startswith('peds_'):
            g['peds'].append(c)
        elif c.endswith('_zR') or c.endswith('_rnkR'):
            g['relative'].append(c)
        elif COND_RE.search(c):
            g['cond_history'].append(c)
        elif re.match(r'(着順|着差|賞金)_(5R|9R|allR)$', c):
            g['form'].append(c)
        elif c.startswith('time_seconds_'):
            g['time'].append(c)
        elif c.startswith(('first_', 'final_')):
            g['position'].append(c)
        elif c.startswith('jockey_'):
            g['jockey_stats'].append(c)
        elif c in ('枠番', '斤量', '年齢', '体重', '体重変化', 'interval', 'age_days') or c.startswith('性_'):
            g['horse_state'].append(c)
        else:
            g['race_context'].append(c)
    return g


def sub_schema(schema: dict, cols: list[str]) -> dict:
    keep = set(cols)
    return {**schema, 'columns': [c for c in schema['columns'] if c in keep],
            'categories': {k: v for k, v in schema['categories'].items() if k in keep}}


def cap_ids(X_by_seg: dict, cols: list[str], fit_key='fit') -> dict:
    """fit で出現回数が MIN_ID_COUNT 未満のカテゴリを1つ（-1）にまとめる。"""
    out = {k: v.copy() for k, v in X_by_seg.items()}
    for c in cols:
        counts = out[fit_key][c].astype('float').value_counts()
        frequent = set(counts[counts >= MIN_ID_COUNT].index)
        cats = sorted(frequent) + [-1.0]
        for k in out:
            v = out[k][c].astype('float')
            out[k][c] = pd.Categorical(v.where(v.isin(frequent), -1.0), categories=cats)
    return out


def segments(fd, fold):
    seg = {s: fd[fold.mask(fd['date'], s)] for s in ('fit', 'select')}
    for s in ('calib', 'outer'):
        seg[s] = fd[fold.mask(fd['date'], s) & fd['is_eval']]
    return seg


def market_probs(seg):
    cl = ConditionalLogit().fit(seg['calib'][['log_q']], seg['calib']['race_id'], seg['calib']['win'])
    return cl.predict_proba(seg['outer'][['log_q']], seg['outer']['race_id'])


def fit_model(X, seg, residual=False):
    params = dict(LGB_PARAMS)
    model = lgb.LGBMClassifier(**params)
    kw = {}
    if residual:
        logit = lambda q: np.log(q / (1 - q))  # noqa: E731
        kw = dict(init_score=logit(seg['fit']['q'].clip(1e-6, 1 - 1e-6)),
                  eval_init_score=[logit(seg['select']['q'].clip(1e-6, 1 - 1e-6))])
    model.fit(X['fit'], seg['fit']['win'], eval_set=[(X['select'], seg['select']['win'])],
              eval_metric='binary_logloss', callbacks=[lgb.early_stopping(100, verbose=False)], **kw)
    return model


def combined_outer(model, X, seg, residual=False):
    """calib で log q と log F を結合し、outer の確率を返す。残差モデルは log(残差補正後の確率) 1変数。"""
    res = {}
    for s in ('calib', 'outer'):
        d = seg[s]
        if residual:
            raw = model.predict(X[s], raw_score=True) + np.log(d['q'] / (1 - d['q'])).to_numpy()
            p = 1 / (1 + np.exp(-raw))
            res[s] = pd.DataFrame({'x1': np.log(np.clip(p, 1e-9, 1))}, index=d.index)
        else:
            p = model.predict_proba(X[s])[:, 1]
            res[s] = pd.DataFrame({'x1': np.log(np.clip(p, 1e-9, 1)), 'log_q': d['log_q']}, index=d.index)
    cl = ConditionalLogit().fit(res['calib'], seg['calib']['race_id'], seg['calib']['win'])
    return cl.predict_proba(res['outer'], seg['outer']['race_id']), dict(zip(res['calib'].columns, map(float, cl.coef_)))


def run_config(name, cols, fd, schema, folds, *, residual=False, capped=False):
    sch = sub_schema(schema, cols)
    per_fold, outer_parts, gains = {}, [], []
    for fold in folds:
        seg = segments(fd, fold)
        X = {s: align_to_schema(seg[s], sch) for s in seg}
        if capped:
            X = cap_ids(X, [c for c in sch['columns'] if c in ID_COLS or c.startswith('peds_')])
        model = fit_model(X, seg, residual=residual)
        p_comb, coef = combined_outer(model, X, seg, residual=residual)
        p_mkt = market_probs(seg)
        o = seg['outer']
        d = paired_delta_r2(p_comb, p_mkt, o['race_id'], o['win'])
        per_fold[fold.name] = {**d, 'best_iteration': int(model.best_iteration_), 'coef': coef}
        outer_parts.append(pd.DataFrame({'race_id': o['race_id'].to_numpy(), 'win': o['win'].to_numpy(),
                                         'p_comb': p_comb, 'p_mkt': p_mkt}))
        gains.append(pd.Series(model.booster_.feature_importance('gain'), index=model.booster_.feature_name()))
    allo = pd.concat(outer_parts)
    pooled = paired_delta_r2(allo['p_comb'], allo['p_mkt'], allo['race_id'], allo['win'])
    gain = pd.concat(gains, axis=1).sum(axis=1)
    return {'config': name, 'n_features': len(sch['columns']), 'residual': residual, 'capped_ids': capped,
            'pooled': pooled, 'folds_positive': int(sum(v['delta_r2'] > 0 for v in per_fold.values())),
            'folds_ci_above_zero': int(sum(v['ci95'][0] > 0 for v in per_fold.values())),
            'per_fold': per_fold}, gain


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    ap.add_argument('--only', nargs='*', default=None, help='実行する構成名だけに絞る')
    a = ap.parse_args()
    out = ROOT / a.out
    out.mkdir(parents=True, exist_ok=True)
    prev = out / 'results.json'
    kept = []
    if a.only and prev.exists():  # 一部だけ再実行するときは、他の構成の結果を残す
        old = json.loads(prev.read_text(encoding='utf-8'))
        kept = [r for r in old['results'] if r['config'] not in set(a.only)]
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, schema = load_inputs(args)
    folds = make_rolling_folds(DEFAULT_OUTER)
    groups = define_groups(schema['columns'])
    (out / 'feature_groups.json').write_text(json.dumps(groups, ensure_ascii=False, indent=2), encoding='utf-8')
    ids = groups['id_horse'] + groups['id_connections'] + groups['peds']
    noid = [c for c in schema['columns'] if c not in set(ids)]
    configs = [
        ('ids_all', schema['columns'], {}),
        ('ids_none', noid, {}),
        ('ids_keep_jockey_trainer', noid + ['jockey_id', 'trainer_id'], {}),
        ('ids_capped', schema['columns'], {'capped': True}),
        ('residual_on_market_noid', noid, {'residual': True}),
        *[(f'group_{g}', cols, {}) for g, cols in groups.items() if cols and g not in ('id_horse',)],
    ]
    if a.only:
        configs = [c for c in configs if c[0] in a.only]
    results, gain_rows = list(kept), []
    if kept and 'residual_on_market_noid' not in {c[0] for c in configs}:
        gain_rows = json.loads(prev.read_text(encoding='utf-8')).get('residual_gain_by_group', [])
    for name, cols, kw in configs:
        t0 = time.time()
        res, gain = run_config(name, cols, fd, schema, folds, **kw)
        res['seconds'] = round(time.time() - t0, 1)
        results.append(res)
        p = res['pooled']
        print(f"[{time.strftime('%H:%M:%S')}] {name:28s} n={res['n_features']:3d} ΔR²={p['delta_r2']:+.5f} "
              f"CI=[{p['ci95'][0]:+.5f},{p['ci95'][1]:+.5f}] folds+={res['folds_positive']}/7 "
              f"iters={[v['best_iteration'] for v in res['per_fold'].values()]}", flush=True)
        if kw.get('residual'):
            col_group = {c: g for g, cs in groups.items() for c in cs}
            gg = gain.groupby(gain.index.map(lambda c: col_group.get(c, 'other'))).sum().sort_values(ascending=False)
            gain_rows = [{'group': g, 'gain': float(v), 'share': float(v / gg.sum()) if gg.sum() else 0.0} for g, v in gg.items()]
        (out / 'results.json').write_text(json.dumps({'results': results, 'residual_gain_by_group': gain_rows},
                                                     ensure_ascii=False, indent=2), encoding='utf-8')
    rows = [{'config': r['config'], 'fold': f, **{k: v[k] for k in ('delta_r2', 'races', 'share_races_a_better', 'best_iteration')},
             'ci_lo': v['ci95'][0], 'ci_hi': v['ci95'][1], 'coef': json.dumps(v['coef'])}
            for r in results for f, v in r['per_fold'].items()]
    pd.DataFrame(rows).to_csv(out / 'fold_table.csv', index=False, encoding='utf-8')


if __name__ == '__main__':
    main()
