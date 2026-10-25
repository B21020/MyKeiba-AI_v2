"""施策D：条件補正した走破能力指数を加えると、市場への上積み（ΔR²）が増えるかを同じ fold で比べる。

構成（ID系は外す）:
  base       : 過去着順・着差・賞金 + 通過順 + 条件別の過去成績 + 騎手成績
  base_si    : base + 走破能力指数（SI_FEATURES と、その レース内 z・順位）
  si_only    : 走破能力指数だけ
基準タイムは fold ごとに「学習本体の期間の終わり（= モデル選択区分の開始）より前」の走だけで推定する。
評価は tools/analyze_stage2_ability.py と同じ（calib で log F + log q を結合 → outer で補正済み市場と比較）。
加えて base_si と base の結合確率どうしの差（同じレースで対）を出す。

使い方:
  .venv/Scripts/python.exe tools/run_measure_d_speed_index.py --out models/20261024/measure_d_speed_index
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import make_rolling_folds, paired_delta_r2  # noqa: E402
from modules.preprocessing._results_processor import coalesce_result_columns  # noqa: E402
from modules.preprocessing._speed_index import (  # noqa: E402
    SI_FEATURES, fit_base_times, horse_speed_features, parse_history, speed_index,
)
from modules.training import align_to_schema  # noqa: E402
from tools.analyze_stage2_ability import (  # noqa: E402
    combined_outer, define_groups, fit_model, market_probs, segments, sub_schema,
)
from tools.run_stage2_market_baseline import DEFAULT_OUTER, load_inputs  # noqa: E402

SI_REL = ['si_mean3_zR', 'si_mean5_zR', 'si_max5_zR', 'si_mean3_rnkR']


def raw_horse_ids(fd: pd.DataFrame) -> pd.Series:
    """特徴量の horse_id はラベル符号化されているので、結果表から (race_id, 馬番) で元の ID を引く。"""
    r = pd.read_pickle(ROOT / 'data/raw/results.pickle')
    r.index.name = 'race_id'
    r = coalesce_result_columns(r.reset_index().drop_duplicates())
    r['race_id'] = r['race_id'].astype(str)
    r['馬番'] = pd.to_numeric(r['馬番'], errors='coerce')
    key = r.dropna(subset=['馬番']).drop_duplicates(['race_id', '馬番'])[['race_id', '馬番', 'horse_id']]
    m = fd[['race_id', '馬番']].merge(key, on=['race_id', '馬番'], how='left')
    if m['horse_id'].isna().any():
        raise RuntimeError(f"{int(m['horse_id'].isna().sum())} rows without raw horse_id")
    return pd.Series(m['horse_id'].astype(str).to_numpy(), index=fd.index)


def add_relative(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ('si_mean3', 'si_mean5', 'si_max5'):
        g = out.groupby('race_id')[c]
        out[f'{c}_zR'] = (out[c] - g.transform('mean')) / g.transform('std').fillna(0).clip(lower=1e-6)
    out['si_mean3_rnkR'] = out.groupby('race_id')['si_mean3'].rank(ascending=False, method='min')
    return out


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    ap.add_argument('--horse-results', default='data/interim/horse_results_merged.pickle')
    a = ap.parse_args()
    out = ROOT / a.out
    out.mkdir(parents=True, exist_ok=True)
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, schema = load_inputs(args)
    fd['horse_raw'] = raw_horse_ids(fd)
    runs = parse_history(pd.read_pickle(ROOT / a.horse_results))
    groups = define_groups(schema['columns'])
    base_cols = groups['form'] + groups['position'] + groups['cond_history'] + groups['jockey_stats']
    base_schema = sub_schema(schema, base_cols)
    folds = make_rolling_folds(DEFAULT_OUTER)
    configs = {'base': (True, False), 'base_si': (True, True), 'si_only': (False, True)}
    res = {k: {'per_fold': {}, 'parts': []} for k in configs}
    diff_parts, coverage = [], {}
    for fold in folds:
        t0 = time.time()
        base = fit_base_times(runs, fold.fit[1])
        rsi = speed_index(runs, base)
        feats = horse_speed_features(rsi.rename(columns={'horse_id': 'horse_id'}),
                                     fd[['horse_raw', 'date']].rename(columns={'horse_raw': 'horse_id'}))
        d = add_relative(pd.concat([fd, feats], axis=1))
        seg = segments(d, fold)
        coverage[fold.name] = {s: float(seg[s]['si_last'].notna().mean()) for s in seg}
        coverage[fold.name]['base_cells'] = int(len(base))
        p_mkt = market_probs(seg)
        o = seg['outer']
        comb = {}
        for name, (use_base, use_si) in configs.items():
            X = {}
            for s in seg:
                parts = []
                if use_base:
                    parts.append(align_to_schema(seg[s], base_schema))
                if use_si:
                    parts.append(seg[s][SI_FEATURES + SI_REL].astype(float))
                X[s] = pd.concat(parts, axis=1)
            model = fit_model(X, seg)
            p_comb, coef = combined_outer(model, X, seg)
            comb[name] = p_comb
            dr = paired_delta_r2(p_comb, p_mkt, o['race_id'], o['win'])
            res[name]['per_fold'][fold.name] = {**dr, 'coef': coef, 'best_iteration': int(model.best_iteration_),
                                                'n_features': X['fit'].shape[1]}
            res[name]['parts'].append(pd.DataFrame({'race_id': o['race_id'].to_numpy(), 'win': o['win'].to_numpy(),
                                                    'p_comb': p_comb, 'p_mkt': p_mkt}))
            if name == 'base_si':
                imp = pd.Series(model.booster_.feature_importance('gain'), index=model.booster_.feature_name())
                res[name].setdefault('gain', []).append(imp)
        diff = paired_delta_r2(comb['base_si'], comb['base'], o['race_id'], o['win'])
        res['base_si']['per_fold'][fold.name]['vs_base'] = diff
        diff_parts.append(pd.DataFrame({'race_id': o['race_id'].to_numpy(), 'win': o['win'].to_numpy(),
                                        'a': comb['base_si'], 'b': comb['base']}))
        print(f"[{time.strftime('%H:%M:%S')}] {fold.name}: " + ' '.join(
            f"{k}={v['per_fold'][fold.name]['delta_r2']:+.5f}" for k, v in res.items())
            + f" | base_si−base={diff['delta_r2']:+.5f} | coverage outer={coverage[fold.name]['outer']:.3f} ({time.time()-t0:.0f}s)",
            flush=True)

    summary = {'configs': {}, 'coverage': coverage, 'base_columns': len(base_cols)}
    for name, r in res.items():
        allo = pd.concat(r['parts'])
        pooled = paired_delta_r2(allo['p_comb'], allo['p_mkt'], allo['race_id'], allo['win'])
        summary['configs'][name] = {'pooled_vs_market': pooled,
                                    'folds_positive': int(sum(v['delta_r2'] > 0 for v in r['per_fold'].values())),
                                    'per_fold': r['per_fold']}
    dd = pd.concat(diff_parts)
    summary['base_si_minus_base'] = paired_delta_r2(dd['a'], dd['b'], dd['race_id'], dd['win'])
    summary['base_si_minus_base']['folds_positive'] = int(sum(v['vs_base']['delta_r2'] > 0 for v in res['base_si']['per_fold'].values()))
    g = pd.concat(res['base_si']['gain'], axis=1).sum(axis=1)
    si_share = float(g[g.index.isin(SI_FEATURES + SI_REL)].sum() / g.sum())
    summary['base_si_gain_share_of_index'] = si_share
    summary['base_si_top_gain'] = {k: float(v) for k, v in g.sort_values(ascending=False).head(15).items()}
    (out / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    rows = [{'config': n, 'fold': f, 'delta_r2': v['delta_r2'], 'ci_lo': v['ci95'][0], 'ci_hi': v['ci95'][1],
             'best_iteration': v['best_iteration'], 'coef': json.dumps(v['coef'])}
            for n, r in res.items() for f, v in r['per_fold'].items()]
    pd.DataFrame(rows).to_csv(out / 'fold_table.csv', index=False, encoding='utf-8')
    print(json.dumps({n: {'ΔR²': round(c['pooled_vs_market']['delta_r2'], 5), 'ci': [round(x, 5) for x in c['pooled_vs_market']['ci95']],
                          'folds+': c['folds_positive']} for n, c in summary['configs'].items()}, ensure_ascii=False))
    print('base_si − base:', json.dumps(summary['base_si_minus_base']), 'index gain share:', round(si_share, 3))


if __name__ == '__main__':
    main()
