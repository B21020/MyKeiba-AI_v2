"""段階2・施策A：市場だけの基準と能力モデル・結合モデルを、同じ fold・同じ指標で比べる。

各 fold（modules/evaluation/_folds.py の5区分）で:
  1. 能力モデル F（LightGBM、目的変数=1着、オッズ・人気は入力しない）を fit で学習し、select で early stopping
  2. calib で条件付きロジットを推定
       market : log(市場確率 q)                      … 本命–大穴バイアスを補正した市場基準
       model  : log(F の確率)                         … 能力モデル単独
       combined: log(F の確率) + log(q)              … Benter の第2段
  3. betsel で各候補の購入条件（p × オッズ > 1 + margin の margin）を選ぶ
  4. outer で確率の指標と、固定100円の単勝回収率を測る（選んだ条件は変えない）
比較用に「毎レース1番人気（オッズ最小）の単勝」も記録する。

注意: オッズは確定オッズ（data の `単勝`）。判断時点のオッズではないので、回収率は参考評価。
最終確認期間（2026-07-01 以降）は既定で読まない。

使い方:
  .venv/Scripts/python.exe tools/run_stage2_market_baseline.py --out models/20261024/stage2_market_baseline
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import (  # noqa: E402
    FINAL_PERIOD_START, ConditionalLogit, bootstrap_by_day, brier, make_rolling_folds, pseudo_r2,
    race_log_loss, tansho_payouts, top1_accuracy, win_auc,
)
from modules.training import align_to_schema, load_schema  # noqa: E402

DEFAULT_OUTER = ['2023-01-01', '2023-07-01', '2024-01-01', '2024-07-01', '2025-01-01', '2025-07-01', '2026-01-01']
MARGINS = [0.0, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0]
CANDIDATES = {'market': ['log_q'], 'model': ['log_pF'], 'combined': ['log_pF', 'log_q']}
LGB_PARAMS = dict(objective='binary', learning_rate=0.05, num_leaves=63, min_child_samples=100,
                  subsample=0.8, subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0,
                  n_estimators=3000, verbose=-1, n_jobs=-1)
MIN_BETS_BETSEL = 100


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    return h.hexdigest()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_inputs(args):
    fd = pd.read_pickle(ROOT / args.featured)
    fd['race_id'] = fd['race_id'].astype(str)
    fd['date'] = pd.to_datetime(fd['date'])
    if fd['date'].max() >= FINAL_PERIOD_START and not args.allow_final:
        fd = fd[fd['date'] < FINAL_PERIOD_START].copy()  # 最終確認期間は読み込み直後に捨てる
    inc = pd.read_csv(ROOT / args.race_inclusion, dtype={'race_id': str})
    eval_ids = set(inc.loc[inc['include_eval'], 'race_id'])
    fd['is_eval'] = fd['race_id'].isin(eval_ids)
    pays = tansho_payouts(pd.read_pickle(ROOT / args.return_tables))
    fd = fd.merge(pays, on=['race_id', '馬番'], how='left')
    fd['tansho_payout'] = fd['tansho_payout'].fillna(0.0)
    # 市場確率: レース内で 1/odds を正規化
    inv = 1.0 / fd['単勝'].astype(float)
    fd['q'] = inv / inv.groupby(fd['race_id']).transform('sum')
    fd['log_q'] = np.log(fd['q'])
    fd['is_fav'] = fd.groupby('race_id')['単勝'].rank(method='first') == 1
    schema = load_schema(ROOT / args.schema)
    if args.drop_feature_prefix:
        drop = [c for c in schema['columns'] if c.startswith(tuple(args.drop_feature_prefix))]
        schema = {**schema, 'columns': [c for c in schema['columns'] if c not in set(drop)],
                  'categories': {k: v for k, v in schema['categories'].items() if k not in set(drop)},
                  'dropped_columns': drop}
    return fd, schema


def train_fundamental(fd, schema, fold):
    fit = fd[fold.mask(fd['date'], 'fit')]
    sel = fd[fold.mask(fd['date'], 'select')]
    Xf, Xs = align_to_schema(fit, schema), align_to_schema(sel, schema)
    model = lgb.LGBMClassifier(**LGB_PARAMS)
    model.fit(Xf, fit['win'], eval_set=[(Xs, sel['win'])], eval_metric='binary_logloss',
              callbacks=[lgb.early_stopping(100, verbose=False)])
    return model, {'fit_rows': len(fit), 'select_rows': len(sel), 'best_iteration': int(model.best_iteration_)}


def choose_margin(seg, col):
    """betsel で回収率が最も高い margin（購入数が MIN_BETS_BETSEL 以上の中から）。

    該当が無ければ None（outer では買わない）。
    """
    best = None
    rows = []
    for m in MARGINS:
        bet = seg[seg[col] * seg['単勝'] > 1 + m]
        n, ret = len(bet), bet['tansho_payout'].sum()
        roi = ret / (100 * n) if n else np.nan
        rows.append({'margin': m, 'bets': n, 'roi': roi})
        if n >= MIN_BETS_BETSEL and (best is None or roi > best[1]):
            best = (m, roi)
    return (best[0] if best else None), rows


def run_fold(fd, schema, fold):
    log(f'{fold.name}: training F')
    model, info = train_fundamental(fd, schema, fold)
    parts = {}
    for seg in ('calib', 'betsel', 'outer'):
        d = fd[fold.mask(fd['date'], seg) & fd['is_eval']].copy()
        raw = model.predict_proba(align_to_schema(d, schema))[:, 1]
        d['pF_raw'] = raw
        d['log_pF'] = np.log(np.clip(raw, 1e-9, 1))
        parts[seg] = d
    cal = parts['calib']
    coefs, margins, margin_tables = {}, {}, {}
    for name, cols in CANDIDATES.items():
        cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['win'])
        coefs[name] = dict(zip(cols, map(float, cl.coef_)))
        for seg in ('calib', 'betsel', 'outer'):
            parts[seg][f'p_{name}'] = cl.predict_proba(parts[seg][cols], parts[seg]['race_id'])
        margins[name], margin_tables[name] = choose_margin(parts['betsel'], f'p_{name}')
    info.update(coefs=coefs, margins=margins, margin_tables=margin_tables)

    # calib 上の Benter 型 ΔR²（参考）と outer の指標
    metrics = {}
    for seg in ('calib', 'outer'):
        d = parts[seg]
        m = {'races': int(d['race_id'].nunique()), 'runners': len(d)}
        for name in ['q', *[f'p_{c}' for c in CANDIDATES]]:
            key = 'market_raw' if name == 'q' else name[2:]
            m[key] = {'pseudo_r2': pseudo_r2(d[name], d['race_id'], d['win']),
                      'log_loss': race_log_loss(d[name], d['race_id'], d['win'])}
            if seg == 'outer':
                m[key].update(auc=win_auc(d[name], d['win']), brier=brier(d[name], d['win']),
                              top1=top1_accuracy(d[name], d['race_id'], d['win']))
        m['delta_r2_combined_vs_market'] = m['combined']['pseudo_r2'] - m['market']['pseudo_r2']
        metrics[seg] = m
    info['metrics'] = metrics
    out = parts['outer']
    out['fold'] = fold.name
    return out, info


def evaluate_bets(outer_all, fold_infos):
    """outer で、各候補の EV 条件（fold ごとに betsel で選んだ margin）と1番人気の単勝を固定100円で買う。"""
    bets = []
    for fold, d in outer_all.groupby('fold'):
        margins = fold_infos[fold]['margins']
        for name in CANDIDATES:
            if margins[name] is None:  # betsel で購入数の下限を満たす条件が無かった
                continue
            b = d[d[f'p_{name}'] * d['単勝'] > 1 + margins[name]]
            bets.append(b.assign(strategy=f'ev_{name}', margin=margins[name]))
        bets.append(d[d['is_fav']].assign(strategy='favorite', margin=np.nan))
    bets = pd.concat(bets)
    bets['stake'] = 100.0
    bets['payout'] = bets['tansho_payout']
    return bets


def summarize_bets(bets, outer_all):
    days = pd.Index(sorted(outer_all['date'].unique()), name='date')
    daily = bets.pivot_table(index='date', columns='strategy', values=['stake', 'payout'], aggfunc='sum').reindex(days).fillna(0)
    daily.columns = [f'{s}_{v}' for v, s in daily.columns]
    summary = {}
    for s in [f'ev_{c}' for c in CANDIDATES] + ['favorite']:
        if f'{s}_stake' not in daily.columns:
            summary[s] = {'bets': 0, 'roi': None, 'note': 'betsel で購入数の下限を満たす条件が無く、どの fold でも買わなかった'}
            continue
        d = pd.DataFrame({'stake': daily[f'{s}_stake'], 'payout': daily[f'{s}_payout']})
        b = bets[bets['strategy'] == s]
        res = bootstrap_by_day(d)
        top = b.nlargest(1, 'payout')['payout'].sum()
        res.update(bets=int(len(b)), races=int(b['race_id'].nunique()), days=int((d['stake'] > 0).sum()),
                   hit_rate=float((b['payout'] > 0).mean()) if len(b) else float('nan'),
                   roi_without_top_hit=float((b['payout'].sum() - top) / b['stake'].sum()) if len(b) else float('nan'))
        cum = (d['payout'] - d['stake']).cumsum()
        res['max_drawdown_yen'] = float((cum.cummax() - cum).max())
        summary[s] = res
    if {'ev_combined_stake', 'ev_market_stake'} <= set(daily.columns):
        summary['ev_combined_minus_ev_market'] = bootstrap_by_day(
            daily.rename(columns=lambda c: c.replace('ev_combined', 'a').replace('ev_market', 'b')),
            stake_col='a_stake', return_col='a_payout', compare=('a', 'b'))
    return daily.reset_index(), summary


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    ap.add_argument('--featured', default='data/tmp/featured_data_20261024_stage1.pickle')
    ap.add_argument('--schema', default='reproducibility/20261024_stage1/feature_schema.json')
    ap.add_argument('--race-inclusion', default='reproducibility/20261024_stage0_audit/race_inclusion.csv')
    ap.add_argument('--return-tables', default='data/raw/return_tables.pickle')
    ap.add_argument('--outer', nargs='*', default=DEFAULT_OUTER)
    ap.add_argument('--fit-years', type=int, default=4)
    ap.add_argument('--allow-final', action='store_true', help='最終確認期間を読む（ユーザーの決定後のみ）')
    ap.add_argument('--drop-feature-prefix', nargs='*', default=[],
                    help='能力モデルの入力から外す列の接頭辞（例: horse_id peds_）。診断用')
    args = ap.parse_args()
    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)

    folds = make_rolling_folds(args.outer, fit_years=args.fit_years, allow_final=args.allow_final)
    config = {
        'experiment': 'stage2_market_baseline', 'hypothesis': '能力モデルは補正済み市場基準に対して確率・回収で上積みがあるか（施策A）',
        'folds': [f.to_dict() for f in folds], 'candidates': CANDIDATES, 'margins_grid': MARGINS,
        'min_bets_betsel': MIN_BETS_BETSEL, 'lgb_params': LGB_PARAMS, 'odds': 'final odds (参考評価)',
        'final_period_start': str(FINAL_PERIOD_START.date()), 'allow_final': args.allow_final,
        'drop_feature_prefix': args.drop_feature_prefix,
        'comparisons': 4 + 1, 'git_head': subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True).stdout.strip(),
    }
    (out / 'config.json').write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
    manifest = {k: {'path': v, 'sha256': sha256(ROOT / v)} for k, v in
                {'featured': args.featured, 'schema': args.schema, 'race_inclusion': args.race_inclusion,
                 'return_tables': args.return_tables}.items()}
    (out / 'data_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')

    log('loading')
    fd, schema = load_inputs(args)
    outer_parts, infos = [], {}
    for fold in folds:
        part, info = run_fold(fd, schema, fold)
        outer_parts.append(part)
        infos[fold.name] = info
        log(f"{fold.name}: ΔR²(outer)={info['metrics']['outer']['delta_r2_combined_vs_market']:+.4f} "
            f"margins={info['margins']}")
    outer_all = pd.concat(outer_parts)

    keep = ['fold', 'race_id', 'date', '馬番', 'win', 'rank', '単勝', 'q', 'pF_raw', 'p_market', 'p_model', 'p_combined',
            'tansho_payout', 'is_fav']
    outer_all[keep].to_parquet(out / 'predictions.parquet', index=False)
    bets = evaluate_bets(outer_all, infos)
    bets[['strategy', 'fold', 'race_id', 'date', '馬番', '単勝', 'p_market', 'p_model', 'p_combined', 'margin', 'stake', 'payout']] \
        .to_csv(out / 'bets.csv', index=False, encoding='utf-8')
    daily, bet_summary = summarize_bets(bets, outer_all)
    daily.to_csv(out / 'returns_by_day.csv', index=False, encoding='utf-8')

    overall = {}
    for key, col in [('market_raw', 'q'), ('market', 'p_market'), ('model', 'p_model'), ('combined', 'p_combined')]:
        overall[key] = {'pseudo_r2': pseudo_r2(outer_all[col], outer_all['race_id'], outer_all['win']),
                        'log_loss': race_log_loss(outer_all[col], outer_all['race_id'], outer_all['win']),
                        'auc': win_auc(outer_all[col], outer_all['win']), 'brier': brier(outer_all[col], outer_all['win']),
                        'top1': top1_accuracy(outer_all[col], outer_all['race_id'], outer_all['win'])}
    overall['delta_r2_combined_vs_market'] = overall['combined']['pseudo_r2'] - overall['market']['pseudo_r2']
    per_fold_delta = {k: v['metrics']['outer']['delta_r2_combined_vs_market'] for k, v in infos.items()}
    metrics = {'overall_outer': overall, 'per_fold_delta_r2_outer': per_fold_delta,
               'folds_with_positive_delta_r2': int(sum(v > 0 for v in per_fold_delta.values())),
               'bets': bet_summary, 'folds': infos}
    (out / 'metrics.json').write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    log('done')
    print(json.dumps({'overall_outer': overall, 'per_fold_delta_r2_outer': per_fold_delta,
                      'bets': {k: {kk: v[kk] for kk in ('roi', 'roi_ci95', 'bets') if kk in v} if 'bets' in v else v
                               for k, v in bet_summary.items()}}, ensure_ascii=False, indent=2, default=float))


if __name__ == '__main__':
    main()
