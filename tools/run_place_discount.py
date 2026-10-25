"""複勝の確率を Harville の代わりに Henery・Stern の型で出したとき、当たり具合と購入の結果がどう変わるか。

比べる型（どれも6分前の単勝の市場の勝つ確率 q6 から出す。複勝オッズは購入の期待値にだけ使う）:
  H     Harville（tools/run_place_market.py と同じ）
  D_fit べき乗の割引（Lo・Bacon-Shone）。2着 ∝ p^λ2、3着 ∝ p^λ3。λ は各 fold の calib で選ぶ（0.50〜1.00、0.05刻み）
  D_lbs べき乗の割引、λ2 = 0.76・λ3 = 0.62（文献でよく引かれる値。参考）
  N     Henery の正規モデル（シミュレーション）
  G_fit Stern のガンマモデル（シミュレーション）。形 r は各 fold の calib で選ぶ（2／4／8）

- fold・評価対象・購入の規則・閾値は tools/run_place_market.py と同じ。確率はロジスティック回帰で calib に合わせて補正
- 主な判定：補正した確率での pseudo-R² の差（各型 − H）と、閾値1.00の回収率
- 最終確認期間（2026-07-01 以降）は読まない

使い方:
  .venv/Scripts/python.exe tools/run_place_discount.py --out models/20261025/place_discount
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
from modules.evaluation import FINAL_PERIOD_START, discounted_place, simulated_place  # noqa: E402
from tools.run_place_market import (THRESHOLDS, bets_table, build, fit_logistic, logit, paired_place_r2,  # noqa: E402
                                    place_r2, race_loss, roi_summary)
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import rolling_splits, seg_mask, t6_odds  # noqa: E402

LAMBDAS = [round(x, 2) for x in np.arange(0.50, 1.0001, 0.05)]
LBS = (0.76, 0.62)
GAMMA_SHAPES = [2.0, 4.0, 8.0]
MODELS = ['H', 'D_fit', 'D_lbs', 'N', 'G_fit']


def per_race(d: pd.DataFrame, fn) -> np.ndarray:
    out = np.empty(len(d))
    pos = pd.Series(np.arange(len(d))).groupby(d['race_id'].to_numpy(), sort=False).groups
    q, k = d['q6'].to_numpy(), d['slots'].to_numpy()
    for idx in pos.values():
        idx = np.asarray(idx)
        out[idx] = fn(q[idx], int(k[idx[0]]))
    return np.clip(out, 1e-6, 1 - 1e-6)


def raw_loss(d: pd.DataFrame, col: str) -> float:
    return float(race_loss(d[col], d['hit'], d['race_id']).mean())


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
    fd, _ = load_inputs(args)
    fd = fd[['race_id', '馬番', 'date', 'win', 'is_eval', '単勝']]
    assert fd['date'].max() < FINAL_PERIOD_START
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp('2025-11-01')) & fd['is_eval'], 'race_id'])
    rt = pd.read_pickle(ROOT / args.return_tables)
    rt = rt[rt.index.astype(str).isin(dev_ids)]
    d, info = build(fd[fd['race_id'].isin(dev_ids)], t6_odds(dev_ids), rt)
    d = d.reset_index(drop=True)
    assert d['date'].max() < FINAL_PERIOD_START
    d['p_null'] = d['slots'] / d['runners']

    t0 = time.time()
    d['p_H'] = d['h'].clip(1e-6, 1 - 1e-6)
    grid = {}
    for l2 in LAMBDAS:
        for l3 in LAMBDAS:
            grid[(l2, l3)] = per_race(d, lambda q, k: discounted_place(q, k, l2, l3))
    d['p_D_lbs'] = per_race(d, lambda q, k: discounted_place(q, k, *LBS))
    d['p_N'] = per_race(d, lambda q, k: simulated_place(q, k, 'normal'))
    gam = {r: per_race(d, lambda q, k: simulated_place(q, k, 'gamma', r)) for r in GAMMA_SHAPES}
    print(f'probabilities built in {time.time() - t0:.0f}s ({len(d)} horses, {d["race_id"].nunique()} races)', flush=True)

    summary = {'info': info, 'lambdas_grid': LAMBDAS, 'lbs': LBS, 'gamma_shapes': GAMMA_SHAPES, 'thresholds': THRESHOLDS, 'folds': {}}
    pooled, all_bets, all_days = [], {}, []
    for sp in rolling_splits():
        cm, om = seg_mask(d['date'], 'calib', sp).to_numpy(), seg_mask(d['date'], 'outer', sp).to_numpy()
        cal_loss = {key: float(race_loss(v[cm], d.loc[cm, 'hit'], d.loc[cm, 'race_id']).mean()) for key, v in grid.items()}
        lam = min(cal_loss, key=cal_loss.get)
        g_loss = {r: float(race_loss(v[cm], d.loc[cm, 'hit'], d.loc[cm, 'race_id']).mean()) for r, v in gam.items()}
        shape = min(g_loss, key=g_loss.get)
        d['p_D_fit'], d['p_G_fit'] = grid[lam], gam[shape]
        for m in MODELS:
            d[f'logit_{m}'] = logit(d[f'p_{m}'])
        cal, out = d[cm].copy(), d[om].copy()
        days = pd.Index(sorted(out['date'].dt.date.unique()))
        all_days.extend(days)
        res = {'chosen_lambda': lam, 'chosen_gamma_shape': shape, 'calib_loss_harville': cal_loss[(1.0, 1.0)],
               'calib_loss_chosen': cal_loss[lam], 'outer_races': int(out['race_id'].nunique()), 'models': {}, 'betting': {}}
        probs = {}
        for m in MODELS:
            lr = fit_logistic(cal, [f'logit_{m}'])
            probs[m] = lr.predict_proba(out[[f'logit_{m}']])[:, 1]
            probs[f'raw_{m}'] = out[f'p_{m}'].to_numpy()
            res['models'][m] = {'raw_place_r2': place_r2(probs[f'raw_{m}'], out['p_null'], out['hit'], out['race_id']),
                                'place_r2': place_r2(probs[m], out['p_null'], out['hit'], out['race_id']),
                                'slope': float(lr.coef_[0][0]), 'intercept': float(lr.intercept_[0])}
            if m != 'H':
                res['models'][m]['vs_H'] = paired_place_r2(probs[m], probs['H'], out['p_null'], out['hit'], out['race_id'])
            for t in THRESHOLDS:
                b = bets_table(out, probs[m], t)
                res['betting'][f'{m}_{t:.2f}'] = roi_summary(b, days)
                all_bets.setdefault(f'{m}_{t:.2f}', []).append(b)
            b = bets_table(out, probs[f'raw_{m}'], 1.0)
            res['betting'][f'raw_{m}_1.00'] = roi_summary(b, days)
            all_bets.setdefault(f'raw_{m}_1.00', []).append(b)
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame({'race_id': out['race_id'].to_numpy(), 'hit': out['hit'].to_numpy(),
                                    'p_null': out['p_null'].to_numpy(), **probs}))
        print(f"outer {sp['outer'][0]}: λ={lam} r={shape} | " + ' '.join(
            f"{m}={res['models'][m]['place_r2']:.4f}" for m in MODELS) + ' | roi@1.00 ' + ' '.join(
            f"{m}={res['betting'][f'{m}_1.00'].get('roi', float('nan')):.2f}({res['betting'][f'{m}_1.00'].get('bets', 0)})" for m in MODELS),
            flush=True)

    P = pd.concat(pooled)
    days = pd.Index(sorted(set(all_days)))
    pool = {'place_r2': {m: place_r2(P[m], P['p_null'], P['hit'], P['race_id']) for m in MODELS},
            'raw_place_r2': {m: place_r2(P[f'raw_{m}'], P['p_null'], P['hit'], P['race_id']) for m in MODELS},
            'vs_H': {m: paired_place_r2(P[m], P['H'], P['p_null'], P['hit'], P['race_id']) for m in MODELS if m != 'H'},
            'raw_vs_raw_H': {m: paired_place_r2(P[f'raw_{m}'], P['raw_H'], P['p_null'], P['hit'], P['race_id'])
                             for m in MODELS if m != 'H'},
            'betting': {k: roi_summary(pd.concat(v), days) for k, v in all_bets.items()}}
    summary['pooled'] = pool
    for m in MODELS:
        pd.concat(all_bets[f'{m}_1.00']).to_csv(out_dir / f'bets_{m}_1.00.csv', index=False, encoding='utf-8')
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print('pooled place R2 (calibrated):', {k: round(v, 4) for k, v in pool['place_r2'].items()})
    print('pooled place R2 (raw):', {k: round(v, 4) for k, v in pool['raw_place_r2'].items()})
    for k, v in pool['vs_H'].items():
        print(f"  {k} − H: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]} | raw: "
              f"{pool['raw_vs_raw_H'][k]['delta_r2']:+.5f} {[round(x, 5) for x in pool['raw_vs_raw_H'][k]['ci95']]}")
    for k, v in pool['betting'].items():
        if v.get('bets'):
            ex = pd.concat(all_bets[k])['fukusho_payout'] / 100
            top3 = (ex.sum() - ex.nlargest(3).sum()) / len(ex)
            print(f"  bet {k}: n={v['bets']} hit={v['hit_rate']:.3f} roi={v['roi']:.3f} {[round(x, 3) for x in v['roi_ci95']]} "
                  f"(without top3: {top3:.3f})")


if __name__ == '__main__':
    main()
