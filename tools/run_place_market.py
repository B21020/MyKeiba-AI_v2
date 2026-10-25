"""複勝の市場と単勝の市場の食い違いを、発走6分前のオッズで調べる（開発期間のみ）。

問い:
  1. 複勝の当たり（3着以内、7頭以下は2着以内）を、単勝の市場から Harville で出した確率（H）と、
     複勝オッズから出した確率（M）のどちらがよく当てるか。両方を使うと（HM）上積みがあるか。
  2. 複勝オッズは、単勝の市場が持っていない「勝ち馬」の情報を持つか（条件付きロジットで ΔR²）。
  3. 期待値 = 確率 × 6分前の複勝オッズの下限 が閾値以上の馬に複勝を買うと、回収率は100%を超えるか
     （Hausch・Ziemba・Rubinstein の方法を6分前の時点で行う）。

- fold・評価対象・6分前のオッズは tools/run_stage2_t6.py の --rolling と同じ（評価は 2026年1〜6月の3期間）
- 確率の補正（ロジスティック回帰）は各 fold の calib で推定し、outer で評価する
- 閾値は事前に決めた4つ（1.00／1.05／1.10／1.20）をすべて出す。主な判定は HM・1.00
- 払戻は確定の複勝払戻（同着を含む）。最終確認期間（2026-07-01 以降）は読まない

使い方:
  .venv/Scripts/python.exe tools/run_place_market.py --out models/20261025/place_market
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import (FINAL_PERIOD_START, ConditionalLogit, bootstrap_by_day, fukusho_payouts,  # noqa: E402
                                harville_place, paired_delta_r2, place_market_probs, place_slots)
from modules.evaluation._place import FLOOR_ODDS  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import rolling_splits, seg_mask, t6_odds  # noqa: E402

THRESHOLDS = [1.00, 1.05, 1.10, 1.20]
PRIMARY = ('HM', 1.00)
FEATURES = {'H': ['logit_h'], 'M': ['logit_pm', 'floored'], 'HM': ['logit_h', 'logit_pm', 'floored']}
EPS = 1e-12


def logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def build(fd: pd.DataFrame, o6: pd.DataFrame, rt: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """6分前の単勝・複勝オッズと確定の複勝払戻を結び、全頭にオッズがそろう複勝発売レースだけを残す。"""
    d = fd.merge(o6, on=['race_id', '馬番'], how='inner')
    pay = fukusho_payouts(rt)
    d = d.merge(pay, on=['race_id', '馬番'], how='left')
    d['fukusho_payout'] = d['fukusho_payout'].fillna(0.0)
    d['hit'] = (d['fukusho_payout'] > 0).astype(int)
    g = d.groupby('race_id')
    n_fd = fd[fd['race_id'].isin(d['race_id'])].groupby('race_id').size()
    ok = (g['odds_t6'].apply(lambda s: s.notna().all() and (s > 0).all())
          & g['place_low'].apply(lambda s: s.notna().all() and (s > 0).all())
          & g['place_high'].apply(lambda s: s.notna().all() and (s > 0).all())
          & (g.size() == n_fd.reindex(g.size().index))
          & (g['hit'].sum() > 0))
    info = {'races_with_t6': int(len(ok)), 'races_usable': int(ok.sum())}
    d = d[d['race_id'].isin(ok[ok].index)].copy()
    d['runners'] = d.groupby('race_id')['馬番'].transform('size')
    d['slots'] = d['runners'].map(place_slots)
    d = d[d['slots'] > 0].copy()
    inv = 1.0 / d['odds_t6']
    d['q6'] = inv / inv.groupby(d['race_id']).transform('sum')
    d['log_q6'] = np.log(d['q6'])
    d['h'] = 0.0
    for _, idx in d.groupby('race_id').groups.items():
        d.loc[idx, 'h'] = harville_place(d.loc[idx, 'q6'].to_numpy(), int(d.loc[idx[0], 'slots']))
    d['pm'] = place_market_probs(d['place_high'], d['race_id'], d['slots'])
    d['logit_h'], d['logit_pm'] = logit(d['h']), logit(d['pm'])
    d['floored'] = (d['place_high'] <= FLOOR_ODDS + 1e-9).astype(float)  # 複勝の上限が最低払戻に張り付いた馬
    d['log_pm'] = np.log(d['pm'])
    hits = d.groupby('race_id').agg(hits=('hit', 'sum'), slots=('slots', 'first'))
    info['races_hits_ne_slots'] = int((hits['hits'] != hits['slots']).sum())  # 同着・降着など
    return d, info


def race_loss(p, y, race_ids) -> pd.Series:
    """レースごとの2値の対数損失の合計。"""
    p = np.clip(np.asarray(p, float), EPS, 1 - EPS)
    y = np.asarray(y, float)
    ll = -(y * np.log(p) + (1 - y) * np.log(1 - p))
    return pd.Series(ll).groupby(pd.Series(race_ids).astype(str).to_numpy(), sort=False).sum()


def paired_place_r2(p_a, p_b, p_null, y, race_ids, n: int = 2000, seed: int = 0) -> dict:
    """複勝の当たりに対する pseudo-R² の差（a − b）。基準は頭数に応じた一様（slots/頭数）。レース単位の再標本化で95%区間。"""
    la, lb, l0 = (race_loss(p, y, race_ids).to_numpy() for p in (p_a, p_b, p_null))
    d = lb - la
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(n, len(d)))
    dist = d[idx].mean(axis=1) / l0[idx].mean(axis=1)
    return {'delta_r2': float(d.mean() / l0.mean()), 'ci95': [float(np.percentile(dist, 2.5)), float(np.percentile(dist, 97.5))],
            'races': int(len(d))}


def place_r2(p, p_null, y, race_ids) -> float:
    return float(1 - race_loss(p, y, race_ids).mean() / race_loss(p_null, y, race_ids).mean())


def fit_logistic(cal: pd.DataFrame, cols: list[str]) -> LogisticRegression:
    return LogisticRegression(C=1e6, max_iter=1000).fit(cal[cols], cal['hit'])


def bets_table(out: pd.DataFrame, p: np.ndarray, threshold: float) -> pd.DataFrame:
    ev = p * out['place_low'].to_numpy()
    b = out.loc[ev >= threshold, ['race_id', 'date', '馬番', 'place_low', 'place_high', 'fukusho_payout']].copy()
    b['p'] = p[ev >= threshold]
    return b


def daily(b: pd.DataFrame, days: pd.Index) -> pd.DataFrame:
    g = b.groupby(b['date'].dt.date).agg(stake=('race_id', 'size'), payout=('fukusho_payout', 'sum'))
    g['stake'] *= 100.0
    return g.reindex(days, fill_value=0.0)


def roi_summary(b: pd.DataFrame, days: pd.Index) -> dict:
    if b.empty:
        return {'bets': 0}
    bs = bootstrap_by_day(daily(b, days))
    return {'bets': int(len(b)), 'races': int(b['race_id'].nunique()), 'hit_rate': float((b['fukusho_payout'] > 0).mean()),
            'roi': bs['roi'], 'roi_ci95': bs['roi_ci95'], 'mean_place_low': float(b['place_low'].mean())}


def descriptive(d: pd.DataFrame) -> dict:
    """6分前の複勝オッズの幅と確定の払戻の関係、人気別の確率のずれ。"""
    h = d[d['hit'] == 1]
    pay = h['fukusho_payout'] / 100
    res = {'hits': int(len(h)), 'final_below_low': float((pay < h['place_low'] - 1e-9).mean()),
           'final_within': float(((pay >= h['place_low'] - 1e-9) & (pay <= h['place_high'] + 1e-9)).mean()),
           'final_above_high': float((pay > h['place_high'] + 1e-9).mean()),
           'final_over_low_median': float((pay / h['place_low']).median())}
    d = d.copy()
    d['win_rank'] = d.groupby('race_id')['odds_t6'].rank(method='first')
    d['band'] = pd.cut(d['win_rank'], [0, 1, 2, 3, 5, 8, 12, 18], labels=['1', '2', '3', '4-5', '6-8', '9-12', '13-18'])
    tab = d.groupby('band', observed=True).agg(n=('hit', 'size'), hit_rate=('hit', 'mean'), harville=('h', 'mean'),
                                               place_market=('pm', 'mean'), place_low=('place_low', 'mean'),
                                               all_bet_roi=('fukusho_payout', lambda s: s.sum() / (100 * len(s))))
    res['by_win_popularity'] = tab.round(4).reset_index().astype({'band': str}).to_dict('records')
    return res


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
    assert d['date'].max() < FINAL_PERIOD_START
    print(json.dumps(info), flush=True)
    d['p_null'] = d['slots'] / d['runners']
    summary = {'info': info, 'thresholds': THRESHOLDS, 'primary': PRIMARY, 'descriptive_all_dev': descriptive(d), 'folds': {}}

    pooled, all_bets, all_days = [], {}, []
    for sp in rolling_splits():
        cal = d[seg_mask(d['date'], 'calib', sp)].copy()
        out = d[seg_mask(d['date'], 'outer', sp)].copy()
        days = pd.Index(sorted(out['date'].dt.date.unique()))
        all_days.extend(days)
        res = {'calib_races': int(cal['race_id'].nunique()), 'outer_races': int(out['race_id'].nunique()), 'models': {}, 'betting': {}}
        probs = {'raw_H': out['h'].to_numpy(), 'raw_M': out['pm'].to_numpy()}
        for name, cols in FEATURES.items():
            m = fit_logistic(cal, cols)
            probs[name] = m.predict_proba(out[cols])[:, 1]
            res['models'][name] = {'coef': dict(zip(cols, map(float, m.coef_[0]))), 'intercept': float(m.intercept_[0]),
                                   'place_r2': place_r2(probs[name], out['p_null'], out['hit'], out['race_id'])}
        for k in ('raw_H', 'raw_M'):
            res['models'][k] = {'place_r2': place_r2(probs[k], out['p_null'], out['hit'], out['race_id'])}
        res['place_delta'] = {f'{x}_vs_{y}': paired_place_r2(probs[x], probs[y], out['p_null'], out['hit'], out['race_id'])
                              for x, y in (('HM', 'H'), ('HM', 'M'), ('M', 'H'))}
        # 勝ち馬：単勝の市場に複勝の市場を足す
        cl0 = ConditionalLogit().fit(cal[['log_q6']], cal['race_id'], cal['win'])
        cl1 = ConditionalLogit().fit(cal[['log_q6', 'log_pm']], cal['race_id'], cal['win'])
        pw0 = cl0.predict_proba(out[['log_q6']], out['race_id'])
        pw1 = cl1.predict_proba(out[['log_q6', 'log_pm']], out['race_id'])
        res['win_q6_plus_place'] = {**paired_delta_r2(pw1, pw0, out['race_id'], out['win']),
                                    'coef': dict(zip(['log_q6', 'log_pm'], map(float, cl1.coef_)))}
        res['betting']['all_horses'] = roi_summary(out.assign(p=1.0), days)
        for name in ('H', 'M', 'HM'):
            for t in THRESHOLDS:
                b = bets_table(out, probs[name], t)
                res['betting'][f'{name}_{t:.2f}'] = roi_summary(b, days)
                all_bets.setdefault(f'{name}_{t:.2f}', []).append(b)
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame({'race_id': out['race_id'].to_numpy(), 'hit': out['hit'].to_numpy(), 'win': out['win'].to_numpy(),
                                    'p_null': out['p_null'].to_numpy(), 'pw0': pw0, 'pw1': pw1, **{k: v for k, v in probs.items()}}))
        hm = res['betting'][f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']
        print(f"outer {sp['outer'][0]} ({res['outer_races']} races): place R2 H={res['models']['H']['place_r2']:.4f} "
              f"M={res['models']['M']['place_r2']:.4f} HM={res['models']['HM']['place_r2']:.4f} | "
              f"HM−M={res['place_delta']['HM_vs_M']['delta_r2']:+.5f} | win dR2(+place)={res['win_q6_plus_place']['delta_r2']:+.5f} | "
              f"HM≥1.00 bets={hm.get('bets')} roi={hm.get('roi', float('nan')):.3f}", flush=True)

    P = pd.concat(pooled)
    days = pd.Index(sorted(set(all_days)))
    out_all = d[d['date'].dt.date.isin(days)]
    pool = {'place_r2': {k: place_r2(P[k], P['p_null'], P['hit'], P['race_id']) for k in ('raw_H', 'raw_M', 'H', 'M', 'HM')},
            'place_delta': {f'{x}_vs_{y}': paired_place_r2(P[x], P[y], P['p_null'], P['hit'], P['race_id'])
                            for x, y in (('HM', 'H'), ('HM', 'M'), ('M', 'H'))},
            'win_q6_plus_place': paired_delta_r2(P['pw1'], P['pw0'], P['race_id'], P['win']),
            'betting': {'all_horses': roi_summary(out_all.assign(p=1.0), days)}}
    for k, bs in all_bets.items():
        pool['betting'][k] = roi_summary(pd.concat(bs), days)
    summary['pooled'] = pool
    for k in ('H_1.00', 'HM_1.00'):
        pd.concat(all_bets[k]).to_csv(out_dir / f'bets_{k}.csv', index=False, encoding='utf-8')
    prim = pool['betting'][f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']
    summary['decision'] = {'primary': f'{PRIMARY[0]}_{PRIMARY[1]:.2f}', 'roi': prim.get('roi'), 'roi_ci95': prim.get('roi_ci95'),
                           'above_100_with_ci': bool(prim.get('bets', 0) and prim['roi_ci95'][0] > 1.0)}
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print('pooled place R2:', {k: round(v, 4) for k, v in pool['place_r2'].items()})
    for k, v in pool['place_delta'].items():
        print(f"  {k}: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}")
    w = pool['win_q6_plus_place']
    print(f"win: q6+place − q6 = {w['delta_r2']:+.5f} {[round(x, 5) for x in w['ci95']]}")
    for k, v in pool['betting'].items():
        if v.get('bets'):
            print(f"  bet {k}: n={v['bets']} hit={v['hit_rate']:.3f} roi={v['roi']:.3f} {[round(x, 3) for x in v['roi_ci95']]}")
    print('decision:', summary['decision'])


if __name__ == '__main__':
    main()
