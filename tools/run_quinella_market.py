"""馬連の市場と単勝の市場の食い違いを、判断時刻（発走4分前・6分前の配信）で調べる（開発期間のみ）。

問い:
  1. 馬連の当たり（1・2着の組）を、単勝の市場から Harville で出した組の確率（H）と、
     馬連オッズから出した組の確率（M）のどちらがよく当てるか。両方を使うと（HM）上積みがあるか。
  2. 期待値 = 組の確率 × 6分前の馬連オッズ が閾値以上の組を買うと、回収率は100%を超えるか。

- 組の確率：H_ij = q_i·q_j/(1−q_i) + q_j·q_i/(1−q_j)（q＝6分前の単勝を正規化）。M_ij ∝ 1/馬連オッズ（レース内の全組で正規化）
- 補正：組を1行とし、レースの全組の中で当たりの組が1つ（同着は複数）として、条件付きロジットを各 fold の calib で推定
- fold は tools/run_stage2_t6.py --rolling と同じ（評価は 2026年1〜6月の3期間）。閾値は事前に決めた4つ、主な判定は HM・1.00
- 6分前の馬連オッズは、保存済みの 0B42 原文（tools/jvlink_collect_0b42.py）から、発表が発走5分前以前の最新の配信を選ぶ
- 払戻は確定の馬連払戻。最終確認期間（2026-07-01 以降）は読まない

使い方:
  .venv/Scripts/python.exe tools/run_quinella_market.py --out models/20261025/quinella_market
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
from modules.evaluation import (FINAL_PERIOD_START, ConditionalLogit, bootstrap_by_day, paired_delta_r2,  # noqa: E402
                                pseudo_r2, umaren_payouts)
from tools.odds_snapshots import parse_o2  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import DECISION_MIN, rolling_splits, seg_mask, t6_odds  # noqa: E402

O2_SIZE = 2042
SETS = {'M': ['log_m'], 'H': ['log_h'], 'HM': ['log_h', 'log_m']}
THRESHOLDS = [1.00, 1.05, 1.10, 1.20]
PRIMARY = ('HM', 1.00)


def harville_pairs(q: np.ndarray) -> np.ndarray:
    """1・2着の組（順不同）の確率の行列。対角は0、合計1。"""
    q = np.asarray(q, float)
    a = np.outer(q, q) / (1.0 - q)[:, None]   # a[i, j] = i が1着・j が2着
    np.fill_diagonal(a, 0.0)
    return a + a.T


def t6_quinella(blob: bytes, post_time: pd.Timestamp) -> dict | None:
    """発表が発走 DECISION_MIN 分前以前の最新の中間配信（組 → オッズ）。無ければ None。"""
    best, best_pub = None, None
    cutoff = post_time - pd.Timedelta(minutes=DECISION_MIN)
    for i in range(0, len(blob), O2_SIZE):
        raw = blob[i:i + O2_SIZE]
        if raw[2:3] != b'1':
            continue
        rec = parse_o2(raw)
        pub = pd.Timestamp(rec['source_published_at'])
        if pub <= cutoff and (best_pub is None or pub > best_pub):
            best, best_pub = rec, pub
    if best is None:
        return None
    return {'published_at': best_pub, 'pool': best['pool'],
            'odds': {(c['first'], c['second']): c['odds'] for c in best['combos'] if c['status'] in ('quoted', 'capped')}}


def build_pairs(fd: pd.DataFrame, o6: pd.DataFrame, pays: pd.DataFrame, post: pd.DataFrame, raw_dir: Path) -> tuple[pd.DataFrame, dict]:
    d = fd.merge(o6[['race_id', '馬番', 'odds_t6']], on=['race_id', '馬番'], how='left')
    ok = d.groupby('race_id')['odds_t6'].apply(lambda s: s.notna().all() and (s > 0).all() and len(s) >= 3)
    d = d[d['race_id'].isin(ok[ok].index)]
    post = post.set_index('race_id')['post_time']
    paid = set(pays['race_id'])
    hits = {(r.race_id, r.first, r.second): r.umaren_payout for r in pays.itertuples()}
    rows, info = [], {'races_with_t6_win': int(ok.sum()), 'no_raw': 0, 'no_t6_quinella': 0, 'missing_pairs': 0, 'no_payout': 0}
    for rid, g in d.groupby('race_id'):
        path = raw_dir / f'{rid}.bin'
        if not path.exists():
            info['no_raw'] += 1
            continue
        if rid not in paid:
            info['no_payout'] += 1
            continue
        snap = t6_quinella(path.read_bytes(), post[rid])
        if snap is None:
            info['no_t6_quinella'] += 1
            continue
        nums = g['馬番'].astype(int).to_numpy()
        inv = 1.0 / g['odds_t6'].to_numpy()
        q = inv / inv.sum()
        h = harville_pairs(q)
        pairs = [(i, j) for i in range(len(nums)) for j in range(i + 1, len(nums))]
        keys = [tuple(sorted((int(nums[i]), int(nums[j])))) for i, j in pairs]
        if any(k not in snap['odds'] for k in keys):
            info['missing_pairs'] += 1
            continue
        odds = np.array([snap['odds'][k] for k in keys])
        m = (1 / odds) / (1 / odds).sum()
        date = g['date'].iloc[0]
        for (i, j), k, o, mm in zip(pairs, keys, odds, m):
            pay = hits.get((rid, k[0], k[1]), 0.0)
            rows.append((rid, date, k[0], k[1], o, mm, h[i, j], int(pay > 0), pay))
    p = pd.DataFrame(rows, columns=['race_id', 'date', 'first', 'second', 'odds6', 'm', 'h', 'hit', 'payout'])
    p['log_m'], p['log_h'] = np.log(p['m']), np.log(np.clip(p['h'], 1e-12, 1))
    info['races'] = int(p['race_id'].nunique())
    info['pairs'] = len(p)
    info['races_without_hit_pair'] = int((p.groupby('race_id')['hit'].sum() == 0).sum())  # 取消などで当たりの組が外れたレース
    return p, info


def bets(out: pd.DataFrame, prob: np.ndarray, t: float) -> pd.DataFrame:
    sel = prob * out['odds6'].to_numpy() >= t
    return out.loc[sel, ['race_id', 'date', 'first', 'second', 'odds6', 'payout']].assign(p=prob[sel])


def roi(b: pd.DataFrame, days: pd.Index) -> dict:
    if b.empty:
        return {'bets': 0}
    g = b.groupby(b['date'].dt.date).agg(stake=('race_id', 'size'), payout=('payout', 'sum'))
    g['stake'] *= 100.0
    bs = bootstrap_by_day(g.reindex(days, fill_value=0.0))
    pay = b['payout'] / 100
    return {'bets': int(len(b)), 'races': int(b['race_id'].nunique()), 'hit_rate': float((pay > 0).mean()), 'roi': bs['roi'],
            'roi_ci95': bs['roi_ci95'], 'roi_without_top3': float((pay.sum() - pay.nlargest(3).sum()) / len(b)),
            'median_odds6': float(b['odds6'].median())}


def descriptive(p: pd.DataFrame) -> dict:
    p = p.copy()
    p['rank_m'] = p.groupby('race_id')['m'].rank(ascending=False, method='first')
    p['band'] = pd.cut(p['rank_m'], [0, 1, 3, 10, 30, 60, 200], labels=['1', '2-3', '4-10', '11-30', '31-60', '61+'])
    tab = p.groupby('band', observed=True).agg(n=('hit', 'size'), hit_rate=('hit', 'mean'), m=('m', 'mean'), h=('h', 'mean'),
                                               all_bet_roi=('payout', lambda s: s.sum() / (100 * len(s))),
                                               median_odds6=('odds6', 'median'))
    hit = p[p['hit'] == 1]
    return {'by_quinella_popularity': tab.round(4).reset_index().astype({'band': str}).to_dict('records'),
            'final_over_t6_odds_median_for_hits': float((hit['payout'] / 100 / hit['odds6']).median()),
            'share_final_below_t6_for_hits': float((hit['payout'] / 100 < hit['odds6'] - 1e-9).mean()),
            'sum_m_per_race': 1.0}


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
    fd = fd[['race_id', '馬番', 'date', 'is_eval']]
    assert fd['date'].max() < FINAL_PERIOD_START
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp('2025-11-01')) & fd['is_eval'], 'race_id'])
    rt = pd.read_pickle(ROOT / args.return_tables)
    pays = umaren_payouts(rt[rt.index.astype(str).isin(dev_ids)])
    post = pd.read_csv(ROOT / 'data/odds/post_times/post_times.csv', dtype={'race_id': str})
    post['post_time'] = pd.to_datetime(post['post_time'])
    p, info = build_pairs(fd[fd['race_id'].isin(dev_ids)], t6_odds(dev_ids), pays, post, ROOT / 'data/odds/jv_0b42/raw')
    assert p['date'].max() < FINAL_PERIOD_START
    print(json.dumps(info), flush=True)
    summary = {'info': info, 'sets': SETS, 'thresholds': THRESHOLDS, 'primary': PRIMARY, 'descriptive_all_dev': descriptive(p),
               'folds': {}}

    pooled, all_bets, all_days = [], {}, []
    for sp in rolling_splits():
        cal = p[seg_mask(p['date'], 'calib', sp)]
        out = p[seg_mask(p['date'], 'outer', sp)]
        days = pd.Index(sorted(out['date'].dt.date.unique()))
        all_days.extend(days)
        r, y = out['race_id'], out['hit']
        res = {'calib_races': int(cal['race_id'].nunique()), 'outer_races': int(r.nunique()), 'models': {}, 'betting': {}}
        probs = {'raw_M': out['m'].to_numpy(), 'raw_H': out['h'].to_numpy()}
        for name, cols in SETS.items():
            cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['hit'])
            probs[name] = cl.predict_proba(out[cols], r)
            res['models'][name] = {'pseudo_r2': pseudo_r2(probs[name], r, y), 'coef': dict(zip(cols, map(float, cl.coef_)))}
        for k in ('raw_M', 'raw_H'):
            res['models'][k] = {'pseudo_r2': pseudo_r2(probs[k], r, y)}
        res['delta'] = {f'{x}_vs_{z}': paired_delta_r2(probs[x], probs[z], r, y) for x, z in (('HM', 'M'), ('HM', 'H'), ('H', 'M'))}
        for name in ('M', 'H', 'HM'):
            for t in THRESHOLDS:
                b = bets(out, probs[name], t)
                res['betting'][f'{name}_{t:.2f}'] = roi(b, days)
                all_bets.setdefault(f'{name}_{t:.2f}', []).append(b)
        res['betting']['all_pairs'] = roi(out[['race_id', 'date', 'first', 'second', 'odds6', 'payout']], days)
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame({'race_id': r.to_numpy(), 'hit': y.to_numpy(), **probs}))
        hm = res['betting'][f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']
        print(f"outer {sp['outer'][0]} ({res['outer_races']} races): R2 " + ' '.join(
            f"{k}={v['pseudo_r2']:.4f}" for k, v in res['models'].items()) + f" | HM−M={res['delta']['HM_vs_M']['delta_r2']:+.5f} "
            f"| HM≥1.00 bets={hm.get('bets')} roi={hm.get('roi', float('nan')):.3f}", flush=True)

    P = pd.concat(pooled)
    days = pd.Index(sorted(set(all_days)))
    outer_all = p[p['date'].dt.date.isin(days)]
    pool = {'pseudo_r2': {k: pseudo_r2(P[k], P['race_id'], P['hit']) for k in ('raw_M', 'raw_H', 'M', 'H', 'HM')},
            'delta': {f'{x}_vs_{z}': paired_delta_r2(P[x], P[z], P['race_id'], P['hit']) for x, z in (('HM', 'M'), ('HM', 'H'), ('H', 'M'))},
            'betting': {'all_pairs': roi(outer_all[['race_id', 'date', 'first', 'second', 'odds6', 'payout']], days),
                        **{k: roi(pd.concat(v), days) for k, v in all_bets.items()}}}
    summary['pooled'] = pool
    prim = pool['betting'][f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']
    summary['decision'] = {'primary': f'{PRIMARY[0]}_{PRIMARY[1]:.2f}', 'roi': prim.get('roi'), 'roi_ci95': prim.get('roi_ci95'),
                           'above_100_with_ci': bool(prim.get('bets', 0) and prim['roi_ci95'][0] > 1.0)}
    pd.concat(all_bets[f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']).to_csv(out_dir / 'bets_primary.csv', index=False, encoding='utf-8')
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print('pooled pseudo R2:', {k: round(v, 4) for k, v in pool['pseudo_r2'].items()})
    for k, v in pool['delta'].items():
        print(f"  {k}: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}")
    for k, v in pool['betting'].items():
        if v.get('bets'):
            print(f"  bet {k}: n={v['bets']} hit={v['hit_rate']:.4f} roi={v['roi']:.3f} {[round(x, 3) for x in v['roi_ci95']]} "
                  f"(w/o top3 {v['roi_without_top3']:.3f})")
    print('decision:', summary['decision'])


if __name__ == '__main__':
    main()
