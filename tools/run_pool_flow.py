"""票数（売れた金額）の流れから、6分前の時点で直前の投票の兆候を読めるか（開発期間のみ）。

各配信の単勝票数合計 W(t)（tools/build_pool_table.py の pools.csv）と、市場確率 q_i(t)（1/オッズを正規化）から、
各馬に売れた額を S_i(t) ≈ q_i(t)·W(t) と推定する（オッズ ≈ 0.8·W/S_i のため）。
2つの配信 a→b の間に入った額の割合 f_i = (S_i(b) − S_i(a)) / (W(b) − W(a))。

6分前に使える特徴（どれも発走5分前までの配信だけ）:
  gap_6_11   log f（11分前→6分前） − log q6   直前の投票が全体より多く入った馬ほど正（主）
  gap_11_20  log f（20分前→11分前） − log q6
  gap_20_30  log f（30分前→20分前） − log q6
  q6×規模    log q6 × 標準化した log W(6分前)、log q6 × 標準化した log(W6/W11)（売上が大きいほど市場が正確か）
説明だけ（判断には使えない）:
  gap_after  log f（6分前→締切後） − log q6。6分前より後の投票の情報量

評価は tools/run_odds_movement.py と同じ枠組み（rolling の3 fold、calib で推定 → outer、ΔR² はレース単位の再標本化）。
判定：[log q6, gap_6_11] の ΔR²（3期間の合計）の区間の下限 > 0 かつ ≥ +0.002 なら有望。
最終確認期間（2026-07-01 以降）は読まない。

使い方:
  .venv/Scripts/python.exe tools/run_pool_flow.py --out models/20261025/pool_flow
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
from modules.evaluation import FINAL_PERIOD_START, ConditionalLogit, paired_delta_r2, pseudo_r2  # noqa: E402
from tools.run_odds_movement import bets, norm_log_q, odds_asof, roi  # noqa: E402
from tools.run_place_market import build  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import DECISION_MIN, rolling_splits, seg_mask, t6_odds  # noqa: E402

OFFSETS = [DECISION_MIN, 10, 20, 30]
WINDOWS = {'gap_6_11': (10, DECISION_MIN), 'gap_11_20': (20, 10), 'gap_20_30': (30, 20)}
MIN_SHARE = 1e-4   # 割り振りが0以下（推定の誤差・取消）のときの下限
SETS = {'q6': ['log_q6'],
        'q6+gap_6_11': ['log_q6', 'gap_6_11'],
        'q6+gaps': ['log_q6', 'gap_6_11', 'gap_11_20', 'gap_20_30'],
        'q6+size': ['log_q6', 'q6_x_size', 'q6_x_growth'],
        'all': ['log_q6', 'gap_6_11', 'gap_11_20', 'gap_20_30', 'q6_x_size', 'q6_x_growth']}
PRIMARY = 'q6+gap_6_11'
THRESHOLDS = [1.00, 1.05, 1.10, 1.20]
DECISION = {'min_delta_r2': 0.002}


def pool_asof(pools: pd.DataFrame, post: pd.DataFrame, offsets: list[int]) -> pd.DataFrame:
    """各レースの、発走 k 分前までに配信された最新の単勝票数合計 W_k と、締切後の W_after。"""
    p = pools.merge(post[['race_id', 'post_time']], on='race_id')
    mid = p[p['data_kind'] == '1'].copy()
    mid['published_at'] = pd.to_datetime(mid['published_at'])
    out = pd.DataFrame({'race_id': sorted(p['race_id'].unique())}).set_index('race_id')
    for k in offsets:
        s = mid[mid['published_at'] <= mid['post_time'] - pd.Timedelta(minutes=k)]
        out[f'W_{k}'] = s.sort_values('published_at').groupby('race_id')['win_pool'].last()
    out['W_after'] = p[p['data_kind'].isin(['3', '4'])].groupby('race_id')['win_pool'].max()
    return out.reset_index()


def flow_gap(lq_a, w_a, lq_b, w_b, lq_ref, race_ids) -> np.ndarray:
    """a→b の間に入った額の割合 f と、基準の log q との差 log f − lq_ref。a に値の無い馬は a で0とする。"""
    s_a = np.exp(np.asarray(lq_a, float)) * np.asarray(w_a, float)
    s_a = np.nan_to_num(s_a, nan=0.0)
    s_b = np.exp(np.asarray(lq_b, float)) * np.asarray(w_b, float)
    flow = pd.Series(s_b - s_a)
    total = flow.clip(lower=0).groupby(np.asarray(race_ids)).transform('sum').to_numpy()
    share = np.maximum(flow.to_numpy(), 0) / np.where(total > 0, total, np.nan)
    return np.log(np.maximum(np.nan_to_num(share, nan=MIN_SHARE), MIN_SHARE)) - np.asarray(lq_ref, float)


def add_flow_features(d: pd.DataFrame, cal_stats: dict | None = None) -> pd.DataFrame:
    for name, (a, b) in WINDOWS.items():
        lq_b = d['log_q6'] if b == DECISION_MIN else d[f'lq_{b}']
        d[name] = flow_gap(d[f'lq_{a}'], d[f'W_{a}'], lq_b, d[f'W_{b}'], d['log_q6'], d['race_id'])
    d['gap_after'] = flow_gap(d['log_q6'], d[f'W_{DECISION_MIN}'], d['log_q'], d['W_after'], d['log_q6'], d['race_id'])
    d['log_size'] = np.log(d[f'W_{DECISION_MIN}'])
    d['log_growth'] = np.log(d[f'W_{DECISION_MIN}'] / d['W_10'])
    return d


def standardize(cal: pd.DataFrame, out: pd.DataFrame):
    for col, name in (('log_size', 'q6_x_size'), ('log_growth', 'q6_x_growth')):
        r = cal.groupby('race_id')[col].first()
        m, s = r.mean(), r.std()
        for df in (cal, out):
            df[name] = df['log_q6'] * (df[col] - m) / s


def descriptive(d: pd.DataFrame) -> dict:
    r = d.groupby('race_id').first()
    share6 = r[f'W_{DECISION_MIN}'] / r['W_after']
    share11 = r['W_10'] / r['W_after']
    d = d.copy()
    d['rank6'] = d.groupby('race_id')['odds_t6'].rank(method='first')
    d['band'] = pd.cut(d['rank6'], [0, 1, 2, 3, 5, 8, 12, 18], labels=['1', '2', '3', '4-5', '6-8', '9-12', '13-18'])
    tab = d.groupby('band', observed=True).agg(n=('win', 'size'), win_rate=('win', 'mean'), q6=('q6', 'mean'),
                                               gap_6_11=('gap_6_11', 'median'), gap_after=('gap_after', 'median'))
    return {'share_of_final_pool_at_t6': share6.describe(percentiles=[.1, .25, .5, .75, .9]).round(4).to_dict(),
            'share_of_final_pool_at_t11': share11.describe(percentiles=[.1, .5, .9]).round(4).to_dict(),
            'win_pool_after_close_median_yen': float(r['W_after'].median() * 100),
            'by_t6_popularity': tab.round(4).reset_index().astype({'band': str}).to_dict('records'),
            'corr': d[['gap_6_11', 'gap_11_20', 'gap_20_30', 'gap_after']].corr().round(3).to_dict()}


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
    fd = fd[['race_id', '馬番', 'date', 'win', 'is_eval', '単勝', 'tansho_payout', 'log_q']]
    assert fd['date'].max() < FINAL_PERIOD_START
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp('2025-11-01')) & fd['is_eval'], 'race_id'])
    rt = pd.read_pickle(ROOT / args.return_tables)
    rt = rt[rt.index.astype(str).isin(dev_ids)]
    d, info = build(fd[fd['race_id'].isin(dev_ids)], t6_odds(dev_ids), rt)
    d = d[d['単勝'].notna() & (d['単勝'] > 0)]
    d = d[d.groupby('race_id')['log_q'].transform(lambda s: s.notna().all())]
    d['log_q'] = norm_log_q(d['単勝'], d['race_id'])
    ids = set(d['race_id'])
    d = d.reset_index(drop=True).merge(odds_asof(ids, OFFSETS), on=['race_id', '馬番'], how='left')
    assert (d[f'lq_{DECISION_MIN}'] - d['log_q6']).abs().max() < 1e-9
    post = pd.read_csv(ROOT / 'data/odds/post_times/post_times.csv', dtype={'race_id': str})
    post['post_time'] = pd.to_datetime(post['post_time'])
    pools = pd.read_csv(ROOT / 'data/odds/jv_0b41/pools.csv', dtype={'race_id': str, 'data_kind': str})
    pools = pools[pools['race_id'].isin(ids)]
    d = d.merge(pool_asof(pools, post, OFFSETS), on='race_id', how='left')
    ok = d.groupby('race_id')[[f'W_{k}' for k in OFFSETS] + ['W_after']].transform(lambda s: s.notna().all() and (s > 0).all()).all(axis=1)
    info['races_without_pools'] = int(d.loc[~ok, 'race_id'].nunique())
    d = add_flow_features(d[ok].copy())
    assert d['date'].max() < FINAL_PERIOD_START
    info.update({'races': int(d['race_id'].nunique()), 'horses': len(d)})
    print(json.dumps(info, ensure_ascii=False), flush=True)
    summary = {'info': info, 'sets': SETS, 'primary': PRIMARY, 'decision_rule': DECISION, 'descriptive_all_dev': descriptive(d),
               'folds': {}}

    pooled, all_bets, all_days = [], {}, []
    for sp in rolling_splits():
        cal = d[seg_mask(d['date'], 'calib', sp)].copy()
        out = d[seg_mask(d['date'], 'outer', sp)].copy()
        standardize(cal, out)
        days = pd.Index(sorted(out['date'].dt.date.unique()))
        all_days.extend(days)
        r, w = out['race_id'], out['win']
        res = {'calib_races': int(cal['race_id'].nunique()), 'outer_races': int(r.nunique()), 'win': {}, 'betting': {}}
        probs = {}
        for name, cols in {**SETS, 'q6+after (not usable)': ['log_q6', 'gap_after'], 'final': ['log_q']}.items():
            cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['win'])
            probs[name] = cl.predict_proba(out[cols], r)
            res['win'][name] = {'pseudo_r2': pseudo_r2(probs[name], r, w), 'coef': dict(zip(cols, map(float, cl.coef_)))}
            if name != 'q6':
                res['win'][name]['vs_q6'] = paired_delta_r2(probs[name], probs['q6'], r, w)
        for name in SETS:
            for t in THRESHOLDS:
                b = bets(out, probs[name], out['odds_t6'].to_numpy(), t)
                res['betting'][f'{name}_{t:.2f}'] = roi(b, days)
                all_bets.setdefault(f'{name}_{t:.2f}', []).append(b)
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame({'race_id': r.to_numpy(), 'win': w.to_numpy(), **{f'p_{k}': v for k, v in probs.items()}}))
        print(f"outer {sp['outer'][0]} ({res['outer_races']} races): " + ' '.join(
            f"{k}={v['vs_q6']['delta_r2']:+.5f}" for k, v in res['win'].items() if 'vs_q6' in v), flush=True)

    P = pd.concat(pooled)
    days = pd.Index(sorted(set(all_days)))
    names = [k for k in summary['folds'][next(iter(summary['folds']))]['win'] if k != 'q6']
    pool = {'vs_q6': {k: paired_delta_r2(P[f'p_{k}'], P['p_q6'], P['race_id'], P['win']) for k in names},
            'betting': {k: roi(pd.concat(v), days) for k, v in all_bets.items()}}
    gap = pool['vs_q6']['final']['delta_r2']
    pool['share_of_final_gap'] = {k: v['delta_r2'] / gap for k, v in pool['vs_q6'].items()}
    summary['pooled'] = pool
    prim = pool['vs_q6'][PRIMARY]
    summary['decision'] = {'primary': PRIMARY, 'delta_r2': prim['delta_r2'], 'ci95': prim['ci95'],
                           'promising': bool(prim['ci95'][0] > 0 and prim['delta_r2'] >= DECISION['min_delta_r2'])}
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print('descriptive:', json.dumps({k: v for k, v in summary['descriptive_all_dev'].items() if k.startswith('share') or k.startswith('win_pool')}, ensure_ascii=False))
    for k, v in pool['vs_q6'].items():
        print(f"  {k} − q6: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]} (share of final gap {pool['share_of_final_gap'][k]:.2f})")
    for k, v in pool['betting'].items():
        if v.get('bets'):
            print(f"  bet {k}: n={v['bets']} hit={v['hit_rate']:.3f} roi={v['roi']:.3f} {[round(x, 3) for x in v['roi_ci95']]} "
                  f"(w/o top3 {v['roi_without_top3']:.3f})")
    print('decision:', summary['decision'])


if __name__ == '__main__':
    main()
