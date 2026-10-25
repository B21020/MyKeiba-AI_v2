"""発走6分前から確定までの単勝オッズの動きを、6分前の時点で読めるか（開発期間のみ）。

確定の市場は6分前の市場より勝ち馬をよく当てる（ΔR² ≈ +0.019、段階2の再比較）。その差のうち、
6分前までに分かること（それまでのオッズの動き、複勝と単勝の食い違い、人気）で前もって読める分があるかを調べる。

6分前に使える特徴（どれも発走5分前までに配信されたものだけ）:
  log_q6        6分前の単勝の市場確率（レース内で正規化）
  mom_10/30/120 6分前の log q − 発走10・30・120分前の log q（それぞれの時点の最新の配信）
  mom_open      6分前の log q − 最初の配信（前日夕方）の log q
  place_gap     logit(複勝オッズから逆算した確率) − logit(単勝から Harville で出した確率)。複勝で単勝より買われている馬ほど正
動き（目的）:
  move = log q（確定） − log q6

調べること（fold は tools/run_stage2_t6.py --rolling と同じ。どれも calib で推定し outer で評価）:
  1. 動きの予測：最小二乗で move を予測し、outer の R²（0 を予測する場合を基準）と相関
  2. 勝ち馬：条件付きロジット [log q6] に特徴を足した ΔR²。確定の市場 [log q] との差（+0.019 前後）のうち何割を取り戻すか
  3. 購入：期待値 = 勝つ確率 × 予測した確定オッズ（6分前のオッズ × exp(−予測した move)）が閾値以上の馬に単勝。払戻は確定
最終確認期間（2026-07-01 以降）は読まない。

使い方:
  .venv/Scripts/python.exe tools/run_odds_movement.py --out models/20261025/odds_movement
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START, ConditionalLogit, bootstrap_by_day, paired_delta_r2, pseudo_r2  # noqa: E402
from tools.run_place_market import build  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import DECISION_MIN, rolling_splits, seg_mask, t6_odds  # noqa: E402

OFFSETS = [10, 30, 120]
CURVE = [120, 60, 30, 20, 10, DECISION_MIN, 0]  # 市場の当たり具合を時刻ごとに見る（0 = 発走直前の最後の配信）
MOM = [f'mom_{k}' for k in OFFSETS] + ['mom_open']
SETS = {'q6': ['log_q6'],
        'q6+mom': ['log_q6'] + MOM,
        'q6+place': ['log_q6', 'place_gap'],
        'q6+mom+place': ['log_q6'] + MOM + ['place_gap']}
THRESHOLDS = [1.00, 1.05, 1.10, 1.20]
PRIMARY = ('q6+mom+place', 1.00)


def norm_log_q(odds: pd.Series, race_ids: pd.Series) -> pd.Series:
    inv = 1.0 / odds
    return np.log(inv / inv.groupby(race_ids).transform('sum'))


def odds_asof(race_ids: set[str], offsets: list[int]) -> pd.DataFrame:
    """各レース・各馬の、発走 k 分前までに配信された最新の単勝オッズの log q と、最初の配信の log q。"""
    pt = pd.read_csv(ROOT / 'data/odds/post_times/post_times.csv', dtype={'race_id': str})
    pt['post_time'] = pd.to_datetime(pt['post_time'])
    chunks = []
    for c in pd.read_csv(ROOT / 'data/odds/jv_0b41/quotes.csv', chunksize=500_000, dtype={'race_id': str, 'data_kind': str},
                         usecols=['race_id', 'data_kind', 'published_at', 'horse_number', 'win_odds']):
        chunks.append(c[c['race_id'].isin(race_ids) & (c['data_kind'] == '1')])
    q = pd.concat(chunks).merge(pt[['race_id', 'post_time']], on='race_id')
    q['published_at'] = pd.to_datetime(q['published_at'])
    return asof_from_quotes(q, offsets)


def asof_from_quotes(q: pd.DataFrame, offsets: list[int]) -> pd.DataFrame:
    """q: race_id, published_at, post_time, horse_number, win_odds。発走 k 分前以前の最新の配信と、最初の配信の log q。"""
    q = q[q['win_odds'] > 0]
    out = None
    for name, cut in [(f'lq_{k}', k) for k in offsets] + [('lq_open', None)]:
        s = q if cut is None else q[q['published_at'] <= q['post_time'] - pd.Timedelta(minutes=cut)]
        pick = s.groupby('race_id')['published_at'].transform('min' if cut is None else 'max')
        s = s[s['published_at'] == pick].copy()
        s[name] = norm_log_q(s['win_odds'], s['race_id'])
        s = s.rename(columns={'horse_number': '馬番'})[['race_id', '馬番', name]]
        out = s if out is None else out.merge(s, on=['race_id', '馬番'], how='outer')
    return out


def add_features(d: pd.DataFrame, asof: pd.DataFrame) -> pd.DataFrame:
    d = d.merge(asof, on=['race_id', '馬番'], how='left')
    for k in [str(k) for k in OFFSETS] + ['open']:
        d[f'mom_{k}'] = (d['log_q6'] - d[f'lq_{k}']).fillna(0.0)  # その時点に配信が無い馬は動き0
    lh = np.log(d['h'] / (1 - d['h']))
    lp = np.log(d['pm'] / (1 - d['pm']))
    d['place_gap'] = (lp - lh).where(d['floored'] == 0, 0.0)  # 最低払戻に張り付いた馬は読めない
    d['move'] = d['log_q'] - d['log_q6']
    return d


def r2(y, yhat) -> float:
    y, yhat = np.asarray(y), np.asarray(yhat)
    return float(1 - ((y - yhat) ** 2).sum() / (y ** 2).sum())


def bets(out: pd.DataFrame, p: np.ndarray, odds_hat: np.ndarray, t: float) -> pd.DataFrame:
    m = p * odds_hat >= t
    return out.loc[m, ['race_id', 'date', '馬番', 'odds_t6', '単勝', 'tansho_payout']].assign(p=p[m], odds_hat=odds_hat[m])


def roi(b: pd.DataFrame, days: pd.Index) -> dict:
    if b.empty:
        return {'bets': 0}
    g = b.groupby(b['date'].dt.date).agg(stake=('race_id', 'size'), payout=('tansho_payout', 'sum'))
    g['stake'] *= 100.0
    bs = bootstrap_by_day(g.reindex(days, fill_value=0.0))
    pay = b['tansho_payout'] / 100
    return {'bets': int(len(b)), 'hit_rate': float((pay > 0).mean()), 'roi': bs['roi'], 'roi_ci95': bs['roi_ci95'],
            'roi_without_top3': float((pay.sum() - pay.nlargest(3).sum()) / len(b)),
            'median_odds_t6': float(b['odds_t6'].median())}


def market_by_time(d: pd.DataFrame) -> dict:
    """発走 k 分前の市場（補正なし）の pseudo-R²。全頭にその時点の配信があるレースだけ。"""
    res = {}
    for k in CURVE:
        col = f'lq_{k}'
        ok = d.groupby('race_id')[col].transform(lambda s: s.notna().all())
        s = d[ok]
        res[f'T-{k}'] = {'races': int(s['race_id'].nunique()), 'pseudo_r2': pseudo_r2(np.exp(s[col]), s['race_id'], s['win']),
                         'final_pseudo_r2_same_races': pseudo_r2(np.exp(s['log_q']), s['race_id'], s['win'])}
    return res


def descriptive(d: pd.DataFrame) -> dict:
    d = d.copy()
    d['rank6'] = d.groupby('race_id')['odds_t6'].rank(method='first')
    d['band'] = pd.cut(d['rank6'], [0, 1, 2, 3, 5, 8, 12, 18], labels=['1', '2', '3', '4-5', '6-8', '9-12', '13-18'])
    tab = d.groupby('band', observed=True).agg(n=('move', 'size'), mean_move=('move', 'mean'), sd_move=('move', 'std'),
                                               win_rate=('win', 'mean'), q6=('q6', 'mean'),
                                               q_final=('log_q', lambda s: np.exp(s).mean()))
    corr = d[MOM + ['place_gap', 'move']].corr()['move'].drop('move')
    return {'by_t6_popularity': tab.round(4).reset_index().astype({'band': str}).to_dict('records'),
            'corr_with_move': corr.round(4).to_dict(), 'sd_move': float(d['move'].std()),
            'share_abs_move_gt_0_2': float((d['move'].abs() > 0.2).mean())}


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
    # 確定の log q は確定に残った馬で正規化し直す（6分前と同じ馬の集合）
    d['log_q'] = norm_log_q(d['単勝'], d['race_id'])
    d = add_features(d.reset_index(drop=True), odds_asof(set(d['race_id']), sorted(set(OFFSETS + CURVE))))
    assert (d['lq_5'] - d['log_q6']).abs().max() < 1e-9, 'as-of 5 must equal the T-6 snapshot'
    assert d['date'].max() < FINAL_PERIOD_START
    info.update({'races': int(d['race_id'].nunique()), 'horses': len(d),
                 'missing_share': {k: float(d[f'lq_{k}'].isna().mean()) for k in [str(k) for k in OFFSETS] + ['open']}})
    print(json.dumps(info, ensure_ascii=False), flush=True)
    summary = {'info': info, 'market_by_time': market_by_time(d), 'sets': SETS, 'thresholds': THRESHOLDS, 'primary': PRIMARY,
               'descriptive_all_dev': descriptive(d), 'folds': {}}

    pooled, all_bets, all_days = [], {}, []
    for sp in rolling_splits():
        cal = d[seg_mask(d['date'], 'calib', sp)].copy()
        out = d[seg_mask(d['date'], 'outer', sp)].copy()
        days = pd.Index(sorted(out['date'].dt.date.unique()))
        all_days.extend(days)
        r, w = out['race_id'], out['win']
        res = {'calib_races': int(cal['race_id'].nunique()), 'outer_races': int(r.nunique()), 'move': {}, 'win': {}, 'betting': {}}
        move_hat = {}
        for name, cols in SETS.items():
            lr = LinearRegression().fit(cal[cols], cal['move'])
            move_hat[name] = lr.predict(out[cols])
            res['move'][name] = {'r2_vs_zero': r2(out['move'], move_hat[name]),
                                 'corr': float(np.corrcoef(out['move'], move_hat[name])[0, 1]),
                                 'coef': dict(zip(cols, map(float, lr.coef_)))}
        probs = {}
        for name, cols in {**SETS, 'final': ['log_q']}.items():
            cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['win'])
            probs[name] = cl.predict_proba(out[cols], r)
            res['win'][name] = {'pseudo_r2': pseudo_r2(probs[name], r, w), 'coef': dict(zip(cols, map(float, cl.coef_)))}
        for name in SETS:
            if name != 'q6':
                res['win'][name]['vs_q6'] = paired_delta_r2(probs[name], probs['q6'], r, w)
        res['win']['final']['vs_q6'] = paired_delta_r2(probs['final'], probs['q6'], r, w)
        for name in SETS:
            # 予測した確定オッズ：6分前のオッズ × exp(−予測した move)。'q6' は動きを予測しない（6分前のオッズのまま）
            odds_hat = out['odds_t6'].to_numpy() * (1.0 if name == 'q6' else np.exp(-move_hat[name]))
            for t in THRESHOLDS:
                b = bets(out, probs[name], odds_hat, t)
                res['betting'][f'{name}_{t:.2f}'] = roi(b, days)
                all_bets.setdefault(f'{name}_{t:.2f}', []).append(b)
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame({'race_id': r.to_numpy(), 'win': w.to_numpy(), 'move': out['move'].to_numpy(),
                                    **{f'p_{k}': v for k, v in probs.items()}, **{f'm_{k}': v for k, v in move_hat.items()}}))
        print(f"outer {sp['outer'][0]} ({res['outer_races']} races): move R2 " +
              ' '.join(f"{k}={v['r2_vs_zero']:+.4f}" for k, v in res['move'].items()) + ' | win dR2 vs q6 ' +
              ' '.join(f"{k}={v['vs_q6']['delta_r2']:+.5f}" for k, v in res['win'].items() if 'vs_q6' in v), flush=True)

    P = pd.concat(pooled)
    days = pd.Index(sorted(set(all_days)))
    gap = paired_delta_r2(P['p_final'], P['p_q6'], P['race_id'], P['win'])
    pool = {'move_r2_vs_zero': {k: r2(P['move'], P[f'm_{k}']) for k in SETS},
            'move_corr': {k: float(np.corrcoef(P['move'], P[f'm_{k}'])[0, 1]) for k in SETS},
            'win_vs_q6': {k: paired_delta_r2(P[f'p_{k}'], P['p_q6'], P['race_id'], P['win']) for k in SETS if k != 'q6'},
            'final_vs_q6': gap,
            'betting': {k: roi(pd.concat(v), days) for k, v in all_bets.items()}}
    pool['share_of_gap_recovered'] = {k: v['delta_r2'] / gap['delta_r2'] for k, v in pool['win_vs_q6'].items()}
    summary['pooled'] = pool
    prim = pool['betting'][f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']
    summary['decision'] = {'primary': f'{PRIMARY[0]}_{PRIMARY[1]:.2f}', 'roi': prim.get('roi'), 'roi_ci95': prim.get('roi_ci95'),
                           'above_100_with_ci': bool(prim.get('bets', 0) and prim['roi_ci95'][0] > 1.0)}
    pd.concat(all_bets[f'{PRIMARY[0]}_{PRIMARY[1]:.2f}']).to_csv(out_dir / 'bets_primary.csv', index=False, encoding='utf-8')
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print('market by time:', {k: (v['races'], round(v['pseudo_r2'], 4), round(v['final_pseudo_r2_same_races'], 4)) for k, v in summary['market_by_time'].items()})
    print('move R2 vs zero:', {k: round(v, 4) for k, v in pool['move_r2_vs_zero'].items()})
    print('move corr:', {k: round(v, 4) for k, v in pool['move_corr'].items()})
    print(f"final − q6: {gap['delta_r2']:+.5f} {[round(x, 5) for x in gap['ci95']]}")
    for k, v in pool['win_vs_q6'].items():
        print(f"  {k} − q6: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]} (share of gap {pool['share_of_gap_recovered'][k]:.2f})")
    for k, v in pool['betting'].items():
        if v.get('bets'):
            print(f"  bet {k}: n={v['bets']} hit={v['hit_rate']:.3f} roi={v['roi']:.3f} {[round(x, 3) for x in v['roi_ci95']]} "
                  f"(w/o top3 {v['roi_without_top3']:.3f})")
    print('decision:', summary['decision'])


if __name__ == '__main__':
    main()
