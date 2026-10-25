"""段階2の比較を、確定オッズの代わりに「発走6分前に配信されたオッズ」で行う（開発期間のみ）。

時系列オッズ（JV-Link 0B41）は 2026-02-16 以降しか無いため、区分は1つの時系列分割に限る:
  fit    能力モデルの学習          2021-07-01 〜 2025-07-01
  select early stopping              2025-07-01 〜 2026-01-01
  calib  条件付きロジットの推定    2026-02-16 〜 2026-04-01
  outer  評価                       2026-04-01 〜 2026-07-01
参考として、calib と outer を入れ替えた逆順（時系列に反する）も出す。
最終確認期間（2026-07-01 以降）は読み込み直後に捨て、時系列オッズも開発期間のレースだけを読む。

比べる確率（どれも calib で推定した条件付きロジット）:
  market_final : log q（確定オッズ）
  market_t6    : log q6（発走6分前のオッズ）
  comb_t6      : log F + log q6
  comb_final   : log F + log q（確定オッズでの結合。段階2と同じ形）
能力モデル F は2通り：base（過去着順＋通過順＋条件別成績＋騎手成績）と noid（ID系を除く全特徴量）。

--rolling を付けると、2025-10-01 以降の時系列オッズを使い、3つの fold（2か月で推定→次の2か月で評価、
評価は 2026年1〜2月・3〜4月・5〜6月）で比べ、合計の ΔR² も出す。

使い方:
  .venv/Scripts/python.exe tools/run_stage2_t6.py --out models/20261024/stage2_t6
  .venv/Scripts/python.exe tools/run_stage2_t6.py --out models/20261024/stage2_t6_rolling --rolling
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
from modules.evaluation import (  # noqa: E402
    FINAL_PERIOD_START, ConditionalLogit, paired_delta_r2, pseudo_r2, race_log_loss, top1_accuracy,
)
from modules.training import align_to_schema  # noqa: E402
from tools.analyze_stage2_ability import define_groups, fit_model, sub_schema  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402

SPLIT = {'fit': ('2021-07-01', '2025-07-01'), 'select': ('2025-07-01', '2026-01-01'),
         'calib': ('2026-02-16', '2026-04-01'), 'outer': ('2026-04-01', '2026-07-01')}
# --rolling：2か月で推定し次の2か月で評価する。能力モデルは calib の開始より前で学習（select は直前6か月）
ROLLING_CALIB_STARTS = ['2025-11-01', '2026-01-01', '2026-03-01']
# 判断は発走4分前、使うのは発表が発走5分前以前の最新の配信（実際は6分前の配信）。
# 2026-10-25 の実測で、6分前の配信は発表から20〜100秒で届き、発走4分20秒前までに受け取れた（即PAT の締切は発走1分前）。
# 次の1分前の配信は締切の後に届くので使えない。prompts/reports/20261025_pool_flow.md
DECISION_MIN = 5


def t6_odds(race_ids: set[str]) -> pd.DataFrame:
    """発走 DECISION_MIN 分前までに配信された最新の中間オッズ（単勝・複勝の下限と上限）。開発期間のレースだけを読む。"""
    pt = pd.read_csv(ROOT / 'data/odds/post_times/post_times.csv', dtype={'race_id': str})
    pt['post_time'] = pd.to_datetime(pt['post_time'])
    chunks = []
    for c in pd.read_csv(ROOT / 'data/odds/jv_0b41/quotes.csv', chunksize=500_000,
                         dtype={'race_id': str, 'data_kind': str},
                         usecols=['race_id', 'data_kind', 'published_at', 'horse_number', 'win_odds', 'win_status',
                                  'place_low', 'place_high']):
        c = c[c['race_id'].isin(race_ids) & (c['data_kind'] == '1')]
        chunks.append(c)
    q = pd.concat(chunks)
    q['published_at'] = pd.to_datetime(q['published_at'])
    q = q.merge(pt[['race_id', 'post_time']], on='race_id')
    q = q[q['published_at'] <= q['post_time'] - pd.Timedelta(minutes=DECISION_MIN)]
    latest = q.groupby('race_id')['published_at'].transform('max')
    q = q[q['published_at'] == latest]
    q['age_min'] = (q['post_time'] - pd.Timedelta(minutes=DECISION_MIN) - q['published_at']).dt.total_seconds() / 60
    cols = ['race_id', '馬番', 'odds_t6', 'win_status', 'place_low', 'place_high', 'published_at', 'age_min']
    return q.rename(columns={'horse_number': '馬番', 'win_odds': 'odds_t6'})[cols]


def seg_mask(dates, name, split=None):
    a, b = (split or SPLIT)[name]
    return (dates >= pd.Timestamp(a)) & (dates < pd.Timestamp(b))


def rolling_splits() -> list[dict]:
    out = []
    for s in ROLLING_CALIB_STARTS:
        C = pd.Timestamp(s)
        o_end = C + pd.DateOffset(months=4)
        assert o_end <= FINAL_PERIOD_START
        fmt = lambda t: str(t.date())  # noqa: E731
        out.append({'fit': (fmt(C - pd.DateOffset(months=54)), fmt(C - pd.DateOffset(months=6))),
                    'select': (fmt(C - pd.DateOffset(months=6)), fmt(C)),
                    'calib': (fmt(C), fmt(C + pd.DateOffset(months=2))),
                    'outer': (fmt(C + pd.DateOffset(months=2)), fmt(o_end))})
    return out


def fit_cl(df, cols):
    return ConditionalLogit().fit(df[cols], df['race_id'], df['win'])


def evaluate(cal, out, label):
    """calib で推定し outer で比べる。"""
    probs, coefs = {}, {}
    for name, cols in {'market_final': ['log_q'], 'market_t6': ['log_q6'], 'comb_t6': ['log_pF', 'log_q6'],
                       'comb_final': ['log_pF', 'log_q']}.items():
        cl = fit_cl(cal, cols)
        coefs[name] = dict(zip(cols, map(float, cl.coef_)))
        probs[name] = cl.predict_proba(out[cols], out['race_id'])
    r, w = out['race_id'], out['win']
    res = {'split': label, 'outer_races': int(r.nunique()), 'calib_races': int(cal['race_id'].nunique()), 'coefs': coefs,
           'quality': {k: {'pseudo_r2': pseudo_r2(p, r, w), 'log_loss': race_log_loss(p, r, w), 'top1': top1_accuracy(p, r, w)}
                       for k, p in probs.items()},
           'delta_r2': {
               'comb_t6_vs_market_t6': paired_delta_r2(probs['comb_t6'], probs['market_t6'], r, w),
               'comb_final_vs_market_final': paired_delta_r2(probs['comb_final'], probs['market_final'], r, w),
               'market_final_vs_market_t6': paired_delta_r2(probs['market_final'], probs['market_t6'], r, w),
               'comb_t6_vs_market_final': paired_delta_r2(probs['comb_t6'], probs['market_final'], r, w),
           }}
    return res, probs


COMPARISONS = {'comb_t6_vs_market_t6': ('comb_t6', 'market_t6'),
               'comb_final_vs_market_final': ('comb_final', 'market_final'),
               'market_final_vs_market_t6': ('market_final', 'market_t6')}


def run_rolling(d, usable, dev_ids, o6, schema, feats, out_dir):
    """fold ごとに能力モデルを学習し、outer の確率を集めて合計の ΔR² も出す。"""
    splits = rolling_splits()
    summary = {'splits': splits, 'decision_minutes_before_post': DECISION_MIN,
               'dev_eval_races_with_t6': len(usable), 'dev_eval_races': len(dev_ids),
               'dev_eval_races_without_t6': sorted(set(dev_ids) - usable)[:50],
               't6_age_min': o6.groupby('race_id')['age_min'].first().describe().round(2).to_dict(), 'models': {}}
    for name, cols in feats.items():
        sch = sub_schema(schema, cols)
        folds, pooled = {}, []
        for sp in splits:
            seg = {s: d[seg_mask(d['date'], s, sp)] for s in ('fit', 'select')}
            cal = d[seg_mask(d['date'], 'calib', sp) & d['race_id'].isin(usable)].copy()
            out = d[seg_mask(d['date'], 'outer', sp) & d['race_id'].isin(usable)].copy()
            model = fit_model({s: align_to_schema(seg[s], sch) for s in seg}, seg)
            for df in (cal, out):
                df['log_pF'] = np.log(np.clip(model.predict_proba(align_to_schema(df, sch))[:, 1], 1e-9, 1))
            label = f"calib {sp['calib'][0]}〜 → outer {sp['outer'][0]}〜{sp['outer'][1]}"
            res, probs = evaluate(cal, out, label)
            res['best_iteration'] = int(model.best_iteration_)
            folds[sp['outer'][0]] = res
            pooled.append(pd.DataFrame({'race_id': out['race_id'].to_numpy(), 'win': out['win'].to_numpy(),
                                        **{k: v for k, v in probs.items()}}))
            dd = res['delta_r2']
            print(f"{name} {label}: comb−mkt(t6)={dd['comb_t6_vs_market_t6']['delta_r2']:+.5f} "
                  f"{[round(x, 5) for x in dd['comb_t6_vs_market_t6']['ci95']]} | comb−mkt(final)="
                  f"{dd['comb_final_vs_market_final']['delta_r2']:+.5f} | mkt final−t6={dd['market_final_vs_market_t6']['delta_r2']:+.5f} "
                  f"(calib {res['calib_races']} / outer {res['outer_races']} races)", flush=True)
        P = pd.concat(pooled)
        pooled_res = {k: paired_delta_r2(P[x], P[y], P['race_id'], P['win']) for k, (x, y) in COMPARISONS.items()}
        summary['models'][name] = {'n_features': len(sch['columns']), 'folds': folds, 'pooled': pooled_res,
                                   'folds_positive_comb_t6': int(sum(f['delta_r2']['comb_t6_vs_market_t6']['delta_r2'] > 0
                                                                     for f in folds.values()))}
        print(f"{name} pooled: " + ' | '.join(f"{k}={v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}"
                                               for k, v in pooled_res.items()), flush=True)
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    ap.add_argument('--rolling', action='store_true', help='2025-11 以降の3つの fold（2か月推定→2か月評価）で比べる')
    a = ap.parse_args()
    out_dir = ROOT / a.out
    out_dir.mkdir(parents=True, exist_ok=True)
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, schema = load_inputs(args)  # 2026-07-01 以降はここで除かれている
    assert fd['date'].max() < FINAL_PERIOD_START
    t6_start = '2025-10-01' if a.rolling else '2026-02-16'
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp(t6_start)) & fd['is_eval'], 'race_id'])
    o6 = t6_odds(dev_ids)
    d = fd.merge(o6, on=['race_id', '馬番'], how='left')
    # 発走6分前のオッズが全頭にある評価対象レースだけを使う
    ok = d[d['race_id'].isin(dev_ids)].groupby('race_id')['odds_t6'].apply(lambda s: s.notna().all() and (s > 0).all())
    usable = set(ok[ok].index)
    inv = 1.0 / d['odds_t6']
    d['q6'] = inv / inv.groupby(d['race_id']).transform('sum')
    d['log_q6'] = np.log(d['q6'])

    groups = define_groups(schema['columns'])
    ids = set(groups['id_horse'] + groups['id_connections'] + groups['peds'])
    feats = {'base': groups['form'] + groups['position'] + groups['cond_history'] + groups['jockey_stats'],
             'noid': [c for c in schema['columns'] if c not in ids]}
    if a.rolling:
        return run_rolling(d, usable, dev_ids, o6, schema, feats, out_dir)
    seg = {s: d[seg_mask(d['date'], s)] for s in ('fit', 'select')}
    cal = d[seg_mask(d['date'], 'calib') & d['race_id'].isin(usable)].copy()
    out = d[seg_mask(d['date'], 'outer') & d['race_id'].isin(usable)].copy()
    summary = {'split': SPLIT, 'decision_minutes_before_post': DECISION_MIN,
               'dev_eval_races_with_t6': len(usable), 'dev_eval_races': len(dev_ids),
               't6_age_min': o6.groupby('race_id')['age_min'].first().describe().round(2).to_dict(),
               't6_vs_final_odds_ratio': float((d.loc[d['race_id'].isin(usable), 'odds_t6'] /
                                                d.loc[d['race_id'].isin(usable), '単勝']).median()),
               'models': {}}
    for name, cols in feats.items():
        sch = sub_schema(schema, cols)
        X = {s: align_to_schema(seg[s], sch) for s in ('fit', 'select')}
        model = fit_model(X, seg)
        for df in (cal, out):
            df['log_pF'] = np.log(np.clip(model.predict_proba(align_to_schema(df, sch))[:, 1], 1e-9, 1))
        forward, probs = evaluate(cal, out, 'calib 2/16-3/31 → outer 4/1-6/30')
        reverse, _ = evaluate(out, cal, '逆順（参考）calib 4/1-6/30 → outer 2/16-3/31')
        summary['models'][name] = {'n_features': len(sch['columns']), 'best_iteration': int(model.best_iteration_),
                                   'forward': forward, 'reverse_reference': reverse}
        dd = forward['delta_r2']
        print(f"{name}: ΔR²(comb vs market) t6={dd['comb_t6_vs_market_t6']['delta_r2']:+.5f} "
              f"{[round(x, 5) for x in dd['comb_t6_vs_market_t6']['ci95']]} | final={dd['comb_final_vs_market_final']['delta_r2']:+.5f} "
              f"| market final−t6={dd['market_final_vs_market_t6']['delta_r2']:+.5f}", flush=True)
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print(json.dumps({k: summary[k] for k in ('dev_eval_races_with_t6', 'dev_eval_races', 't6_age_min', 't6_vs_final_odds_ratio')},
                     ensure_ascii=False, default=float))
    for name, m in summary['models'].items():
        print(name, json.dumps(m['forward']['quality'], ensure_ascii=False, default=float))
        print(name, 'coefs', json.dumps(m['forward']['coefs'], ensure_ascii=False))


if __name__ == '__main__':
    main()
