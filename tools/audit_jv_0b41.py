"""時系列オッズ（0B41）の取り込み結果を監査する。

レースごとに:
  - 取得できたか（JVRTOpen の戻り値、レコード数、解析エラー）
  - 中間オッズの件数・最初と最後の配信時刻・配信間隔
  - 発走予定時刻（tools/fetch_post_times.py）に対して「発走5分前・2分前までに配信された最新の中間オッズ」があるか、その古さ
  - 中間オッズの頭数と、結果表の出走頭数（取消・除外を除く）の一致
  - 開発期間（2026-07-01 より前）のみ：締切後の中間オッズが確定単勝オッズと一致するか（値の確認）
最終確認期間（2026-07-01 以降）は件数・時刻・頭数などの構造だけを確かめ、オッズの値は読まない。

使い方:
  .venv/Scripts/python.exe tools/audit_jv_0b41.py --data data/odds/jv_0b41 --post-times data/odds/post_times/post_times.csv
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START  # noqa: E402
from modules.preprocessing._results_processor import coalesce_result_columns, parse_finish_code  # noqa: E402

MAX_AGE_MIN = 10


def results_table() -> pd.DataFrame:
    r = pd.read_pickle(ROOT / 'data/raw/results.pickle')
    r.index.name = 'race_id'
    r = coalesce_result_columns(r.reset_index().drop_duplicates())
    r['race_id'] = r['race_id'].astype(str)
    r['finish'] = r['着順'].map(parse_finish_code)
    r = r[r['finish'].notna()]
    r['馬番'] = pd.to_numeric(r['馬番'], errors='coerce').astype('Int64')
    r['final_odds'] = pd.to_numeric(r['単勝'], errors='coerce')
    return r[['race_id', '馬番', 'final_odds']]


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data', required=True)
    ap.add_argument('--post-times', required=True)
    ap.add_argument('--targets', default='data/odds/20261023_setup/targets.csv')
    args = ap.parse_args()
    data = ROOT / args.data
    targets = pd.read_csv(ROOT / args.targets, dtype=str, encoding='utf-8-sig')
    targets['race_date'] = pd.to_datetime(targets['race_date'])
    log = pd.read_json(data / 'collection_log.jsonl', lines=True, dtype={'race_id': str})
    log = log.sort_values('started_at').drop_duplicates('race_id', keep='last')
    pt = pd.read_csv(ROOT / args.post_times, dtype={'race_id': str})
    pt['post_time'] = pd.to_datetime(pt['post_time'])
    q = pd.read_csv(data / 'quotes.csv', dtype={'race_id': str, 'jv_key': str, 'data_kind': str})
    q['published_at'] = pd.to_datetime(q['published_at'])
    res = results_table()

    a = targets.merge(log[['race_id', 'open_code', 'records', 'gets_error', 'parse_errors']], on='race_id', how='left')
    a = a.merge(pt[['race_id', 'post_time']], on='race_id', how='left')
    a['final_period'] = a['race_date'] >= FINAL_PERIOD_START
    inter = q[q['data_kind'] == '1']
    snaps = inter.groupby(['race_id', 'published_at']).agg(horses=('horse_number', 'nunique')).reset_index()
    g = snaps.groupby('race_id')
    a = a.merge(pd.DataFrame({'interim_snapshots': g.size(), 'first_published': g['published_at'].min(),
                              'last_published': g['published_at'].max(),
                              'median_step_min': g['published_at'].apply(lambda s: s.sort_values().diff().dt.total_seconds().median() / 60)}),
                left_on='race_id', right_index=True, how='left')
    a = a.merge(q.groupby('race_id')['data_kind'].apply(lambda s: ''.join(sorted(set(s)))).rename('kinds'),
                left_on='race_id', right_index=True, how='left')
    starters = res.groupby('race_id').size().rename('starters')
    a = a.merge(starters, left_on='race_id', right_index=True, how='left')

    for m in (5, 2):
        cut = a.set_index('race_id')['post_time'] - pd.Timedelta(minutes=m)
        s = snaps.copy()
        s['cut'] = s['race_id'].map(cut)
        s = s[s['published_at'] <= s['cut']].sort_values('published_at').groupby('race_id').last()
        a[f'asof_{m}_published'] = a['race_id'].map(s['published_at'])
        a[f'asof_{m}_age_min'] = (a['post_time'] - pd.Timedelta(minutes=m) - a[f'asof_{m}_published']).dt.total_seconds() / 60
        a[f'asof_{m}_horses'] = a['race_id'].map(s['horses'])
        a[f'asof_{m}_ok'] = a[f'asof_{m}_age_min'].le(MAX_AGE_MIN) & a[f'asof_{m}_horses'].eq(a['starters'])

    # 値の確認（開発期間のみ）：最後の中間オッズ＝確定単勝オッズの割合
    dev_ids = set(a.loc[~a['final_period'], 'race_id'])
    last = inter[inter['race_id'].isin(dev_ids)].sort_values('published_at').groupby(['race_id', 'horse_number']).last().reset_index()
    last = last.merge(res, left_on=['race_id', 'horse_number'], right_on=['race_id', '馬番'], how='inner')
    eq = pd.Series(np.isclose(last['win_odds'].astype(float), last['final_odds'].astype(float)),
                   index=last.index).groupby(last['race_id']).mean()
    a['dev_last_equals_final'] = a['race_id'].map(eq)

    a['collected'] = a['open_code'].isin([0, -1]) & a['gets_error'].isna() & a['records'].gt(0)
    a.to_csv(data / 'audit_by_race.csv', index=False, encoding='utf-8')
    a['month'] = a['race_date'].dt.strftime('%Y-%m')
    by_month = a.groupby('month').agg(races=('race_id', 'size'), collected=('collected', 'sum'),
                                      with_post_time=('post_time', lambda s: s.notna().sum()),
                                      asof5_ok=('asof_5_ok', 'sum'), asof2_ok=('asof_2_ok', 'sum'),
                                      median_snapshots=('interim_snapshots', 'median'),
                                      median_step_min=('median_step_min', 'median'))
    by_month.to_csv(data / 'audit_by_month.csv', encoding='utf-8')
    summary = {
        'targets': len(a), 'collected': int(a['collected'].sum()),
        'not_collected': a.loc[~a['collected'], ['race_id', 'open_code', 'records', 'gets_error']].head(20).to_dict('records'),
        'parse_errors_total': int(a['parse_errors'].fillna(0).sum()),
        'with_post_time': int(a['post_time'].notna().sum()),
        'asof_5_ok': int(a['asof_5_ok'].sum()), 'asof_2_ok': int(a['asof_2_ok'].sum()),
        'asof_5_age_min': a['asof_5_age_min'].describe().round(2).to_dict(),
        'horse_count_mismatch_at_asof5': int((a['asof_5_horses'].notna() & a['asof_5_horses'].ne(a['starters'])).sum()),
        'kinds': a['kinds'].value_counts().to_dict(),
        'final_period_races': int(a['final_period'].sum()),
        'dev_last_equals_final_mean': float(a['dev_last_equals_final'].mean()),
        'dev_races_last_not_all_equal': int((a['dev_last_equals_final'] < 1).sum()),
    }
    (data / 'audit_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print(by_month.to_string())


if __name__ == '__main__':
    main()
