"""保存済みの 0B41 の原文（raw/<race_id>.bin）を読み直し、配信ごとの票数合計の表を作る。

JV-Link の追加取得はしない。出力は git 対象外の data/odds/jv_0b41/pools.csv:
  race_id, data_kind, published_at, win_pool, place_pool, wakuren_pool（単位百円、返還分を含む）
同じ配信（データ区分・発表時刻）が重複して入っていれば1行にする。

検査（標準出力と pools_audit.json）:
  - 中間の配信で、票数合計が時刻とともに減るレース
  - 締切後（データ区分 3・4）の票数 < 最後の中間の票数 となるレース

使い方:
  .venv/Scripts/python.exe tools/build_pool_table.py --dir data/odds/jv_0b41
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.jvlink_collect_0b41 import split_records  # noqa: E402
from tools.odds_snapshots import parse_o1  # noqa: E402

POOLS = ['win_pool', 'place_pool', 'wakuren_pool']


def pools_from_blob(blob: bytes) -> list[dict]:
    rows = []
    for raw in split_records(blob):
        r = parse_o1(raw)
        rows.append({'race_id': r['race_id'], 'data_kind': r['data_kind'], 'published_at': r['source_published_at'],
                     **{k: r[k] for k in POOLS}})
    return rows


def audit(df: pd.DataFrame) -> dict:
    mid = df[df['data_kind'] == '1'].sort_values(['race_id', 'published_at'])
    dec = mid.groupby('race_id')['win_pool'].apply(lambda s: bool((s.diff().dropna() < 0).any()))
    last_mid = mid.groupby('race_id')['win_pool'].last()
    after = df[df['data_kind'].isin(['3', '4'])].groupby('race_id')['win_pool'].max()
    both = pd.concat([last_mid, after], axis=1, keys=['last_mid', 'after']).dropna()
    return {'races': int(df['race_id'].nunique()), 'rows': len(df),
            'races_with_decreasing_interim_win_pool': int(dec.sum()),
            'races_after_close_below_last_interim': int((both['after'] < both['last_mid']).sum()),
            'races_with_after_close': int(len(both)), 'rows_without_win_pool': int(df['win_pool'].isna().sum())}


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dir', required=True)
    a = ap.parse_args()
    d = ROOT / a.dir
    rows = []
    for f in sorted((d / 'raw').glob('*.bin')):
        rows.extend(pools_from_blob(f.read_bytes()))
    df = pd.DataFrame(rows).drop_duplicates(['race_id', 'data_kind', 'published_at'])
    df.to_csv(d / 'pools.csv', index=False, encoding='utf-8')
    res = audit(df)
    (d / 'pools_audit.json').write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(res, ensure_ascii=False))


if __name__ == '__main__':
    main()
