"""開催日ごとのレース一覧（netkeiba の公開ページ）から、各レースの発走予定時刻を取得する。

時系列オッズ（JV-Link 0B41）には発走時刻が入っていないため、「発走 N 分前のオッズ」を選ぶのに使う。
- 1開催日1リクエスト、前のリクエストから最低1.5秒空ける。HTTP エラーで停止し、再試行しない
- 取得したページは原文のまま保存し、再実行時は保存済みのページを使う
- 一覧の時刻は「発走予定」。当日の発走時刻の変更は反映されていない場合がある

使い方:
  .venv/Scripts/python.exe tools/fetch_post_times.py --targets data/odds/20261023_setup/targets.csv --out data/odds/post_times
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
URL = 'https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={date}'


def parse_race_list(html: str, date: str) -> pd.DataFrame:
    """レース一覧の1項目ごとに race_id と発走予定時刻を取り出す。"""
    rows = []
    for block in re.split(r'<li class="RaceList_DataItem', html)[1:]:
        rid = re.search(r'race_id=(\d{12})', block)
        tm = re.search(r'RaceList_Itemtime">\s*(\d{1,2}):(\d{2})', block)
        if not rid or not tm:
            continue
        rows.append({'race_id': rid[1], 'race_date': f'{date[:4]}-{date[4:6]}-{date[6:]}',
                     'post_time': f'{date[:4]}-{date[4:6]}-{date[6:]}T{int(tm[1]):02d}:{tm[2]}:00+09:00'})
    df = pd.DataFrame(rows, columns=['race_id', 'race_date', 'post_time']).drop_duplicates('race_id')
    return df


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--targets', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    targets = pd.read_csv(ROOT / args.targets, dtype=str, encoding='utf-8-sig')
    dates = sorted(targets['race_date'].str.replace('-', '').unique())
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ja-JP,ja;q=0.9'})
    frames, log = [], []
    last = 0.0
    for d in dates:
        path = out / 'raw' / f'race_list_{d}.html'
        if not path.exists():
            time.sleep(max(0.0, 1.5 - (time.monotonic() - last)))
            r = session.get(URL.format(date=d), timeout=30)
            last = time.monotonic()
            r.raise_for_status()
            path.write_bytes(r.content)
            log.append({'date': d, 'bytes': len(r.content), 'sha256': hashlib.sha256(r.content).hexdigest()})
        df = parse_race_list(path.read_bytes().decode('utf-8', errors='replace'), d)
        frames.append(df)
        print(f'{d}: {len(df)} races', flush=True)
    pt = pd.concat(frames, ignore_index=True)
    pt.to_csv(out / 'post_times.csv', index=False, encoding='utf-8')
    missing = sorted(set(targets['race_id']) - set(pt['race_id']))
    pd.DataFrame(log).to_csv(out / 'requests.csv', index=False, mode='a', header=not (out / 'requests.csv').exists())
    print(f'post times: {len(pt)} races, targets without post time: {len(missing)}', missing[:10])


if __name__ == '__main__':
    main()
