"""開催日に JV-Link の時系列オッズ（0B41）を繰り返し読み、各配信が手元に届くまでの遅れを測る。

- 当日のレース一覧（netkeiba の公開ページ、1回だけ取得）から発走予定時刻を取り、対象レースを選ぶ
- 各レースについて、発走 --before 分前〜 --after 分後の間、約 --interval 秒ごとに JVRTOpen('0B41') で読む
- 新しい配信（データ区分・発表時刻の組）を初めて受け取った時刻を記録する
  遅れ = 初めて受け取った時刻 − 発表時刻（発表時刻は分単位なので、0〜60秒の幅を含む）
  残り = 発走予定時刻 − 初めて受け取った時刻（その配信を見てから投票できる時間の目安。即PAT の締切は発走1分前）
- 利用キーは扱わない（JVInit("UNKNOWN")）。呼び出しの間は最低1.2秒空ける。認証系のエラー（-300番台）で停止する
- 受け取ったレコードは原文のまま保存する（git 対象外の data/odds/ の下）

使い方:
  .venv/Scripts/python.exe tools/jvlink_live_latency.py run --date 20261025 --out data/odds/live_latency/20261025 --races 202605050911 ...
  .venv/Scripts/python.exe tools/jvlink_live_latency.py report --out data/odds/live_latency/20261025
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.fetch_post_times import URL, parse_race_list  # noqa: E402
from tools.jvlink_collect_0b41 import collect_race  # noqa: E402
from tools.odds_snapshots import JST, parse_o1  # noqa: E402

MIN_GAP = 1.2


def jv_key(race_id: str, date: str) -> str:
    """netkeiba の race_id（年・場・回・日・R）と開催日から、JV-Link のキー（開催日・場・回・日・R）を作る。"""
    return date + race_id[4:]


def load_schedule(date: str, out: Path) -> pd.DataFrame:
    path = out / f'race_list_{date}.html'
    if not path.exists():
        r = requests.get(URL.format(date=date), headers={'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ja-JP,ja;q=0.9'},
                         timeout=30)
        r.raise_for_status()
        path.write_bytes(r.content)
    df = parse_race_list(path.read_bytes().decode('utf-8', errors='replace'), date)
    df['post_time'] = pd.to_datetime(df['post_time'])
    return df


def new_snapshots(records: list[bytes], seen: set, race_id: str, post_time: datetime, now: datetime) -> list[dict]:
    """まだ見ていない配信（データ区分・発表時刻）を、受け取った時刻つきで返す。"""
    rows = []
    for raw in records:
        rec = parse_o1(raw)
        key = (rec['data_kind'], rec['source_published_at'])
        if key in seen:
            continue
        seen.add(key)
        pub = datetime.fromisoformat(rec['source_published_at']) if rec['source_published_at'] else None
        rows.append({'race_id': race_id, 'data_kind': rec['data_kind'], 'published_at': rec['source_published_at'],
                     'first_seen_at': now.isoformat(), 'post_time': post_time.isoformat(),
                     'published_min_before_post': None if pub is None else round((post_time - pub).total_seconds() / 60, 2),
                     'lag_seconds': None if pub is None else round((now - pub).total_seconds(), 1),
                     'seconds_left_before_post': round((post_time - now).total_seconds(), 1)})
    return rows


def run(args):
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    sched = load_schedule(args.date, out)
    if args.races:
        sched = sched[sched['race_id'].isin(args.races)]
    now = datetime.now(JST)
    sched = sched[sched['post_time'] + timedelta(minutes=args.after) > now].sort_values('post_time')
    if sched.empty:
        raise SystemExit('no target races left today')
    print(sched.to_string(index=False), flush=True)

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    init = int(jv.JVInit('UNKNOWN'))
    if init != 0:
        raise SystemExit(f'JVInit failed ({init})')
    seen = {rid: set() for rid in sched['race_id']}
    last_end = 0.0
    log_path = out / 'snapshots.jsonl'
    with log_path.open('a', encoding='utf-8') as log, (out / 'polls.jsonl').open('a', encoding='utf-8') as polls:
        while True:
            now = datetime.now(JST)
            end = (sched['post_time'] + timedelta(minutes=args.after)).max()
            if now > end:
                break
            cycle_start = time.monotonic()
            active = sched[(sched['post_time'] - timedelta(minutes=args.before) <= now)
                           & (now <= sched['post_time'] + timedelta(minutes=args.after))]
            for _, r in active.iterrows():
                time.sleep(max(0.0, MIN_GAP - (time.monotonic() - last_end)))
                t0 = datetime.now(JST)
                code, records, err = collect_race(jv, jv_key(r['race_id'], args.date))
                t1 = datetime.now(JST)
                last_end = time.monotonic()
                polls.write(json.dumps({'race_id': r['race_id'], 'started_at': t0.isoformat(), 'finished_at': t1.isoformat(),
                                        'open_code': code, 'gets_error': err, 'records': len(records)}) + '\n')
                polls.flush()
                if -399 <= code <= -300 or (err is not None and -399 <= err <= -300):
                    raise SystemExit(f'authentication error for {r["race_id"]}: open={code} gets={err}; stopped')
                if records:
                    rows = new_snapshots(records, seen[r['race_id']], r['race_id'], r['post_time'].to_pydatetime(), t1)
                    if rows:
                        with (out / 'raw' / f"{r['race_id']}.bin").open('ab') as f:
                            f.write(b''.join(records))
                    for row in rows:
                        log.write(json.dumps(row, ensure_ascii=False) + '\n')
                        if row['data_kind'] != '1' or (row['published_min_before_post'] or 99) <= 12:
                            print(f"[{t1:%H:%M:%S}] {row['race_id']} kind={row['data_kind']} pub={row['published_at']} "
                                  f"lag={row['lag_seconds']}s left={row['seconds_left_before_post']}s", flush=True)
                    log.flush()
            wait = args.interval - (time.monotonic() - cycle_start)
            if active.empty:
                nxt = (sched['post_time'] - timedelta(minutes=args.before) - datetime.now(JST)).dt.total_seconds()
                nxt = nxt[nxt > 0]
                wait = min(nxt.min(), 300) if len(nxt) else args.interval
            time.sleep(max(1.0, wait))
    jv.JVClose()
    print('finished', flush=True)


def report(args):
    out = ROOT / args.out
    s = pd.read_json(out / 'snapshots.jsonl', lines=True, dtype={'race_id': str, 'data_kind': str})
    p = pd.read_json(out / 'polls.jsonl', lines=True, dtype={'race_id': str})
    p['seconds'] = (pd.to_datetime(p['finished_at']) - pd.to_datetime(p['started_at'])).dt.total_seconds()
    near = s[(s['data_kind'] == '1') & (s['published_min_before_post'] <= 12)].copy()
    near['offset'] = near['published_min_before_post'].round().astype(int)
    tab = near.groupby('offset').agg(n=('lag_seconds', 'size'), lag_median=('lag_seconds', 'median'),
                                     lag_max=('lag_seconds', 'max'), left_min=('seconds_left_before_post', 'min'),
                                     left_median=('seconds_left_before_post', 'median'))
    res = {'races': int(s['race_id'].nunique()), 'polls': len(p), 'poll_seconds_median': float(p['seconds'].median()),
           'poll_seconds_max': float(p['seconds'].max()), 'open_codes': p['open_code'].value_counts().to_dict(),
           'by_published_offset': tab.reset_index().to_dict('records'),
           'post_close_kinds': s[s['data_kind'] != '1'].groupby('data_kind')['seconds_left_before_post'].median().to_dict()}
    (out / 'report.json').write_text(json.dumps(res, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    print(json.dumps(res, ensure_ascii=False, indent=2, default=float))


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    r = sub.add_parser('run')
    r.add_argument('--date', required=True)
    r.add_argument('--out', required=True)
    r.add_argument('--races', nargs='*')
    r.add_argument('--before', type=float, default=15)
    r.add_argument('--after', type=float, default=2)
    r.add_argument('--interval', type=float, default=20)
    q = sub.add_parser('report')
    q.add_argument('--out', required=True)
    a = ap.parse_args()
    run(a) if a.cmd == 'run' else report(a)


if __name__ == '__main__':
    main()
