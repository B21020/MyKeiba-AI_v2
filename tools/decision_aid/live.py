"""意思決定の補助：開催日に JV-Link の0B41を読み、判断時刻（発走4分前・6分前の配信）の数字をブラウザに出す。
あわせて、固定した規則で複勝の紙上の検証を記録し、最終レースの後に精算する。

サブコマンド:
  run     開催日に動かす。発走15分前〜2分後に約20秒ごとに読み、6分前の配信を受け取った時点で判断して記録する。
          ページ：http://127.0.0.1:8765（自分のPCだけ）。最終レースの約30分後に精算して終わる
  serve   記録を見るだけのページを開く（JV-Link は使わない）
  replay  保存済みの0B41原文（data/odds/jv_0b41/raw）で、ある過去の開催日を再生する（受け取りは発表の --lag 秒後とみなす）。
          記録は --db の別のデータベースに書く（本番の紙上の記録には混ぜない）

利用キーは扱わない（JVInit("UNKNOWN")）。呼び出しの間は最低1.2秒空け、認証系のエラー（-300番台）で停止する。

使い方:
  .venv/Scripts/python.exe tools/decision_aid/live.py run --date 20261107
  .venv/Scripts/python.exe tools/decision_aid/live.py serve --date 20261107
  .venv/Scripts/python.exe tools/decision_aid/live.py replay --date 20260530 --db data/decision_aid/replay.sqlite
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.decision_aid.core import Store, latest_t6, load_rule  # noqa: E402
from tools.jvlink_collect_0b41 import collect_race, split_records  # noqa: E402
from tools.jvlink_live_latency import jv_key, load_schedule  # noqa: E402
from tools.odds_snapshots import JST, parse_o1  # noqa: E402

RULE = ROOT / 'reproducibility/20261101_decision_aid/frozen_rule.json'
DB = ROOT / 'data/decision_aid/decision_aid.sqlite'
STATIC = Path(__file__).resolve().parent / 'static'
MIN_GAP = 1.2
BEFORE_MIN, AFTER_MIN, INTERVAL = 15, 2, 20
SETTLE_AFTER_MIN = 30


def iso_date(date: str) -> str:
    return f'{date[:4]}-{date[4:6]}-{date[6:]}'


def parse_all(records: list[bytes]) -> list[dict]:
    out = []
    for raw in records:
        try:
            out.append(parse_o1(raw))
        except ValueError:
            continue
    return out


def serve(store: Store, rule: dict, date: str, port: int, clock=lambda: datetime.now(JST)) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            u = urlparse(self.path)
            if u.path == '/api/state':
                d = parse_qs(u.query).get('date', [iso_date(date)])[0]
                body = json.dumps(store.state(d, clock(), rule), ensure_ascii=False, default=float).encode('utf-8')
                ctype = 'application/json; charset=utf-8'
            elif u.path in ('/', '/index.html'):
                body, ctype = (STATIC / 'index.html').read_bytes(), 'text/html; charset=utf-8'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', ctype)
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f'page: http://127.0.0.1:{port}/  (date {iso_date(date)})', flush=True)
    return httpd


def decide(store: Store, rule: dict, rid: str, records: list[dict], post: datetime, received_at: datetime | None,
           now: datetime) -> dict:
    rec = latest_t6(records, post, rule['paper_rule']['snapshot_published_at_or_before_min'])
    return store.record_decision(rid, rec, received_at if rec else None, now, post, rule)


def run(a):
    rule, store = load_rule(Path(a.rule)), Store(Path(a.db))
    raw_dir = ROOT / 'data/decision_aid/raw' / a.date
    raw_dir.mkdir(parents=True, exist_ok=True)
    sched = load_schedule(a.date, raw_dir)
    if sched.empty:
        print('no races on this date', flush=True)
        return
    store.save_schedule(iso_date(a.date), sched)
    httpd = serve(store, rule, a.date, a.port)

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    init = int(jv.JVInit('UNKNOWN'))
    if init != 0:
        raise SystemExit(f'JVInit failed ({init})')
    skip_min = rule['paper_rule']['skip_after_min']
    last_end = 0.0
    last_poll = (sched['post_time'] + timedelta(minutes=AFTER_MIN)).max()
    try:
        while datetime.now(JST) <= last_poll:
            cycle = time.monotonic()
            now = datetime.now(JST)
            for r in sched.itertuples():
                post = r.post_time.to_pydatetime()
                if store.decided(r.race_id) or not (post - timedelta(minutes=BEFORE_MIN) <= now <= post + timedelta(minutes=AFTER_MIN)):
                    continue
                time.sleep(max(0.0, MIN_GAP - (time.monotonic() - last_end)))
                code, records, err = collect_race(jv, jv_key(r.race_id, a.date))
                last_end = time.monotonic()
                if -399 <= code <= -300 or (err is not None and -399 <= err <= -300):
                    raise SystemExit(f'authentication error for {r.race_id}: open={code} gets={err}; stopped')
                got = datetime.now(JST)
                parsed = parse_all(records)
                if latest_t6(parsed, post, rule['paper_rule']['snapshot_published_at_or_before_min']) is not None:
                    (raw_dir / f'{r.race_id}.bin').write_bytes(b''.join(records))
                    res = decide(store, rule, r.race_id, parsed, post, got, got)
                    print(f'[{got:%H:%M:%S}] {r.race_id} decided: {res}', flush=True)
                elif got > post - timedelta(minutes=skip_min):
                    res = decide(store, rule, r.race_id, [], post, None, got)
                    print(f'[{got:%H:%M:%S}] {r.race_id} skipped: {res}', flush=True)
            time.sleep(max(1.0, INTERVAL - (time.monotonic() - cycle)))
    finally:
        jv.JVClose()
    # 最終レースの結果ページが出るまで待ってから精算する（ページはその間も見られる）
    wait = (sched['post_time'].max() + timedelta(minutes=SETTLE_AFTER_MIN) - datetime.now(JST)).total_seconds()
    time.sleep(max(0.0, wait))
    from tools.decision_aid.settle import settle
    try:
        print('settlement:', settle(a.date, Path(a.db)), flush=True)
    except Exception as e:  # 精算は翌日以降に手で再実行できる
        print(f'settlement failed: {e!r}', flush=True)
    httpd.shutdown()


def replay(a):
    """保存済みの原文で、過去の開催日を再生する。受け取り時刻 = 発表時刻 + lag 秒。"""
    rule, store = load_rule(Path(a.rule)), Store(Path(a.db))
    post = pd.read_csv(ROOT / 'data/odds/post_times/post_times.csv', dtype={'race_id': str})
    post['post_time'] = pd.to_datetime(post['post_time'])
    day = post[post['race_date'] == iso_date(a.date)].sort_values('post_time')
    if day.empty:
        raise SystemExit('no races for this date in post_times.csv')
    if day['post_time'].max() >= pd.Timestamp('2026-07-01', tz='Asia/Tokyo'):
        raise SystemExit('replay is limited to the development period (before 2026-07-01)')
    store.save_schedule(iso_date(a.date), day)
    n = 0
    for r in day.itertuples():
        path = ROOT / 'data/odds/jv_0b41/raw' / f'{r.race_id}.bin'
        if not path.exists() or store.decided(r.race_id):
            continue
        p = r.post_time.to_pydatetime()
        parsed = parse_all(split_records(path.read_bytes()))
        rec = latest_t6(parsed, p, rule['paper_rule']['snapshot_published_at_or_before_min'])
        got = datetime.fromisoformat(rec['source_published_at']) + timedelta(seconds=a.lag) if rec else None
        res = decide(store, rule, r.race_id, parsed, p, got, got or p)
        n += 1
        print(f'{r.race_id} {res}', flush=True)
    print(f'replayed {n} races into {a.db}', flush=True)
    if a.serve:
        last = day['post_time'].max().to_pydatetime()
        serve(store, rule, a.date, a.port, clock=lambda: last - timedelta(minutes=10))
        print('press Ctrl+C to stop', flush=True)
        while True:
            time.sleep(3600)


def serve_only(a):
    rule, store = load_rule(Path(a.rule)), Store(Path(a.db))
    serve(store, rule, a.date, a.port)
    print('press Ctrl+C to stop', flush=True)
    while True:
        time.sleep(3600)


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name in ('run', 'serve', 'replay'):
        s = sub.add_parser(name)
        s.add_argument('--date', required=True)
        s.add_argument('--db', default=str(DB))
        s.add_argument('--rule', default=str(RULE))
        s.add_argument('--port', type=int, default=8765)
        if name == 'replay':
            s.add_argument('--lag', type=float, default=100.0)
            s.add_argument('--serve', action='store_true')
    a = ap.parse_args()
    {'run': run, 'serve': serve_only, 'replay': replay}[a.cmd](a)


if __name__ == '__main__':
    main()
