"""JV-Link で対象レースの時系列オッズ（0B41：単勝・複勝の中間オッズ）をまとめて取得する。

- 利用キーは扱わない（JVInit("UNKNOWN")）。1レースずつ、前のレースの終了から最低1.2秒空ける
- レースごとに受け取ったレコードを原文のまま保存（raw/<race_id>.bin、レコードを順に連結）し、完了印を残す。
  再実行すると完了済みのレースは飛ばす（中断しても続きから再開できる）
- 認証系のエラー（-301 など -300 番台）・JVInit の失敗は、その場で停止する。その他の負の戻り値は記録して次へ進む
- 解析は tools/odds_snapshots.parse_o1。単勝・複勝の値を quotes.csv に縦持ちで追記する

使い方:
  .venv/Scripts/python.exe tools/jvlink_collect_0b41.py --targets data/odds/20261023_setup/targets.csv --out data/odds/jv_0b41
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.odds_snapshots import JST, netkeiba_id, parse_o1  # noqa: E402

QUOTE_FIELDS = ['race_id', 'jv_key', 'record_index', 'data_kind', 'published_at', 'registered', 'runners',
                'horse_number', 'win_odds', 'win_status', 'win_popularity', 'place_low', 'place_high', 'place_popularity']


def split_records(blob: bytes) -> list[bytes]:
    """連結した O1 レコード（各 962 バイト、CRLF 終端）を分ける。"""
    if len(blob) % 962:
        raise ValueError(f'blob length {len(blob)} is not a multiple of 962')
    return [blob[i:i + 962] for i in range(0, len(blob), 962)]


def collect_race(jv, key: str) -> tuple[int, list[bytes], int | None]:
    code = int(jv.JVRTOpen('0B41', key))
    records, err = [], None
    try:
        if code == 0:
            while True:
                rc, buf, _ = jv.JVGets(bytearray(110000), 110000, bytearray())
                rc = int(rc)
                if rc == 0:
                    break
                if rc == -1:
                    continue
                if rc < 0:
                    err = rc
                    break
                raw = bytes(buf)[:rc]
                if len(raw) == 960:
                    raw += b'\r\n'
                records.append(raw)
    finally:
        jv.JVClose()
    return code, records, err


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--targets', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    (out / 'done').mkdir(exist_ok=True)
    targets = list(csv.DictReader((ROOT / args.targets).open(encoding='utf-8-sig')))
    if args.limit:
        targets = targets[:args.limit]

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    init = int(jv.JVInit('UNKNOWN'))
    if init != 0:
        raise SystemExit(f'JVInit failed ({init})')
    qpath = out / 'quotes.csv'
    new_q = not qpath.exists()
    last_end = 0.0
    done = skipped = 0
    with qpath.open('a', newline='', encoding='utf-8') as qf, (out / 'collection_log.jsonl').open('a', encoding='utf-8') as lf:
        qw = csv.DictWriter(qf, fieldnames=QUOTE_FIELDS)
        if new_q:
            qw.writeheader()
        for t in targets:
            key, rid = t['jv_key'], netkeiba_id(t['jv_key'])
            if (out / 'done' / rid).exists():
                skipped += 1
                continue
            time.sleep(max(0.0, 1.2 - (time.monotonic() - last_end)))
            started = datetime.now(JST).isoformat()
            code, records, err = collect_race(jv, key)
            last_end = time.monotonic()
            entry = {'race_id': rid, 'jv_key': key, 'started_at': started, 'open_code': code,
                     'records': len(records), 'gets_error': err, 'parse_errors': 0}
            if -399 <= code <= -300 or (err is not None and -399 <= err <= -300):
                lf.write(json.dumps(entry) + '\n')
                raise SystemExit(f'authentication error for {rid}: open={code} gets={err}; stopped')
            (out / 'raw' / f'{rid}.bin').write_bytes(b''.join(records))
            for i, raw in enumerate(records):
                try:
                    rec = parse_o1(raw)
                except Exception as e:  # 解析できないレコードは数だけ残す（原文は raw にある）
                    entry['parse_errors'] += 1
                    entry.setdefault('first_parse_error', repr(e)[:160])
                    continue
                for q in rec['quotes']:
                    qw.writerow({'race_id': rec['race_id'], 'jv_key': rec['jv_key'], 'record_index': i,
                                 'data_kind': rec['data_kind'], 'published_at': rec['source_published_at'],
                                 'registered': rec['registered'], 'runners': rec['runners'],
                                 **{k: q[k] for k in ('horse_number', 'win_odds', 'win_status', 'win_popularity',
                                                      'place_low', 'place_high', 'place_popularity')}})
            qf.flush()
            entry['finished_at'] = datetime.now(JST).isoformat()
            lf.write(json.dumps(entry) + '\n')
            lf.flush()
            if code in (0, -1) and err is None:
                (out / 'done' / rid).write_text(entry['finished_at'], encoding='utf-8')
            done += 1
            if done % 50 == 0:
                print(f"[{datetime.now(JST):%H:%M:%S}] {done} races collected ({skipped} skipped)", flush=True)
    print(f'finished: collected {done}, skipped {skipped}', flush=True)


if __name__ == '__main__':
    main()
