"""JV-Link で時系列オッズ（0B42：馬連の中間オッズ）をレースごとに取得し、原文のまま保存する。

- 利用キーは扱わない（JVInit("UNKNOWN")）。1レースずつ、前のレースの終了から最低1.2秒空ける
- レースごとに受け取ったレコード（O2、各2042バイト）を raw/<race_id>.bin に連結して保存し、完了印を残す。
  再実行すると完了済みのレースは飛ばす
- 認証系のエラー（-300番台）・JVInit の失敗で停止する。その他の負の戻り値は記録して次へ進む
- 組ごとの表は作らない（1レース約180配信×最大153組で大きいため）。解析は tools/odds_snapshots.parse_o2 で必要な配信だけ行う
- --until で、それ以降の開催日のレースを対象から外す（最終確認期間を読まないため）

使い方:
  .venv/Scripts/python.exe tools/jvlink_collect_0b42.py --targets data/odds/20261024_setup/targets_all_20251001_20261021.csv
      --out data/odds/jv_0b42 --since 2025-11-01 --until 2026-07-01
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.jvlink_collect_0b41 import collect_race  # noqa: E402
from tools.odds_snapshots import JST, netkeiba_id  # noqa: E402

O2_SIZE = 2042


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--targets', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--since', required=True)
    ap.add_argument('--until', required=True)
    args = ap.parse_args()
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    (out / 'done').mkdir(exist_ok=True)
    targets = [t for t in csv.DictReader((ROOT / args.targets).open(encoding='utf-8-sig'))
               if args.since <= t['race_date'] < args.until]
    print(f'{len(targets)} target races {args.since} .. {args.until} (exclusive)', flush=True)

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    init = int(jv.JVInit('UNKNOWN'))
    if init != 0:
        raise SystemExit(f'JVInit failed ({init})')
    last_end = 0.0
    done = skipped = 0
    with (out / 'collection_log.jsonl').open('a', encoding='utf-8') as lf:
        for t in targets:
            key, rid = t['jv_key'], netkeiba_id(t['jv_key'])
            if (out / 'done' / rid).exists():
                skipped += 1
                continue
            time.sleep(max(0.0, 1.2 - (time.monotonic() - last_end)))
            started = datetime.now(JST).isoformat()
            code, records, err = collect_race(jv, key, '0B42', O2_SIZE)
            last_end = time.monotonic()
            bad = sum(len(r) != O2_SIZE for r in records)
            entry = {'race_id': rid, 'jv_key': key, 'started_at': started, 'open_code': code, 'records': len(records),
                     'bad_length': bad, 'gets_error': err, 'finished_at': datetime.now(JST).isoformat()}
            lf.write(json.dumps(entry) + '\n')
            lf.flush()
            if -399 <= code <= -300 or (err is not None and -399 <= err <= -300):
                raise SystemExit(f'authentication error for {rid}: open={code} gets={err}; stopped')
            (out / 'raw' / f'{rid}.bin').write_bytes(b''.join(records))
            if code in (0, -1) and err is None and bad == 0:
                (out / 'done' / rid).write_text(entry['finished_at'], encoding='utf-8')
            done += 1
            if done % 100 == 0:
                print(f"[{datetime.now(JST):%H:%M:%S}] {done} races collected ({skipped} skipped)", flush=True)
    print(f'finished: collected {done}, skipped {skipped}', flush=True)


if __name__ == '__main__':
    main()
