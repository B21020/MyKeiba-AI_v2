"""JV-Link で調教データ（SLOP：坂路、WOOD：ウッドチップ）を期間を指定して取得する。

- 利用キーは扱わない（JVInit("UNKNOWN")）。dataspec ごと・期間ごとに JVOpen → ダウンロード完了を JVStatus で待つ → JVGets で読む
- option は既定で 1（通常データ）。セットアップ（3・4）は初回にダイアログが出るので使わない
- 受け取ったレコードは原文のまま raw/<spec>_<期間>.bin に保存し、解析結果を training_<spec>.csv に追記する
- 期間ごとに完了印を残し、再実行時は飛ばす。認証エラー（-300番台）は停止

使い方:
  .venv/Scripts/python.exe tools/jvlink_fetch_training.py --spec SLOP --start 2025-10-01 --end 2025-10-08 --out data/jv_training
  .venv/Scripts/python.exe tools/jvlink_fetch_training.py --spec SLOP WOOD --start 2020-01-01 --end 2026-10-22 --chunk-days 31 --out data/jv_training
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.preparing._jv_training import parse_hc, parse_wc  # noqa: E402

PARSERS = {'SLOP': parse_hc, 'WOOD': parse_wc}


def fetch(jv, spec: str, fromtime: str, option: int, timeout_s: int = 600) -> dict:
    res = jv.JVOpen(spec, fromtime, option, 0, 0, '')
    code = int(res[0]) if isinstance(res, tuple) else int(res)
    info = {'spec': spec, 'fromtime': fromtime, 'option': option, 'open_code': code}
    if isinstance(res, tuple) and len(res) >= 4:
        info.update(readcount=int(res[1]), downloadcount=int(res[2]), lastfiletimestamp=str(res[3]))
    records = []
    try:
        if code != 0:
            return {**info, 'records': records}
        dl = info.get('downloadcount', 0)
        t0 = time.monotonic()
        while dl:
            st = int(jv.JVStatus())
            if st < 0:
                info['status_error'] = st
                return {**info, 'records': records}
            if st >= dl:
                break
            if time.monotonic() - t0 > timeout_s:
                info['status_error'] = 'timeout'
                return {**info, 'records': records}
            time.sleep(1)
        while True:
            rc, buf, fname = jv.JVGets(bytearray(110000), 110000, bytearray())
            rc = int(rc)
            if rc == 0:
                break
            if rc in (-1, -3):  # ファイルの切り替わり・ダウンロード中
                if rc == -3:
                    time.sleep(0.5)
                continue
            if rc < 0:
                info['gets_error'] = rc
                break
            records.append(bytes(buf)[:rc])
    finally:
        jv.JVClose()
    return {**info, 'records': records}


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--spec', nargs='+', choices=list(PARSERS), required=True)
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--chunk-days', type=int, default=7)
    ap.add_argument('--option', type=int, default=1)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    (out / 'done').mkdir(exist_ok=True)

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    init = int(jv.JVInit('UNKNOWN'))
    if init != 0:
        raise SystemExit(f'JVInit failed ({init})')
    start, end = datetime.fromisoformat(args.start), datetime.fromisoformat(args.end)
    with (out / 'fetch_log.jsonl').open('a', encoding='utf-8') as log:
        for spec in args.spec:
            parse = PARSERS[spec]
            csv_path = out / f'training_{spec}.csv'
            writer_file = csv_path.open('a', newline='', encoding='utf-8')
            writer = None
            a = start
            while a < end:
                b = min(a + timedelta(days=args.chunk_days), end)
                tag = f'{spec}_{a:%Y%m%d}_{b:%Y%m%d}'
                if (out / 'done' / tag).exists():
                    a = b
                    continue
                fromtime = f'{a:%Y%m%d}000000-{b:%Y%m%d}000000'
                r = fetch(jv, spec, fromtime, args.option)
                recs = r.pop('records')
                (out / 'raw' / f'{tag}.bin').write_bytes(b''.join(recs))
                parsed, errors = [], 0
                for raw in recs:
                    try:
                        parsed.append(parse(raw))
                    except Exception:
                        errors += 1
                if parsed:
                    if writer is None:
                        writer = csv.DictWriter(writer_file, fieldnames=list(parsed[0].keys()))
                        if csv_path.stat().st_size == 0:
                            writer.writeheader()
                    writer.writerows(parsed)
                    writer_file.flush()
                r.update(tag=tag, records=len(recs), parsed=len(parsed), parse_errors=errors,
                         at=datetime.now().isoformat(timespec='seconds'))
                log.write(json.dumps(r, ensure_ascii=False) + '\n')
                log.flush()
                print(json.dumps(r, ensure_ascii=False), flush=True)
                code = r['open_code']
                if -399 <= code <= -300:
                    raise SystemExit(f'authentication error ({code}); stopped')
                # -1（該当データなし）は完了扱いにしない（取得方法・期間を変えて取り直せるように）
                if code == 0 and 'gets_error' not in r and 'status_error' not in r:
                    (out / 'done' / tag).write_text(r['at'], encoding='utf-8')
                a = b
            writer_file.close()


if __name__ == '__main__':
    main()
