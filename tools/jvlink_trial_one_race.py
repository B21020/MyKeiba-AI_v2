"""JV-Link で1レース分の時系列オッズ（0B41）を取得し、中身を確かめる（お試し期間の動作確認用）。

- 利用キーは扱わない。JVInit には公式案内どおり "UNKNOWN" を渡す
- 1レースだけ・再試行なし。受け取ったレコードは原文のまま保存する
- 既存の data/odds/snapshots.sqlite には書かない
- 最後の時刻のオッズを、結果表の確定単勝オッズと比べる

使い方:
  .venv/Scripts/python.exe tools/jvlink_trial_one_race.py --jv-key 2026062810020211 --out data/odds/20261024_jv_trial
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.odds_snapshots import JST, netkeiba_id, parse_o1  # noqa: E402


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--jv-key', required=True, help='YYYYMMDD+場+回+日+R の16桁')
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    out = ROOT / args.out
    (out / 'raw').mkdir(parents=True, exist_ok=True)
    rid = netkeiba_id(args.jv_key)
    summary = {'jv_key': args.jv_key, 'race_id': rid, 'started_at': datetime.now(JST).isoformat()}

    import win32com.client
    jv = win32com.client.Dispatch('JVDTLab.JVLink')
    summary['jvinit'] = int(jv.JVInit('UNKNOWN'))
    if summary['jvinit'] != 0:
        return finish(out, summary)
    summary['jvrtopen'] = int(jv.JVRTOpen('0B41', args.jv_key))
    records, errors = [], []
    try:
        if summary['jvrtopen'] == 0:
            while True:
                code, buf, filename = jv.JVGets(bytearray(110000), 110000, bytearray())
                code = int(code)
                if code == 0:
                    break
                if code == -1:
                    continue
                if code < 0:
                    summary['jvgets_error'] = code
                    break
                raw = bytes(buf)[:code]
                (out / 'raw' / f'{len(records):04d}.bin').write_bytes(raw)
                try:
                    rec = parse_o1(raw)
                    records.append(rec)
                except Exception as e:  # 記録して続ける
                    errors.append({'index': len(records) + len(errors), 'error': repr(e)[:200], 'head': raw[:30].decode('ascii', 'replace')})
    finally:
        jv.JVClose()
    summary['records'] = len(records)
    summary['parse_errors'] = errors[:10]
    if records:
        kinds = pd.Series([r['data_kind'] for r in records]).value_counts().to_dict()
        times = sorted({r['source_published_at'] for r in records if r['source_published_at']})
        summary['data_kinds'] = kinds
        summary['published_first'] = times[0] if times else None
        summary['published_last'] = times[-1] if times else None
        steps = pd.Series(pd.to_datetime(times)).diff().dt.total_seconds().div(60).dropna()
        summary['interval_minutes'] = steps.describe().round(2).to_dict() if len(steps) else None
        summary['published_times_last10'] = times[-10:]
        first = records[0]
        summary['horses'] = len(first['quotes'])
        summary['has_place_bounds'] = all(q['place_low'] is not None and q['place_high'] is not None
                                          for q in first['quotes'] if q['win_status'] == 'quoted')
        # 最後の時刻のオッズと確定単勝オッズ（結果表）を比べる
        last = max(records, key=lambda r: (r['source_published_at'] or '', r['data_kind']))
        res = pd.read_pickle(ROOT / 'data/raw/results.pickle')
        res = res[res.index.astype(str) == rid].drop_duplicates()
        odds_col = res['単勝'].where(res['単勝'].notna(), res.get('単勝オッズ'))
        final = dict(zip(pd.to_numeric(res['馬番'], errors='coerce').astype('Int64'), pd.to_numeric(odds_col, errors='coerce')))
        comp = [{'horse': q['horse_number'], 'last_interim': q['win_odds'], 'final': final.get(q['horse_number'])}
                for q in last['quotes']]
        summary['last_record'] = {'data_kind': last['data_kind'], 'published': last['source_published_at']}
        summary['last_vs_final'] = comp
    return finish(out, summary)


def finish(out: Path, summary: dict):
    summary['finished_at'] = datetime.now(JST).isoformat()
    (out / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
