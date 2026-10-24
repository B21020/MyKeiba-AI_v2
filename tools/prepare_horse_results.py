"""馬戦績の原本（約8GiB）と4.2の更新差分を合成し、重複を除いた中間ファイルを作る。

再開計画 段階1：
- 原本 data/raw/horse_results.pickle は変更しない。読むのは一度だけ（空きメモリの安全装置つき）。
- 全列が一致する重複行を除く（直近Nレースの集計が同じレースの複製で埋まるのを防ぐ）。
- 4.2 の差分（公開版の戦績表）は列ずれを直してから、対象馬の行を差分で置き換える。
- 予測に使えない日付（--cutoff より後）の行を除く。

使い方:
  .venv/Scripts/python.exe tools/prepare_horse_results.py \
    --out data/interim/horse_results_merged.pickle \
    --report reproducibility/20261024_stage1/horse_results_prepare.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import pandas as pd
import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.preparing._horse_history_columns import LEGACY_COLUMNS, realign_public_horse_history  # noqa: E402
from tools.convert_horse_results_parquet import MemoryWatch, gib  # noqa: E402


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    return h.hexdigest()


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--raw', default='data/raw/horse_results.pickle')
    ap.add_argument('--delta', default='data/updates/20261023_results/delta/horse_results.pickle')
    ap.add_argument('--out', default='data/interim/horse_results_merged.pickle')
    ap.add_argument('--report', required=True)
    ap.add_argument('--cutoff', default='2026-10-21')
    ap.add_argument('--min-free-gib', type=float, default=1.5)
    args = ap.parse_args()
    raw_path, delta_path, out_path, report_path = (ROOT / p for p in (args.raw, args.delta, args.out, args.report))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    rep = {'raw': args.raw, 'raw_bytes': raw_path.stat().st_size, 'delta': args.delta, 'cutoff': args.cutoff,
           'available_gib_before': gib(psutil.virtual_memory().available)}

    with MemoryWatch(args.min_free_gib) as mw:
        t0 = time.time()
        raw = pd.read_pickle(raw_path)
        rep['load_seconds'] = round(time.time() - t0, 1)
        rep['peak_rss_gib_after_load'] = gib(mw.peak_rss)
        raw.index = raw.index.astype(str)
        rep['raw_rows'] = len(raw)
        rep['raw_horses'] = int(raw.index.nunique())
        rep['raw_columns'] = list(map(str, raw.columns))
        # 原本にも公開版の列ずれ（Unnamed 列）が混ざっていれば直す
        from modules.preparing._horse_history_columns import shifted_rows
        rep['raw_shifted_rows'] = int(shifted_rows(raw).sum()) if 'Unnamed:31' in raw.columns else 0
        if rep['raw_shifted_rows']:
            raw = realign_public_horse_history(raw)
        keep = [c for c in LEGACY_COLUMNS if c in raw.columns]
        rep['raw_columns_dropped'] = [c for c in raw.columns if c not in keep]
        raw = raw.loc[:, keep]
        dup = raw.reset_index().duplicated().to_numpy()
        rep['raw_exact_duplicate_rows'] = int(dup.sum())
        raw = raw[~dup]
        rep['raw_rows_after_dedup'] = len(raw)

        delta = pd.read_pickle(delta_path)
        delta.index = delta.index.astype(str)
        rep['delta_rows'] = len(delta)
        rep['delta_horses'] = int(delta.index.nunique())
        delta = realign_public_horse_history(delta)
        delta = delta[~delta.reset_index().duplicated().to_numpy()]

        replaced = raw.index.isin(delta.index.unique())
        rep['raw_rows_replaced_by_delta'] = int(replaced.sum())
        merged = pd.concat([raw.loc[~replaced], delta.loc[:, keep]])
        del raw, delta
        dates = pd.to_datetime(merged['日付'], errors='coerce')
        rep['rows_unparsed_date'] = int(dates.isna().sum())
        after = dates > pd.Timestamp(args.cutoff)
        rep['rows_after_cutoff_removed'] = int(after.sum())
        merged = merged[~after.to_numpy()]
        merged.index.name = None
        rep['rows'] = len(merged)
        rep['horses'] = int(merged.index.nunique())
        rep['date_min'] = str(dates[~after].min().date())
        rep['date_max'] = str(dates[~after].max().date())
        merged.to_pickle(out_path)
        rep['peak_rss_gib'] = gib(mw.peak_rss)
        rep['min_available_gib'] = gib(mw.min_available)

    rep['out'] = args.out
    rep['out_bytes'] = out_path.stat().st_size
    rep['out_sha256'] = sha256(out_path)
    report_path.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in rep.items() if k != 'raw_columns'}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
