"""保存済みの馬ページHTML（data/html/horse）から馬戦績を作り直し、4.2差分と合成した中間ファイルを作る。

data/raw/horse_results.pickle（約8.1GiB）は全件読み込みに空きメモリ18GiBでも足りないため、
その元になった HTML を並列で読み直す。原本の pickle・HTML は変更しない。

- 4.2差分にある馬（2026年に出走した馬）は差分を使う（列ずれ修正後）。HTML は読まない
- それ以外の馬は HTML を get_rawdata_horse_results と同じ方法で読む
- 全列一致の重複を除き、--cutoff より後の行を除く
- 出力は tools/prepare_horse_results.py と同じ形式（旧 raw と同じ列名・列順、index=horse_id）

使い方:
  .venv/Scripts/python.exe tools/rebuild_horse_results_from_html.py \
    --out data/interim/horse_results_merged.pickle \
    --report reproducibility/20261024_stage1/horse_results_prepare.json --workers 20
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.preparing._horse_history_columns import LEGACY_COLUMNS, realign_public_horse_history  # noqa: E402


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    return h.hexdigest()


def parse_chunk(paths: list[str]) -> tuple[pd.DataFrame, dict]:
    from modules.preparing import get_rawdata_horse_results
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
        df = get_rawdata_horse_results(paths)
    log = buf.getvalue()
    stats = {'files': len(paths),
             'skipped_messages': sum(1 for line in log.splitlines() if line.startswith('horse_results ')),
             'columns': sorted(map(str, df.columns))}
    if df.empty:
        return df, stats
    if any('Unnamed' in str(c) for c in df.columns):
        df = realign_public_horse_history(df)
        stats['realigned'] = True
    for c in LEGACY_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df.loc[:, LEGACY_COLUMNS].astype(object)
    df.index = df.index.astype(str)
    df = df[~df.reset_index().astype(str).duplicated().to_numpy()]
    stats['horses'] = int(df.index.nunique())
    stats['rows'] = len(df)
    return df, stats


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--html-dir', default='data/html/horse')
    ap.add_argument('--delta', default='data/updates/20261023_results/delta/horse_results.pickle')
    ap.add_argument('--out', default='data/interim/horse_results_merged.pickle')
    ap.add_argument('--report', required=True)
    ap.add_argument('--cutoff', default='2026-10-21')
    ap.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 2) - 4))
    ap.add_argument('--chunk', type=int, default=200)
    args = ap.parse_args()
    out_path, report_path = ROOT / args.out, ROOT / args.report
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    delta = pd.read_pickle(ROOT / args.delta)
    delta.index = delta.index.astype(str)
    delta = realign_public_horse_history(delta).astype(object)
    delta = delta[~delta.reset_index().astype(str).duplicated().to_numpy()]
    delta_horses = set(delta.index)

    paths = []
    with os.scandir(ROOT / args.html_dir) as it:
        for e in it:
            if e.name.endswith('.bin') and e.name[:-4] not in delta_horses:
                paths.append(e.path)
    paths.sort()
    rep = {'html_dir': args.html_dir, 'html_files_to_parse': len(paths), 'delta': args.delta,
           'delta_rows': len(delta), 'delta_horses': len(delta_horses), 'workers': args.workers,
           'cutoff': args.cutoff, 'source': 'html (raw pickle not loaded: needs >18GiB free memory)'}
    chunks = [paths[i:i + args.chunk] for i in range(0, len(paths), args.chunk)]
    parts, skipped, column_sets, realigned = [], 0, {}, 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(parse_chunk, c) for c in chunks]
        for i, fut in enumerate(as_completed(futures), 1):
            df, st = fut.result()
            skipped += st['skipped_messages']
            key = ' '.join(st['columns'])
            column_sets[key] = column_sets.get(key, 0) + st['files']
            realigned += int(st.get('realigned', False))
            if not df.empty:
                parts.append(df)
            if i % 25 == 0 or i == len(chunks):
                print(f'[{time.strftime("%H:%M:%S")}] chunks {i}/{len(chunks)}', flush=True)

    html = pd.concat(parts)
    del parts
    rep['html_rows'] = len(html)
    rep['html_horses'] = int(html.index.nunique())
    rep['html_files_skipped_messages'] = skipped
    rep['html_chunks_realigned'] = realigned
    rep['html_column_variants'] = {k[:200]: v for k, v in column_sets.items()}

    merged = pd.concat([html, delta.loc[:, LEGACY_COLUMNS]])
    dup = merged.reset_index().astype(str).duplicated().to_numpy()
    rep['duplicate_rows_removed'] = int(dup.sum())
    merged = merged[~dup]
    dates = pd.to_datetime(merged['日付'], errors='coerce')
    rep['rows_unparsed_date'] = int(dates.isna().sum())
    after = (dates > pd.Timestamp(args.cutoff)).to_numpy()
    rep['rows_after_cutoff_removed'] = int(after.sum())
    merged = merged[~after]
    merged.index.name = None
    rep['rows'] = len(merged)
    rep['horses'] = int(merged.index.nunique())
    rep['date_min'] = str(dates[~after].min().date())
    rep['date_max'] = str(dates[~after].max().date())

    # 学習対象レースの出走馬のうち、戦績がある馬の割合
    results = pd.read_pickle(ROOT / 'data/raw/results.pickle')
    hid = set(results['horse_id'].dropna().astype(str))
    rep['results_horses'] = len(hid)
    rep['results_horses_with_history'] = len(hid & set(merged.index))
    del results

    merged.to_pickle(out_path)
    rep['out'] = args.out
    rep['out_bytes'] = out_path.stat().st_size
    rep['out_sha256'] = sha256(out_path)
    rep['seconds'] = round(time.time() - t0, 1)
    report_path.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in rep.items() if k != 'html_column_variants'}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
