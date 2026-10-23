"""horse_results.pickle（約8GiB）を Parquet の中間形式に変換し、件数・重複・ピークメモリを記録する。

目的:
- 再開計画 4.1：全件ロードを繰り返さないよう、一度だけ読み込んで列指向の中間形式を作る。
- 原本の pickle は変更しない。変換後は Parquet から必要列だけ読む運用にする。

使い方例:
  .venv/Scripts/python.exe tools/convert_horse_results_parquet.py \
    --src data/raw/horse_results.pickle \
    --out data/interim/horse_results.parquet \
    --report reproducibility/20261023_baseline/horse_results_conversion.json

安全策:
- 空き物理メモリが --min-free-gib を下回ったら、変換を中断して終了する（スワップでマシンが固まるのを防ぐ）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from modules.constants import HorseResultsCols  # noqa: E402

KNOWN_COLS = [v for k, v in vars(HorseResultsCols).items() if not k.startswith("_") and isinstance(v, str)]


class MemoryWatch:
    """別スレッドでプロセスの RSS ピークと空きメモリを監視する。"""

    def __init__(self, min_free_gib: float, interval: float = 0.5):
        self.proc = psutil.Process(os.getpid())
        self.min_free = min_free_gib * 1024**3
        self.interval = interval
        self.peak_rss = 0
        self.min_available = psutil.virtual_memory().available
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            self.peak_rss = max(self.peak_rss, self.proc.memory_info().rss)
            avail = psutil.virtual_memory().available
            self.min_available = min(self.min_available, avail)
            if avail < self.min_free:
                print(f"ABORT: available memory {avail / 1024**3:.1f} GiB < limit", flush=True)
                os._exit(3)
            time.sleep(self.interval)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join()


def gib(n: int) -> float:
    return round(n / 1024**3, 2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", default="data/raw/horse_results.pickle")
    parser.add_argument("--out", default="data/interim/horse_results.parquet")
    parser.add_argument("--report", required=True)
    parser.add_argument("--min-free-gib", type=float, default=1.5)
    args = parser.parse_args()

    src, out, report_path = (REPO_ROOT / p for p in (args.src, args.out, args.report))
    out.parent.mkdir(parents=True, exist_ok=True)
    report: dict = {
        "src": args.src,
        "src_bytes": src.stat().st_size,
        "available_gib_before": gib(psutil.virtual_memory().available),
    }

    with MemoryWatch(args.min_free_gib) as mw:
        t0 = time.time()
        df = pd.read_pickle(src)
        report["load_seconds"] = round(time.time() - t0, 1)
        report["peak_rss_gib_after_load"] = gib(mw.peak_rss)

        report["rows"] = len(df)
        report["index_name"] = df.index.name
        report["horses"] = int(df.index.nunique())
        report["columns"] = list(map(str, df.columns))
        report["unknown_columns"] = [c for c in df.columns if c not in KNOWN_COLS]
        report["missing_known_columns"] = [c for c in KNOWN_COLS if c not in df.columns]
        if HorseResultsCols.DATE in df.columns:
            d = pd.to_datetime(df[HorseResultsCols.DATE], errors="coerce")
            report["date_min"], report["date_max"] = str(d.min().date()), str(d.max().date())
            report["date_unparsed"] = int(d.isna().sum())

        # 重複の監査：完全一致の行と、「馬×日付×レース名」キーの重複
        full_dup = df.reset_index().duplicated()
        report["duplicate_rows_exact"] = int(full_dup.sum())
        key = [c for c in (HorseResultsCols.DATE, HorseResultsCols.RACE_NAME) if c in df.columns]
        if key:
            report["duplicate_rows_horse_date_race"] = int(df.reset_index().duplicated(subset=[df.index.name or "index", *key]).sum())

        # 型が混在する object 列は文字列にそろえて保存する（元の値は変えず表現だけ統一）
        table_df = df.reset_index()
        for c in table_df.columns:
            if table_df[c].dtype == object:
                table_df[c] = table_df[c].astype("string")
        del df
        t1 = time.time()
        table = pa.Table.from_pandas(table_df, preserve_index=False)
        del table_df
        pq.write_table(table, out, compression="zstd", row_group_size=1_000_000)
        report["write_seconds"] = round(time.time() - t1, 1)
        report["peak_rss_gib"] = gib(mw.peak_rss)
        report["min_available_gib"] = gib(mw.min_available)

    report["out"] = args.out
    report["out_bytes"] = out.stat().st_size
    h = hashlib.sha256()
    with open(out, "rb") as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    report["out_sha256"] = h.hexdigest()
    report["parquet_rows"] = pq.ParquetFile(out).metadata.num_rows
    report["schema"] = {f.name: str(f.type) for f in pq.read_schema(out)}

    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k not in ("columns", "schema")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
