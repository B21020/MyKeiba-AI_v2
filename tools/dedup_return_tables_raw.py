"""Deduplicate exact duplicate rows in data/raw/return_tables.pickle.

This script removes *exact* duplicate rows including the race_id (index) and all columns.
It is safe for cases where the same payout table was appended multiple times.

Usage (PowerShell):
  C:/.../.venv/Scripts/python.exe tools/dedup_return_tables_raw.py

Options:
  --input  Path to input pickle (default: data/raw/return_tables.pickle)
  --inplace Overwrite input file (default: true)
  --dry-run Only print stats
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil

import pandas as pd


def _drop_exact_duplicates_including_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    tmp = df.reset_index()
    index_cols = list(tmp.columns[: df.index.nlevels])
    subset = index_cols + list(df.columns)
    tmp = tmp.drop_duplicates(subset=subset, keep="first")
    if len(index_cols) == 1:
        out = tmp.set_index(index_cols[0])
        out.index.name = df.index.name
    else:
        out = tmp.set_index(index_cols)
        out.index.names = df.index.names
    return out


def _summarize(df: pd.DataFrame, label: str) -> None:
    tmp = df.reset_index().rename(columns={"index": "race_id"})
    subset = ["race_id", *list(df.columns)]
    dups = int(tmp.duplicated(subset=subset, keep="first").sum())
    print(label)
    print("  rows:", len(tmp))
    print("  races:", int(df.index.nunique()))
    print("  exact_dup_rows:", dups)
    print("  unique_rows:", int(len(tmp) - dups))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=os.path.join("data", "raw", "return_tables.pickle"))
    ap.add_argument("--inplace", action="store_true", default=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    in_path = args.input
    if not os.path.isfile(in_path):
        raise FileNotFoundError(in_path)

    print("loading:", in_path)
    df = pd.read_pickle(in_path)
    _summarize(df, "[before]")

    if args.dry_run:
        print("dry-run: no changes")
        return 0

    # backup (do not overwrite existing .bak)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = in_path + f".bak_{stamp}"
    print("backup ->", backup_path)
    shutil.copy2(in_path, backup_path)

    print("deduplicating exact duplicate rows...")
    out = _drop_exact_duplicates_including_index(df)
    _summarize(out, "[after]")

    if args.inplace:
        print("writing (inplace):", in_path)
        out.to_pickle(in_path)
    else:
        out_path = in_path + ".dedup"
        print("writing:", out_path)
        out.to_pickle(out_path)

    # quick spot check
    for rid in ["202206050805", "202205050505", "201509050812", "202301010101"]:
        if rid not in out.index:
            continue
        sub = out.loc[rid]
        if isinstance(sub, pd.Series):
            sub = sub.to_frame().T
        total = len(sub)
        uniq = len(sub.drop_duplicates())
        print("spotcheck", rid, "total", total, "unique", uniq, "dup", total - uniq)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
