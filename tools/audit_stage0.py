"""Stage-0 data audit (recovery plan 3 and 7): raw results / race_info / return_tables / feature caches.

Read-only: raw pickles and feature caches are never modified.

Usage:
  .venv/Scripts/python.exe tools/audit_stage0.py --out reproducibility/20261024_stage0_audit

Outputs (--out):
  summary.json            counts for every check below
  race_inclusion.csv      one row per race: date, race_type, include_train, include_eval, reasons
  runner_status.csv       non-numeric finishing codes and how they are treated
  missing_by_year.csv     missing rate of key result columns per year after coalescing old/new names
  feature_cache_columns.csv  per cache: n columns, coverage of P11 lists, missing P11 columns
  feature_missing_by_year.csv missing rate of 単勝/枠番/体重/体重変化 in the latest feature cache

Inclusion rules (decided in prompts/reports/20261024_stage0_audit.md):
  Data validity (exclude from training and evaluation):
    no_results / no_race_info / no_return_tables / bad_date / date_year_mismatch / no_winner
  Scope (excluded from the first comparisons, kept in raw):
    jump_race                     障害 races
  Evaluation-only exclusions (kept for training, excluded from betting evaluation):
    withdrawal                    a runner was 取消/除外 (sale-time odds may not reflect it)
    odds_1_0                      a runner had final win odds 1.0 (JRA Plus 10 payouts)
  Runner level:
    取消/取/除外/除  -> runner removed (did not start)
    中止/中/失       -> started, not finished: kept as a non-placing runner
    N(降)           -> finishing position N after demotion
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

# Old/new column names that coexist in data/raw/results.pickle (new layout: 2015-2019 and part of 2026)
COALESCE = {"単勝": "単勝オッズ", "枠番": "枠", "馬体重": "馬体重(増減)", "調教師": "厩舎"}
WITHDRAWN = {"取消", "取", "除外", "除"}
NOT_FINISHED = {"中止", "中", "失格", "失"}
P11_LISTS = ["selected_positive_perm", "selected_top50", "selected_top100", "selected_top200"]
EVAL_PERIOD_END = "2026-10-21"


def runner_status(code) -> tuple[str, int | None]:
    s = str(code).strip()
    if s.isdigit():
        return "finished", int(s)
    m = re.fullmatch(r"(\d+)\(降\)", s)
    if m:
        return "demoted", int(m[1])
    if s in WITHDRAWN:
        return "withdrawn", None
    if s in NOT_FINISHED:
        return "not_finished", None
    return "unknown", None


def audit_results(path: Path) -> tuple[pd.DataFrame, dict, pd.DataFrame, pd.DataFrame]:
    raw = pd.read_pickle(path)
    raw.index.name = "race_id"
    df = raw.reset_index()
    df["race_id"] = df["race_id"].astype(str)
    info = {"rows": len(df), "races": int(df["race_id"].nunique())}

    exact = df.duplicated()
    info["exact_duplicate_rows"] = int(exact.sum())
    df = df[~exact].copy()
    key_dup = df.duplicated(["race_id", "馬番"], keep=False)
    info["rows_after_exact_dedup"] = len(df)
    info["conflicting_key_duplicate_rows"] = int(key_dup.sum())
    info["conflicting_key_duplicate_races"] = int(df.loc[key_dup, "race_id"].nunique())

    for old, new in COALESCE.items():
        info[f"rows_only_{new}"] = int((df[old].isna() & df[new].notna()).sum())
        df[old] = df[old].where(df[old].notna(), df[new])
    df["year"] = df["race_id"].str[:4]
    missing = df.groupby("year")[["単勝", "人気", "枠番", "馬体重", "タイム", "horse_id", "jockey_id"]] \
        .apply(lambda x: (x.isna().mean() * 100).round(2))

    st = df["着順"].map(runner_status)
    df["status"] = st.str[0]
    df["finish_pos"] = st.str[1]
    codes = (df[df["status"] != "finished"].groupby(["status", "着順"]).size()
             .rename("rows").reset_index())
    codes["treatment"] = codes["status"].map({
        "withdrawn": "remove runner (did not start)",
        "not_finished": "keep, non-placing",
        "demoted": "use position after demotion",
        "unknown": "review"})
    info["runner_status_counts"] = df["status"].value_counts().to_dict()
    df["単勝_num"] = pd.to_numeric(df["単勝"], errors="coerce")
    return df, info, codes, missing


def audit_race_info(path: Path) -> tuple[pd.DataFrame, dict]:
    ri = pd.read_pickle(path)
    ri.index.name = "race_id"
    ri = ri.reset_index()
    ri["race_id"] = ri["race_id"].astype(str)
    info = {"rows": len(ri), "duplicate_race_ids": int(ri["race_id"].duplicated().sum())}
    ri = ri.drop_duplicates("race_id")
    ri["date_parsed"] = pd.to_datetime(ri["date"], format="%Y年%m月%d日", errors="coerce")
    info["unparsed_dates"] = int(ri["date_parsed"].isna().sum())
    info["date_min"] = str(ri["date_parsed"].min().date())
    info["date_max"] = str(ri["date_parsed"].max().date())
    ri["date_year_mismatch"] = ri["date_parsed"].dt.year.astype("Int64").astype(str) != ri["race_id"].str[:4]
    info["date_year_mismatch"] = int(ri["date_year_mismatch"].sum())
    info["race_type_counts"] = ri["race_type"].value_counts(dropna=False).to_dict()
    for c in ("course_len", "race_type", "ground_state", "race_class"):
        info[f"missing_{c}"] = int(ri[c].isna().sum())
    return ri, info


def audit_returns(path: Path) -> tuple[set, set, dict]:
    rt = pd.read_pickle(path)
    rt.index.name = "race_id"
    rt = rt.reset_index()
    rt["race_id"] = rt["race_id"].astype(str)
    info = {"rows": len(rt), "exact_duplicate_rows": int(rt.duplicated().sum())}
    rt = rt.drop_duplicates()
    kinds = rt.groupby("race_id")[0].apply(set)
    info["races"] = len(kinds)
    info["races_without_tansho"] = int((~kinds.map(lambda s: "単勝" in s)).sum())
    info["races_without_fukusho"] = int((~kinds.map(lambda s: "複勝" in s)).sum())
    info["bet_type_rows"] = rt[0].value_counts().to_dict()
    return set(kinds.index), set(kinds[kinds.map(lambda s: "単勝" in s)].index), info


def audit_features(out: Path) -> dict:
    lists = {}
    for n in P11_LISTS:
        p = ROOT / f"models/20260210/p11/{n}.json"
        if p.exists():
            lists[n] = json.loads(p.read_text(encoding="utf-8"))
    rows, latest, latest_cols = [], None, None
    for p in sorted(glob.glob(str(ROOT / "data/tmp/featured_data_*.pickle"))):
        f = pd.read_pickle(p)
        cols = list(f.columns)
        row = {"cache": os.path.basename(p), "rows": len(f), "columns": len(cols),
               "date_min": str(pd.to_datetime(f["date"]).min().date()),
               "date_max": str(pd.to_datetime(f["date"]).max().date())}
        for n, lst in lists.items():
            row[n] = f"{len(set(lst) & set(cols))}/{len(lst)}"
        if "selected_positive_perm" in lists:
            row["missing_positive_perm"] = " ".join(c for c in lists["selected_positive_perm"] if c not in set(cols))
        rows.append(row)
        if os.path.basename(p) == "featured_data_20260219.pickle":
            f["year"] = pd.to_datetime(f["date"]).dt.year
            keys = [c for c in ("単勝", "枠番", "体重", "体重変化") if c in f.columns]
            latest = f.groupby("year")[keys].apply(lambda x: (x.isna().mean() * 100).round(1))
            latest_cols = cols
        del f
    pd.DataFrame(rows).to_csv(out / "feature_cache_columns.csv", index=False, encoding="utf-8")
    if latest is not None:
        latest.to_csv(out / "feature_missing_by_year.csv", encoding="utf-8")
    odds_like = [c for c in (latest_cols or []) if any(k in c for k in ("単勝", "人気", "odds"))]
    return {"caches": len(rows), "odds_like_columns_in_latest_cache": odds_like,
            "odds_like_in_p11_positive_perm": [c for c in lists.get("selected_positive_perm", [])
                                               if any(k in c for k in ("単勝", "人気", "odds"))]}


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--skip-features", action="store_true")
    args = ap.parse_args()
    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)

    res, res_info, codes, missing = audit_results(ROOT / "data/raw/results.pickle")
    ri, ri_info = audit_race_info(ROOT / "data/raw/race_info.pickle")
    rt_ids, rt_tansho_ids, rt_info = audit_returns(ROOT / "data/raw/return_tables.pickle")
    codes.to_csv(out / "runner_status.csv", index=False, encoding="utf-8")
    missing.to_csv(out / "missing_by_year.csv", encoding="utf-8")

    g = res.groupby("race_id")
    races = pd.DataFrame({
        "runners_listed": g.size(),
        "withdrawn": g["status"].apply(lambda s: int((s == "withdrawn").sum())),
        "winners": g["finish_pos"].apply(lambda s: int((s == 1).sum())),
        "min_win_odds": g["単勝_num"].min(),
    })
    all_ids = sorted(set(races.index) | set(ri["race_id"]) | rt_ids)
    races = races.reindex(all_ids)
    ri_idx = ri.set_index("race_id")
    races["date"] = ri_idx["date_parsed"].reindex(all_ids).dt.date.astype(str)
    races["race_type"] = ri_idx["race_type"].reindex(all_ids)
    races["race_class"] = ri_idx["race_class"].reindex(all_ids)

    def reasons(r):
        data, scope, ev = [], [], []
        if pd.isna(r["runners_listed"]):
            data.append("no_results")
        if r.name not in ri_idx.index:
            data.append("no_race_info")
        elif r["date"] in ("NaT", "None") or bool(ri_idx.loc[r.name, "date_year_mismatch"]):
            data.append("bad_date")
        if r.name not in rt_ids:
            data.append("no_return_tables")
        elif r.name not in rt_tansho_ids:
            data.append("no_tansho_payout")
        if not pd.isna(r["runners_listed"]) and r["winners"] == 0:
            data.append("no_winner")
        if r["race_type"] == "障害":
            scope.append("jump_race")
        if not pd.isna(r["withdrawn"]) and r["withdrawn"] > 0:
            ev.append("withdrawal")
        if not pd.isna(r["min_win_odds"]) and r["min_win_odds"] <= 1.0:
            ev.append("odds_1_0")
        return pd.Series({"data_issues": " ".join(data), "scope": " ".join(scope), "eval_only": " ".join(ev)})

    races = races.join(races.apply(reasons, axis=1))
    races["include_train"] = (races["data_issues"] == "") & (races["scope"] == "")
    races["include_eval"] = races["include_train"] & (races["eval_only"] == "")
    races["dead_heat_win"] = races["winners"] > 1
    races.index.name = "race_id"
    races.to_csv(out / "race_inclusion.csv", encoding="utf-8")

    def count_reason(col, tag):
        return int(races[col].str.split().map(lambda xs: tag in xs).sum())

    summary = {
        "results": res_info, "race_info": ri_info, "return_tables": rt_info,
        "races_total": len(races),
        "races_include_train": int(races["include_train"].sum()),
        "races_include_eval": int(races["include_eval"].sum()),
        "reasons": {t: count_reason(c, t) for c, ts in {
            "data_issues": ["no_results", "no_race_info", "bad_date", "no_return_tables", "no_tansho_payout", "no_winner"],
            "scope": ["jump_race"], "eval_only": ["withdrawal", "odds_1_0"]}.items() for t in ts},
        "dead_heat_win_races": int(races["dead_heat_win"].sum()),
        "races_after_eval_period_end": int((races["date"] > EVAL_PERIOD_END).sum()),
        "include_train_by_year": races[races["include_train"]].index.str[:4].value_counts().sort_index().to_dict(),
    }
    if not args.skip_features:
        summary["features"] = audit_features(out)
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k not in ("race_info",)}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
