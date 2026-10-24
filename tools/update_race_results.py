"""Resume the recovery plan's section 4.2 without loading the 8 GiB horse pickle.

Public pages are fetched sequentially with at least 1.2 seconds between requests.
Raw originals and their existing .bak files are preserved. Horse histories are
published separately; their dates, rather than today's profile aggregates, are
the only inputs suitable for rebuilding historical features.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
import unicodedata

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from modules.preparing._horse_history_columns import realign_public_horse_history  # noqa: E402
from modules.preparing._get_rawdata import (  # noqa: E402
    get_rawdata_info, get_rawdata_results, get_rawdata_return,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


def write_pickle(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pd.to_pickle(value, tmp)
    os.replace(tmp, path)


def append_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as out:
        out.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")


class AccessStopped(RuntimeError):
    """Stop rather than retry an access restriction or rate limit."""


class Fetcher:
    def __init__(self, work: Path, interval: float):
        if interval < 1.0:
            raise ValueError("Request interval must be at least one second")
        self.work, self.interval, self.last_end = work, interval, 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.5",
        })

    def get(self, url: str, path: Path, *, referer: str = "") -> bytes:
        if path.exists():
            return path.read_bytes()
        for attempt in range(2):
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_end)))
            try:
                response = self.session.get(
                    url, headers={"Referer": referer} if referer else {}, timeout=30,
                )
                self.last_end = time.monotonic()
                append_json(self.work / "requests.jsonl", {
                    "at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
                    "url": url, "status": response.status_code,
                    "bytes": len(response.content), "cache": str(path.relative_to(self.work)),
                    "sha256": hashlib.sha256(response.content).hexdigest(),
                })
                if response.status_code in (401, 403, 429):
                    raise AccessStopped(f"HTTP {response.status_code}: {url}")
                response.raise_for_status()
                head = response.content[:20000].lower()
                if b"<title>access denied" in head or b"<title>just a moment" in head:
                    raise AccessStopped(f"Access restriction page: {url}")
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_name(path.name + ".tmp")
                tmp.write_bytes(response.content)
                os.replace(tmp, path)
                return response.content
            except AccessStopped:
                raise
            except requests.RequestException:
                self.last_end = time.monotonic()
                if attempt:
                    raise
                time.sleep(4)
        raise AssertionError("unreachable")


def soup_for(data: bytes):
    return BeautifulSoup(data, "lxml")


def calendar_dates(data: bytes) -> list[str]:
    table = soup_for(data).select_one("table.Calendar_Table")
    if table is None:
        raise ValueError("Calendar_Table missing; calendar not accepted")
    return sorted({
        match for a in table.select("a[href]")
        for match in re.findall(r"kaisai_date=(\d{8})", a["href"])
    })


def listed_races(data: bytes, day: str) -> dict[str, str]:
    rows = {}
    for a in soup_for(data).select("a[href]"):
        for rid in re.findall(r"race_id=(\d{12})", a["href"]):
            if rid.startswith(day[:4]) and 1 <= int(rid[4:6]) <= 10:
                rows[rid] = day
    if not rows:
        raise ValueError(f"No JRA race IDs in dated race list: {day}")
    return rows


def apply_date_overrides(manifest: dict, work: Path) -> dict:
    """Keep documented actual dates when a postponed meeting lists earlier races."""
    path = work / "schedule_date_overrides.json"
    if path.exists():
        for rid, evidence in json.loads(path.read_text(encoding="utf-8")).items():
            if rid in manifest:
                manifest[rid] = evidence["actual_date"]
    return manifest


def discover(args, fetch: Fetcher, work: Path):
    manifest_path = work / "expected_races.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    end = pd.Timestamp(args.end)
    years = [end.year] + list(range(args.audit_start_year, end.year))
    for year in years:
        for month in range(1, (end.month if year == end.year else 12) + 1):
            calendar_path = work / "calendar" / f"{year}{month:02d}.html"
            data = fetch.get(
                f"https://race.netkeiba.com/top/calendar.html?year={year}&month={month}",
                calendar_path,
            )
            for day in calendar_dates(data):
                if day[:6] != f"{year}{month:02d}" or pd.Timestamp(day) > end:
                    continue
                data = fetch.get(
                    f"https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={day}",
                    work / "race_lists" / f"{day}.html",
                    referer=f"https://race.netkeiba.com/top/race_list.html?kaisai_date={day}",
                )
                manifest.update(listed_races(data, day))
                apply_date_overrides(manifest, work)
                write_json(manifest_path, manifest)
            progress(work, "discover", month=f"{year}-{month:02d}", expected_races=len(manifest))
    write_json(work / "discovery_complete.json", {
        "audit_start_year": args.audit_start_year, "end": args.end,
        "expected_races": len(manifest),
    })
    return manifest


def progress(work: Path, stage: str, **values):
    state = {"stage": stage, "at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(), **values}
    write_json(work / "progress.json", state)
    print(json.dumps(state, ensure_ascii=False), flush=True)


def inventory():
    info = pd.read_pickle(ROOT / "data/raw/race_info.pickle")
    dates = pd.to_datetime(info["date"], format="%Y年%m月%d日", errors="coerce")
    date_map = {str(rid): d.strftime("%Y%m%d") for rid, d in dates.items() if pd.notna(d)}
    # Read one raw table at a time; never load the giant horse pickle.
    results = pd.read_pickle(ROOT / "data/raw/results.pickle")
    result_ids = set(results.index.astype(str))
    del results
    returns = pd.read_pickle(ROOT / "data/raw/return_tables.pickle")
    return date_map, result_ids, set(returns.index.astype(str))


def normalize_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for canonical, source in {
        "枠番": "枠", "単勝": "単勝オッズ", "馬体重": "馬体重(増減)", "調教師": "厩舎",
    }.items():
        if source in df:
            df[canonical] = df[canonical].combine_first(df[source]) if canonical in df else df[source]
    return df


def parse_race(path: Path, rid: str, day: str):
    data = path.read_bytes()
    soup = soup_for(data)
    canonical = soup.select_one('meta[property="og:url"]')
    if canonical is None or rid not in canonical.get("content", ""):
        raise ValueError("Result page identity does not match requested race")
    result_table = soup.select_one("#All_Result_Table") or soup.find("table", summary="レース結果")
    if result_table is None or not soup.select("table.Payout_Detail_Table"):
        title = unicodedata.normalize("NFKC", soup.title.get_text() if soup.title else "")
        date_match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
        title_day = "".join(f"{int(v):0{width}d}" for v, width in zip(date_match.groups(), (4, 2, 2))) if date_match else None
        # Only a cancellation in the identified race's title is authoritative;
        # a sidebar mentioning another cancelled race is not sufficient.
        if title_day == day and re.search(r"取り止め|取りやめ|取止|中止", title):
            return {"race_id": rid, "date": day, "status": "cancelled", "source_title": title}
        raise ValueError("Result or payout table missing; not classified as cancellation")
    # Reuse existing parsers but require a complete result from all three.
    output = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
        results = normalize_results(get_rawdata_results([str(path.resolve())]))
        info = get_rawdata_info([str(path.resolve())])
        returns = get_rawdata_return([str(path.resolve())])
    if set(results.index) != {rid} or set(info.index) != {rid} or set(returns.index) != {rid}:
        raise ValueError("Inconsistent race keys from parsers")
    parsed_day = pd.to_datetime(info.iloc[0]["date"], format="%Y年%m月%d日").strftime("%Y%m%d")
    if parsed_day != day:
        raise ValueError(f"Listed date {day} differs from result date {parsed_day}")
    if results.empty or results["馬番"].duplicated().any():
        raise ValueError("Empty or duplicated race/horse result")
    rank = pd.to_numeric(results["着順"], errors="coerce")
    if not rank.eq(1).any():
        raise ValueError("No numeric winning finish found")
    for key in ("horse_id", "jockey_id", "trainer_id"):
        if results[key].isna().any():
            raise ValueError(f"Missing {key}")
    # A reconstruction, NOT a historical pre-race snapshot or timestamped odds.
    card_columns = [
        "枠番", "馬番", "馬名", "性齢", "斤量", "騎手", "調教師", "馬体重",
        "horse_id", "jockey_id", "trainer_id",
    ]
    card = results[[c for c in card_columns if c in results]].copy()
    return {
        "race_id": rid, "date": day, "status": "ok", "results": results,
        "race_info": info, "return_tables": returns, "entries": card,
        "entry_provenance": "reconstructed_from_result; not_a_pre_race_snapshot",
    }


def fetch_races(args, fetch: Fetcher, work: Path, manifest: dict):
    dates, existing, payouts = inventory()
    targets = {
        rid: day for rid, day in manifest.items()
        if rid not in existing or rid not in payouts or dates.get(rid) != day
    }
    # New dates first; historical repairs follow, without examining betting performance.
    ordered = sorted(targets, key=lambda rid: (targets[rid] < args.start.replace("-", ""), targets[rid], rid))
    write_json(work / "target_races.json", {rid: targets[rid] for rid in ordered})
    done = fetched = consecutive_errors = 0
    for rid in ordered:
        parsed = work / "parsed" / f"{rid}.pickle"
        if parsed.exists():
            done += 1
            continue
        if args.limit_races and fetched >= args.limit_races:
            break
        try:
            cached = ROOT / "data/html/race" / f"{rid}.bin"
            path = work / "race" / f"{rid}.bin"
            if not path.exists() and cached.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(cached, path)
            if path.exists():
                old_bytes = path.read_bytes()
                identity = soup_for(old_bytes).select_one('meta[property="og:url"]')
                if identity is None or rid not in identity.get("content", ""):
                    # Historical db.netkeiba HTML is retained as evidence, but
                    # refresh it from the result endpoint before strict parsing.
                    archive = work / "superseded_race_cache" / f"{rid}.{hashlib.sha256(old_bytes).hexdigest()[:12]}.bin"
                    if not path.resolve().is_relative_to(work.resolve()) or not archive.resolve().is_relative_to(work.resolve()):
                        raise ValueError("Cache archive would leave the update directory")
                    archive.parent.mkdir(parents=True, exist_ok=True)
                    if archive.exists():
                        if archive.read_bytes() != old_bytes:
                            raise ValueError("Archived cache hash collision")
                        path.unlink()
                    else:
                        path.replace(archive)
            fetch.get(
                f"https://race.netkeiba.com/race/result.html?race_id={rid}", path,
                referer=f"https://race.netkeiba.com/top/race_list.html?kaisai_date={targets[rid]}",
            )
            bundle = parse_race(path, rid, targets[rid])
            write_pickle(parsed, bundle)
            done += 1
            fetched += 1
            consecutive_errors = 0
        except AccessStopped:
            raise
        except Exception as error:
            append_json(work / "failures.jsonl", {"stage": "races", "id": rid, "error": repr(error)})
            print(f"ERROR race {rid}: {error}", flush=True)
            consecutive_errors += 1
            if consecutive_errors >= 3:
                raise RuntimeError("Three consecutive result failures; check source before resuming") from error
        if done % 10 == 0:
            progress(work, "races", completed=done, target=len(ordered), current=rid)
    progress(work, "races", completed=done, target=len(ordered), fetched_this_run=fetched)


def read_bundles(work: Path):
    return [pd.read_pickle(p) for p in sorted((work / "parsed").glob("*.pickle"))]


def merge_races(work: Path):
    bundles = [b for b in read_bundles(work) if b["status"] == "ok"]
    if not bundles:
        return
    # Publish versioned deltas before touching compatibility raw files.
    for name in ("results", "race_info", "return_tables", "entries"):
        new = pd.concat([b[name] for b in bundles])
        write_pickle(work / "delta" / f"{name}.pickle", new)
        if name == "entries":
            continue
        dest = ROOT / "data/raw" / f"{name}.pickle"
        backup = work / "original_raw" / dest.name
        backup.parent.mkdir(parents=True, exist_ok=True)
        if not backup.exists():
            shutil.copy2(dest, backup)
        old = pd.read_pickle(dest)
        merged = pd.concat([old.loc[~old.index.isin(new.index.unique())], new])
        write_pickle(dest, merged)
        progress(work, "merge", table=name, added_races=new.index.nunique(), rows=len(merged))
        del old, merged, new
    write_json(work / "delta/entries_provenance.json", {
        "source": "result pages", "historical_pre_race_snapshot": False,
        "timestamped_odds": False, "profile_aggregates_included": False,
    })


def parse_horse(data: bytes, horse_id: str, end: str):
    payload = json.loads(data)
    if payload.get("status") != "OK" or not isinstance(payload.get("data"), str):
        raise ValueError("Horse result fragment is not OK")
    table = soup_for(payload["data"].encode("utf-8")).select_one("table.db_h_race_results")
    if table is None:
        raise ValueError("Horse history table missing")
    df = pd.read_html(io.StringIO(str(table)))[0]
    df.columns = [unicodedata.normalize("NFKC", str(c)).replace(" ", "") for c in df.columns]
    if "日付" not in df or "着順" not in df:
        raise ValueError("Horse history schema changed")
    # 公開版は伏せ字の有料列のため「通過」以降が4列右にずれる。旧 raw と同じ列名・列順に戻す
    df = realign_public_horse_history(df)
    dates = pd.to_datetime(df["日付"], errors="coerce")
    if dates.isna().any():
        raise ValueError("Unparsed date in horse history")
    df = df.loc[dates <= pd.Timestamp(end)].copy()
    if df.empty:
        raise ValueError("No horse history on or before cutoff")
    df.index = pd.Index([horse_id] * len(df))
    # Keep the provider's date/meeting/R fields: they also identify non-JRA starts.
    df = df.reset_index().drop_duplicates().set_index("index")
    df.index.name = None
    return df


def fetch_horses(args, fetch: Fetcher, work: Path):
    bundles = [b for b in read_bundles(work) if b["status"] == "ok"]
    horse_ids = sorted({str(h) for b in bundles for h in b["results"]["horse_id"].dropna()})
    write_json(work / "target_horses.json", horse_ids)
    done = fetched = consecutive_errors = 0
    for horse_id in horse_ids:
        out = work / "horse_parsed" / f"{horse_id}.pickle"
        if out.exists():
            done += 1
            continue
        if args.limit_horses and fetched >= args.limit_horses:
            break
        try:
            data = fetch.get(
                f"https://db.netkeiba.com/horse/ajax_horse_results.html?id={horse_id}&input=UTF-8&output=json",
                work / "horse" / f"{horse_id}.json",
                referer=f"https://db.netkeiba.com/horse/{horse_id}/",
            )
            df = parse_horse(data, horse_id, args.end)
            write_pickle(out, df)
            done += 1
            fetched += 1
            consecutive_errors = 0
        except AccessStopped:
            raise
        except Exception as error:
            append_json(work / "failures.jsonl", {"stage": "horses", "id": horse_id, "error": repr(error)})
            print(f"ERROR horse {horse_id}: {error}", flush=True)
            consecutive_errors += 1
            if consecutive_errors >= 3:
                raise RuntimeError("Three consecutive horse failures; check source before resuming") from error
        if done % 25 == 0:
            progress(work, "horses", completed=done, target=len(horse_ids), current=horse_id)
    # Small dated overlay rather than unsafe all-history pickle loading/replacement.
    parts = [pd.read_pickle(p) for p in sorted((work / "horse_parsed").glob("*.pickle"))]
    if parts:
        new = pd.concat(parts)
        write_pickle(work / "delta/horse_results.pickle", new)
        write_json(work / "delta/horse_results_provenance.json", {
            "source": "public horse result fragments", "cutoff": args.end,
            "horses": new.index.nunique(), "rows": len(new),
            "complete_horse_count": done, "target_horse_count": len(horse_ids),
            "legacy_raw_replaced": False,
            "asof_rule": "Use starts strictly before the predicted race date; never today's profile aggregates.",
        })
    progress(work, "horses", completed=done, target=len(horse_ids), fetched_this_run=fetched)


def audit(args, work: Path, manifest: dict):
    dates, results, returns = inventory()
    cancelled = {b["race_id"] for b in read_bundles(work) if b["status"] == "cancelled"}
    rows = [{
        "race_id": rid, "date": day, "year": day[:4], "month": day[:6],
        "venue": rid[4:6], "race_number": rid[-2:],
        "has_results": rid in results, "has_payout": rid in returns,
        "date_matches": dates.get(rid) == day, "cancelled": rid in cancelled,
    } for rid, day in sorted(manifest.items())]
    frame = pd.DataFrame(rows)
    frame["complete"] = frame.has_results & frame.has_payout & frame.date_matches
    frame.to_csv(work / "race_audit.csv", index=False, encoding="utf-8-sig")
    frame.groupby(["year", "month", "venue"])[
        ["has_results", "has_payout", "date_matches", "cancelled", "complete"]
    ].agg(["count", "sum"]).to_csv(work / "coverage_by_month_venue.csv", encoding="utf-8-sig")
    unresolved = frame.loc[~frame.complete & ~frame.cancelled]
    unresolved.to_csv(work / "unresolved_races.csv", index=False, encoding="utf-8-sig")
    horse_target = work / "target_horses.json"
    target_count = len(json.loads(horse_target.read_text(encoding="utf-8"))) if horse_target.exists() else None
    horse_count = len(list((work / "horse_parsed").glob("*.pickle")))
    summary = {
        "start": args.start, "cutoff": args.end, "calendar_audit_start_year": args.audit_start_year,
        "listed_races": len(frame), "complete_races": int(frame.complete.sum()),
        "cancelled_races": len(cancelled), "unresolved_races": len(unresolved),
        "newly_parsed_races": len(list((work / "parsed").glob("*.pickle"))),
        "horse_histories_completed": horse_count, "horse_histories_target": target_count,
        "raw_horse_results_replaced": False,
        "latest_complete_date": str(frame.loc[frame.complete, "date"].max()),
        "historical_entry_snapshots_available": False,
        "betting_metrics_examined": False,
    }
    write_json(work / "summary.json", summary)
    progress(work, "audit", **summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["discover", "races", "merge", "horses", "audit", "all"], default="all")
    parser.add_argument("--start", default="2026-02-16")
    parser.add_argument("--end", default="2026-10-21")
    parser.add_argument("--audit-start-year", type=int, default=2015)
    parser.add_argument("--work-dir", default="data/updates/20261023_results")
    parser.add_argument("--interval", type=float, default=1.2)
    parser.add_argument("--limit-races", type=int, default=0)
    parser.add_argument("--limit-horses", type=int, default=0)
    args = parser.parse_args()
    if pd.Timestamp(args.end) < pd.Timestamp(args.start):
        parser.error("end precedes start")
    if not 1900 <= args.audit_start_year <= pd.Timestamp(args.end).year:
        parser.error("audit-start-year must be on or before end year")
    if args.limit_races < 0 or args.limit_horses < 0:
        parser.error("limits must be nonnegative")
    work = (ROOT / args.work_dir).resolve()
    if not work.is_relative_to(ROOT / "data"):
        parser.error("work-dir must stay within repository data directory")
    work.mkdir(parents=True, exist_ok=True)
    # Exclusive file prevents two collectors/mergers operating on the same raw set.
    lock = work / "run.lock"
    try:
        with lock.open("x", encoding="ascii") as out:
            out.write(str(os.getpid()))
    except FileExistsError:
        raise RuntimeError(f"Collector already locked: {lock}; check its PID before removing a stale lock")
    try:
        scope = {"start": args.start, "end": args.end}
        scope_path = work / "run_scope.json"
        if scope_path.exists() and json.loads(scope_path.read_text(encoding="utf-8")) != scope:
            raise ValueError("This work directory belongs to another date range; use a different work-dir")
        write_json(scope_path, scope)
        write_json(work / "last_run_config.json", vars(args))
        fetch = Fetcher(work, args.interval)
        manifest_path = work / "expected_races.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        apply_date_overrides(manifest, work)
        if manifest:
            write_json(manifest_path, manifest)
        if args.stage in ("discover", "all"):
            manifest = discover(args, fetch, work)
        if args.stage in ("races", "all"):
            if not manifest:
                raise ValueError("Run discovery first")
            fetch_races(args, fetch, work, manifest)
        if args.stage in ("merge", "all"):
            merge_races(work)
        if args.stage in ("horses", "all"):
            fetch_horses(args, fetch, work)
        if args.stage in ("audit", "all"):
            if not manifest:
                raise ValueError("Run discovery first")
            summary = audit(args, work, manifest)
            if args.stage == "all":
                limited = bool(args.limit_races or args.limit_horses)
                complete = not summary["unresolved_races"] and summary["horse_histories_target"] == summary["horse_histories_completed"]
                progress(work, "complete" if complete and not limited else "partial", **summary)
    except BaseException as error:
        progress(work, "stopped", error=repr(error))
        raise
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
