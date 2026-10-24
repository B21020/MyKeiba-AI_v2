r"""Measure race-day freshness of public netkeiba odds at fixed offsets before post time.

Plan 4.3 follow-up: for a few races, run capture_public_odds.capture() at T-30/10/5/2 minutes,
then report the lag between the provider's publication time and the actual observation time,
and how often the publication time advanced between captures.

Usage (PowerShell, repository root):
  # 1) write the schedule (no network)
  .\.venv\Scripts\python.exe tools\odds_freshness_probe.py plan --out data\odds\probe_20261024_25 `
      --race 202605040801@2026-10-24T10:05 --race 202608040911@2026-10-25T15:40
  # 2) show what would run on a day (no network)
  .\.venv\Scripts\python.exe tools\odds_freshness_probe.py run --out data\odds\probe_20261024_25 --date 2026-10-24 --dry-run
  # 3) run for one day (sequential, one capture at a time; stops after the last planned capture)
  .\.venv\Scripts\python.exe tools\odds_freshness_probe.py run --out data\odds\probe_20261024_25 --date 2026-10-24
  # 4) summarize
  .\.venv\Scripts\python.exe tools\odds_freshness_probe.py report --out data\odds\probe_20261024_25

Rules: captures are never retried; a capture whose planned time passed by more than --late-tolerance
seconds is logged as "missed" instead of being run late. Every attempt (ok/error/missed) is appended
to captures.jsonl with planned and actual times.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.odds_snapshots import JST, timestamp  # noqa: E402

DEFAULT_OFFSETS = (30, 10, 5, 2)
SCHEDULE_FIELDS = ["race_id", "scheduled_start_at", "offset_min", "planned_at"]


def build_schedule(races: list[str], offsets=DEFAULT_OFFSETS) -> list[dict]:
    rows = []
    for spec in races:
        race_id, _, start = spec.partition("@")
        if len(race_id) != 12 or not race_id.isdigit() or not start:
            raise ValueError(f"Expected RACEID@YYYY-MM-DDTHH:MM, got {spec!r}")
        start_at = datetime.fromisoformat(start).replace(tzinfo=JST)
        for off in offsets:
            rows.append({"race_id": race_id, "scheduled_start_at": start_at.isoformat(),
                         "offset_min": off, "planned_at": (start_at - timedelta(minutes=off)).isoformat()})
    return sorted(rows, key=lambda r: (r["planned_at"], r["race_id"]))


def read_schedule(out: Path, date: str | None = None) -> list[dict]:
    with open(out / "schedule.csv", encoding="utf-8") as f:
        rows = [dict(r, offset_min=int(r["offset_min"])) for r in csv.DictReader(f)]
    if date:
        rows = [r for r in rows if r["planned_at"].startswith(date)]
    return sorted(rows, key=lambda r: (r["planned_at"], r["race_id"]))


def append_log(out: Path, entry: dict) -> None:
    with open(out / "captures.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run(out: Path, date: str, *, dry_run: bool, late_tolerance: int, db: Path, capture=None, now=None, sleep=time.sleep):
    rows = read_schedule(out, date)
    now = now or (lambda: datetime.now(JST))
    if dry_run:
        for r in rows:
            print(f"{r['planned_at']}  T-{r['offset_min']:>2}  {r['race_id']}  (post {r['scheduled_start_at'][11:16]})")
        print(f"{len(rows)} captures planned on {date}; nothing executed (dry run)")
        return
    if capture is None:
        from tools.capture_public_odds import capture
    for r in rows:
        planned = timestamp(r["planned_at"])
        wait = (planned - now()).total_seconds()
        if wait > 0:
            sleep(wait)
        started = now()
        entry = {**r, "started_at": started.isoformat()}
        if (started - planned).total_seconds() > late_tolerance:
            entry["status"] = "missed"
        else:
            try:
                meta = capture(r["race_id"], db)
                entry.update(status="ok", observed_at=meta["observed_at"],
                             source_published_at=meta["source_published_at"],
                             page_scheduled_start_at=meta["scheduled_start_at"],
                             snapshot_id=meta["snapshot_id"], horses=meta["horses"])
            except Exception as e:  # recorded, never retried
                entry.update(status="error", error=f"{type(e).__name__}: {e}"[:300])
        append_log(out, entry)
        print(json.dumps({k: entry.get(k) for k in ("race_id", "offset_min", "status", "source_published_at", "observed_at", "error")},
                         ensure_ascii=False), flush=True)


def summarize(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    """Per-capture lag and per-race publication-time progression."""
    captures, races = [], {}
    for e in entries:
        row = {k: e.get(k) for k in ("race_id", "offset_min", "planned_at", "status", "observed_at", "source_published_at", "error")}
        if e.get("status") == "ok":
            obs, pub = timestamp(e["observed_at"]), timestamp(e["source_published_at"])
            start = timestamp(e.get("page_scheduled_start_at") or e["scheduled_start_at"])
            row["lag_seconds"] = round((obs - pub).total_seconds(), 1)
            row["published_min_before_post"] = round((start - pub).total_seconds() / 60, 2)
            row["post_time_changed"] = e.get("page_scheduled_start_at") not in (None, e["scheduled_start_at"])
            races.setdefault(e["race_id"], []).append((obs, pub))
        captures.append(row)
    race_rows = []
    for race_id, seq in sorted(races.items()):
        seq.sort()
        pubs = [p for _, p in seq]
        distinct = sorted(set(pubs))
        steps = [round((b - a).total_seconds() / 60, 2) for a, b in zip(distinct, distinct[1:])]
        race_rows.append({"race_id": race_id, "ok_captures": len(seq), "distinct_published": len(distinct),
                          "published_steps_min": steps,
                          "max_lag_seconds": max(round((o - p).total_seconds(), 1) for o, p in seq),
                          "repeat_without_update": sum(1 for a, b in zip(pubs, pubs[1:]) if a == b)})
    return captures, race_rows


def report(out: Path) -> None:
    path = out / "captures.jsonl"
    entries = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()] if path.exists() else []
    captures, races = summarize(entries)
    with open(out / "freshness_by_capture.csv", "w", newline="", encoding="utf-8") as f:
        fields = ["race_id", "offset_min", "planned_at", "status", "observed_at", "source_published_at",
                  "lag_seconds", "published_min_before_post", "post_time_changed", "error"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(captures)
    (out / "freshness_by_race.json").write_text(json.dumps(races, ensure_ascii=False, indent=2), encoding="utf-8")
    by_status = {}
    for c in captures:
        by_status[c["status"]] = by_status.get(c["status"], 0) + 1
    print(json.dumps({"captures": by_status, "races": races}, ensure_ascii=False, indent=2))
    lags = {}
    for c in captures:
        if c.get("lag_seconds") is not None:
            lags.setdefault(c["offset_min"], []).append(c["lag_seconds"])
    for off in sorted(lags, reverse=True):
        v = sorted(lags[off])
        print(f"T-{off}: n={len(v)} lag_seconds min={v[0]} median={v[len(v)//2]} max={v[-1]}")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--out", required=True)
    p.add_argument("--race", action="append", required=True, help="RACEID@YYYY-MM-DDTHH:MM (JST post time)")
    p.add_argument("--offsets", default="30,10,5,2")
    r = sub.add_parser("run")
    r.add_argument("--out", required=True)
    r.add_argument("--date", required=True, help="YYYY-MM-DD")
    r.add_argument("--db", default="data/odds/snapshots.sqlite")
    r.add_argument("--late-tolerance", type=int, default=60)
    r.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("report")
    s.add_argument("--out", required=True)
    args = parser.parse_args()

    out = ROOT / args.out
    if args.cmd == "plan":
        out.mkdir(parents=True, exist_ok=True)
        rows = build_schedule(args.race, tuple(int(x) for x in args.offsets.split(",")))
        with open(out / "schedule.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=SCHEDULE_FIELDS)
            w.writeheader()
            w.writerows(rows)
        print(f"{len(rows)} captures written to {out / 'schedule.csv'}")
    elif args.cmd == "run":
        run(out, args.date, dry_run=args.dry_run, late_tolerance=args.late_tolerance, db=ROOT / args.db)
    else:
        report(out)


if __name__ == "__main__":
    main()
