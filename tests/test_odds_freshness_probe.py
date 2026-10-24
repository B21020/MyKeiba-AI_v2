import csv
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from tools import odds_freshness_probe as probe
from tools.odds_snapshots import JST


class FakeClock:
    def __init__(self, start):
        self.t = start

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.t += timedelta(seconds=seconds)


def write_schedule(out: Path, rows):
    with open(out / "schedule.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=probe.SCHEDULE_FIELDS)
        w.writeheader()
        w.writerows(rows)


class FreshnessProbeTests(unittest.TestCase):
    def test_schedule_offsets_are_before_post_and_sorted(self):
        rows = probe.build_schedule(["202608040911@2026-10-25T15:40", "202604040411@2026-10-25T15:10"])
        self.assertEqual(len(rows), 8)
        self.assertEqual([r["planned_at"] for r in rows], sorted(r["planned_at"] for r in rows))
        kikka = [r for r in rows if r["race_id"] == "202608040911"]
        self.assertEqual([r["planned_at"][11:16] for r in kikka], ["15:10", "15:30", "15:35", "15:38"])
        with self.assertRaises(ValueError):
            probe.build_schedule(["2026080409@2026-10-25T15:40"])

    def test_run_waits_logs_errors_and_marks_late_captures_missed(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            write_schedule(out, probe.build_schedule(["202608040911@2026-10-25T15:40"]))
            # Start after T-30 has passed by more than the tolerance.
            clock = FakeClock(datetime(2026, 10, 25, 15, 12, tzinfo=JST))
            calls = []

            def fake_capture(race_id, db):
                calls.append(clock.now())
                if len(calls) == 2:
                    raise RuntimeError("HTTP 503")
                return {"observed_at": clock.now().isoformat(), "source_published_at": "2026-10-25T15:28:00+09:00",
                        "scheduled_start_at": "2026-10-25T15:40:00+09:00", "snapshot_id": "x", "horses": 18}

            probe.run(out, "2026-10-25", dry_run=False, late_tolerance=60, db=out / "db.sqlite",
                      capture=fake_capture, now=clock.now, sleep=clock.sleep)
            entries = [json.loads(l) for l in (out / "captures.jsonl").read_text(encoding="utf-8").splitlines()]
            self.assertEqual([e["status"] for e in entries], ["missed", "ok", "error", "ok"])
            self.assertEqual([c.strftime("%H:%M") for c in calls], ["15:30", "15:35", "15:38"])
            self.assertIn("HTTP 503", entries[2]["error"])

    def test_summary_lag_and_publication_steps(self):
        base = {"race_id": "202608040911", "scheduled_start_at": "2026-10-25T15:40:00+09:00", "status": "ok"}
        entries = [
            {**base, "offset_min": 10, "observed_at": "2026-10-25T15:30:05+09:00", "source_published_at": "2026-10-25T15:28:00+09:00"},
            {**base, "offset_min": 5, "observed_at": "2026-10-25T15:35:05+09:00", "source_published_at": "2026-10-25T15:34:00+09:00"},
            {**base, "offset_min": 2, "observed_at": "2026-10-25T15:38:05+09:00", "source_published_at": "2026-10-25T15:34:00+09:00"},
        ]
        captures, races = probe.summarize(entries)
        self.assertEqual([c["lag_seconds"] for c in captures], [125.0, 65.0, 245.0])
        self.assertEqual(captures[0]["published_min_before_post"], 12.0)
        self.assertEqual(races[0]["distinct_published"], 2)
        self.assertEqual(races[0]["published_steps_min"], [6.0])
        self.assertEqual(races[0]["repeat_without_update"], 1)


if __name__ == "__main__":
    unittest.main()
