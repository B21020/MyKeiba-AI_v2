"""Offline checks for identity, chronology, access stops and non-destructive publication."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import requests

from tools import update_race_results as updater


class UpdateResultsTests(unittest.TestCase):
    def test_listing_only_accepts_jra_and_requested_year(self):
        html = b'<a href="result.html?race_id=202605010701">1</a>' \
               b'<a href="result.html?race_id=202644010701">NAR</a>' \
               b'<a href="result.html?race_id=202505010701">old</a>'
        self.assertEqual(updater.listed_races(html, "20260221"), {"202605010701": "20260221"})
        with self.assertRaises(ValueError):
            updater.listed_races(b"<html>unavailable</html>", "20260221")

    def test_calendar_does_not_accept_an_error_page(self):
        with self.assertRaises(ValueError):
            updater.calendar_dates(b"<html>temporarily unavailable</html>")

    def test_cancelled_race_requires_matching_identity_and_title_date(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            self.assertTrue(Path(folder).resolve().is_relative_to(Path(__file__).resolve().parent))
            path = Path(folder) / "201805010304.bin"
            path.write_text('<html><head><title>（取り止め）障害未勝利 | 2018年2月3日 東京4R</title><meta property="og:url" content="https://race.netkeiba.com/race/result.html?race_id=201805010304"></head></html>', encoding="utf-8")
            result = updater.parse_race(path, "201805010304", "20180203")
            self.assertEqual(result["status"], "cancelled")
            with self.assertRaises(ValueError):
                updater.parse_race(path, "201805010304", "20180204")
            with self.assertRaises(ValueError):
                updater.parse_race(path, "201805010305", "20180203")

    def test_documented_actual_date_survives_meeting_listing(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            self.assertTrue(Path(folder).resolve().is_relative_to(Path(__file__).resolve().parent))
            work = Path(folder)
            updater.write_json(work / "schedule_date_overrides.json", {"202006030201": {"actual_date": "20200329"}})
            self.assertEqual(updater.apply_date_overrides({"202006030201": "20200331"}, work), {"202006030201": "20200329"})

    def test_new_column_names_preserve_existing_non_null_values(self):
        data = pd.DataFrame({"枠番": [2, None], "枠": [1, 3], "単勝オッズ": [4.2, 8.5], "馬体重(増減)": ["450(+2)", "460(0)"]})
        result = updater.normalize_results(data)
        self.assertEqual(result["枠番"].tolist(), [2, 3])
        self.assertEqual(result["単勝"].tolist(), [4.2, 8.5])
        self.assertEqual(result["馬体重"].tolist(), ["450(+2)", "460(0)"])

    def test_horse_history_filters_post_cutoff_and_retains_past_only(self):
        table = '<table class="db_h_race_results"><tr><th>日付</th><th>着順</th></tr><tr><td>2026/10/22</td><td>1</td></tr><tr><td>2026/10/18</td><td>2</td></tr></table>'
        data = json.dumps({"status": "OK", "data": table}).encode()
        result = updater.parse_horse(data, "2023102602", "2026-10-21")
        self.assertEqual(result["日付"].tolist(), ["2026/10/18"])
        self.assertEqual(result.index.tolist(), ["2023102602"])

    def test_access_restriction_is_not_retried_or_cached(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            self.assertTrue(Path(folder).resolve().is_relative_to(Path(__file__).resolve().parent))
            work = Path(folder)
            client = updater.Fetcher(work, 1.2)
            response = requests.Response()
            response.status_code, response._content = 429, b"too many requests"
            with patch.object(client.session, "get", return_value=response) as get:
                with self.assertRaises(updater.AccessStopped):
                    client.get("https://example.test/", work / "page.html")
            self.assertEqual(get.call_count, 1)
            self.assertFalse((work / "page.html").exists())

    def test_publishing_preserves_original_and_is_idempotent(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as folder:
            self.assertTrue(Path(folder).resolve().is_relative_to(Path(__file__).resolve().parent))
            root = Path(folder)
            raw, work = root / "data/raw", root / "data/updates/run"
            raw.mkdir(parents=True)
            old = pd.DataFrame({"value": [1, 2]}, index=["old", "replace"])
            for name in ("results", "race_info", "return_tables"):
                old.to_pickle(raw / f"{name}.pickle")
            bundle = {"status": "ok", "race_id": "replace"}
            for name in ("results", "race_info", "return_tables", "entries"):
                bundle[name] = pd.DataFrame({"value": [3]}, index=["replace"])
            updater.write_pickle(work / "parsed/replace.pickle", bundle)
            with patch.object(updater, "ROOT", root):
                updater.merge_races(work)
                updater.merge_races(work)
            for name in ("results", "race_info", "return_tables"):
                pd.testing.assert_frame_equal(pd.read_pickle(work / "original_raw" / f"{name}.pickle"), old)
                updated = pd.read_pickle(raw / f"{name}.pickle")
                self.assertEqual(updated.index.tolist(), ["old", "replace"])
                self.assertEqual(updated["value"].tolist(), [1, 3])


if __name__ == "__main__":
    unittest.main()
