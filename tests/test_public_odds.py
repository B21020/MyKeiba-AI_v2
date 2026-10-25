import json
from pathlib import Path
import tempfile
import unittest

from tools import capture_public_odds as public
from tools import odds_snapshots as odds


def payload(status="middle", win=None, place=None):
    data = {"status": status, "data": {"official_datetime": "2026-10-23 22:35:23", "odds": {
        "1": win if win is not None else {"1": ["1.7", "0", "1", "08"], "2": ["3.2", "0", "2", "01"]},
        "2": place if place is not None else {"1": ["1.1", "1.2", "1", "08"], "2": ["1.4", "1.9", "2", "01"]},
    }}}
    return ("quoteCapture(" + json.dumps(data) + ");").encode()


CONTEXT = {"race_id": "202608040911", "jv_key": "2026102508040911", "race_date": "2026-10-25", "registered": 2}


class PublicOddsTests(unittest.TestCase):
    def test_rank_sorted_rows_use_actual_horse_number(self):
        result = public.parse_public(payload(), CONTEXT)
        self.assertEqual([r["horse_number"] for r in result["quotes"]], [1, 8])
        self.assertEqual(result["quotes"][1]["win_odds"], 1.7)
        self.assertEqual(result["quotes"][0]["place_high"], 1.9)
        self.assertIsNone(result["runners"])
        self.assertIsNone(result["place_paid_to"])

    def test_predicted_or_final_odds_are_not_live_market_quotes(self):
        for status in ["yoso", "final", "end", "before"]:
            with self.assertRaises(ValueError):
                public.parse_public(payload(status=status), CONTEXT)

    def test_horse_sets_and_coverage_must_match(self):
        with self.assertRaises(ValueError):
            public.parse_public(payload(place={"1": ["1.1", "1.2", "1", "08"]}), CONTEXT)
        with self.assertRaises(ValueError):
            public.parse_public(payload(), {**CONTEXT, "registered": 18})

    def test_jsonp_is_parsed_as_data_not_executed(self):
        with self.assertRaises(ValueError):
            public.parse_public(payload() + b";evil()", CONTEXT)

    def test_source_is_kept_separate_from_jra_van(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as name:
            work = Path(name).resolve()
            self.assertTrue(work.is_relative_to(Path(__file__).resolve().parent))
            conn = odds.connect(work / "test.sqlite")
            try:
                odds.store_normalized(conn, public.parse_public(payload(), CONTEXT), payload(), observed_at="2026-10-23T22:36:00+09:00", mode="live", archive=work / "raw", source="netkeiba_public")
                with self.assertRaises(ValueError):
                    odds.select_asof(conn, CONTEXT["race_id"], "2026-10-23T22:37:00+09:00", mode="live")
                selected = odds.select_asof(conn, CONTEXT["race_id"], "2026-10-23T22:37:00+09:00", mode="live", source="netkeiba_public")
                self.assertEqual(selected["snapshot"]["source"], "netkeiba_public")
                self.assertEqual(len(selected["quotes"]), 2)
            finally:
                conn.close()

    def test_context_post_time_is_timezone_aware(self):
        html = '<html><head><title>菊花賞 | 2026年10月25日</title><meta property="og:url" content="https://race.netkeiba.com/odds/index.html?race_id=202608040911"></head><body><div class="RaceList_Item02">15:40発走 18頭</div></body></html>'.encode()
        result = public.parse_context(html, "202608040911")
        self.assertEqual(result["scheduled_start_at"], "2026-10-25T15:40:00+09:00")


if __name__ == "__main__":
    unittest.main()
