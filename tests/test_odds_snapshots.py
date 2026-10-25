"""Synthetic protocol fixtures only; no sample odds are published as real data."""
from pathlib import Path
import tempfile
import unittest

from tools import odds_snapshots as odds


def make_o1(published="10181454", kind="1", win="0055", place_low="0021", place_high="0030", key="2026101805040511"):
    buf = bytearray(b" " * 960 + b"\r\n")
    def put(offset, value):
        buf[offset:offset + len(value)] = value.encode("ascii")
    put(0, "O1" + kind + "20261018")
    put(11, key)
    put(27, published)
    put(35, "08087773")
    for i in range(8):
        put(43 + 8 * i, f"{i+1:02d}" + win + f"{i+1:02d}")
        put(267 + 12 * i, f"{i+1:02d}" + place_low + place_high + f"{i+1:02d}")
    return bytes(buf)


class OddsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.work = Path(self.tmp.name).resolve()
        self.assertTrue(self.work.is_relative_to(Path(__file__).resolve().parent))
        self.conn = odds.connect(self.work / "test.sqlite")

    def tearDown(self):
        self.conn.close()
        self.assertTrue(self.work.is_relative_to(Path(__file__).resolve().parent))
        self.tmp.cleanup()

    def save(self, raw, mode="historical", observed="2026-10-23T22:00:00+09:00"):
        return odds.store(self.conn, raw, observed_at=observed, mode=mode, archive=self.work / "raw")

    def select(self, mode="historical", when="2026-10-18T14:55:00+09:00"):
        return odds.select_asof(self.conn, "202605040511", when, mode=mode)

    def test_parse_prices_boundaries_and_key(self):
        data = odds.parse_o1(make_o1())
        self.assertEqual(data["race_id"], "202605040511")
        self.assertEqual(data["source_published_at"], "2026-10-18T14:54:00+09:00")
        self.assertEqual(data["quotes"][0]["win_odds"], 5.5)
        self.assertEqual((data["quotes"][0]["place_low"], data["quotes"][0]["place_high"]), (2.1, 3.0))
        with self.assertRaises(ValueError):
            odds.parse_o1(make_o1(place_low="0031"))

    def test_special_prices_are_not_fake_zero_odds(self):
        for value, state in [("0000", "no_votes"), ("----", "cancelled_before_sale"), ("****", "cancelled_after_sale"), ("    ", "unregistered")]:
            parsed = odds.parse_o1(make_o1(win=value))
            self.assertIsNone(parsed["quotes"][0]["win_odds"])
            self.assertEqual(parsed["quotes"][0]["win_status"], state)
        self.assertEqual(odds.quote_value("9999"), (999.9, "capped"))

    def test_wrong_race_is_rejected_before_archiving(self):
        with self.assertRaises(ValueError):
            odds.store(self.conn, make_o1(), observed_at="2026-10-23T22:00:00+09:00", mode="historical", archive=self.work / "raw", expected_race_id="202605040512")
        self.assertFalse((self.work / "raw").exists())

    def test_future_publication_is_not_selected(self):
        old = self.save(make_o1())
        self.save(make_o1(published="10181456", win="0070"))
        self.assertEqual(self.select()["snapshot"]["snapshot_id"], old)

    def test_replay_does_not_claim_live_observation(self):
        self.save(make_o1())
        self.assertEqual(len(self.select()["quotes"]), 8)
        self.save(make_o1(), mode="live")
        with self.assertRaisesRegex(ValueError, "No eligible"):
            self.select(mode="live")

    def test_live_snapshot_must_have_arrived_by_decision(self):
        self.save(make_o1(), mode="live", observed="2026-10-18T14:54:20+09:00")
        self.assertEqual(len(self.select(mode="live")["quotes"]), 8)
        with self.assertRaises(ValueError):
            self.save(make_o1(published="10181456"), mode="live", observed="2026-10-18T14:54:20+09:00")

    def test_same_publication_correction_only_applies_after_it_arrived(self):
        original = self.save(make_o1(), mode="live", observed="2026-10-18T14:54:20+09:00")
        corrected = self.save(make_o1(win="0070"), mode="live", observed="2026-10-18T14:55:30+09:00")
        self.assertEqual(self.select(mode="live")["snapshot"]["snapshot_id"], original)
        self.assertEqual(self.select(mode="live", when="2026-10-18T14:56:00+09:00")["snapshot"]["snapshot_id"], corrected)

    def test_final_prices_never_enter_interim_selection(self):
        self.save(make_o1(kind="4", published="00000000"))
        with self.assertRaises(ValueError):
            self.select()

    def test_stale_quote_is_not_used(self):
        self.save(make_o1(published="10181440"))
        with self.assertRaisesRegex(ValueError, "stale"):
            self.select()

    def test_latest_cancellation_is_not_backfilled_from_older_quote(self):
        self.save(make_o1(published="10181453"))
        newest = self.save(make_o1(win="****"))
        selected = self.select()
        self.assertEqual(selected["snapshot"]["snapshot_id"], newest)
        self.assertIsNone(selected["quotes"][0]["win_odds"])

    def test_repeat_ingestion_is_idempotent_and_keeps_observations(self):
        first = self.save(make_o1())
        second = self.save(make_o1(), observed="2026-10-23T23:00:00+09:00")
        self.assertEqual(first, second)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0], 8)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 2)

    def test_cross_year_pre_sale_and_timezone_validation(self):
        data = odds.parse_o1(make_o1(key="2026010406010101", published="12311500"))
        self.assertEqual(data["source_published_at"], "2025-12-31T15:00:00+09:00")
        with self.assertRaisesRegex(ValueError, "offset"):
            odds.timestamp("2026-10-18T14:55:00")


if __name__ == "__main__":
    unittest.main()
