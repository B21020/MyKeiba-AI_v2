import unittest
from pathlib import Path

from tools.fetch_post_times import parse_race_list
from tools.jvlink_collect_0b41 import split_records
from tools.odds_snapshots import parse_o1

TRIAL = Path('data/odds/20261024_jv_trial/raw')


class PostTimeParseTests(unittest.TestCase):
    def test_each_item_pairs_its_own_id_and_time(self):
        html = ('<ul><li class="RaceList_DataItem"><a href="?race_id=202608040912"></a>'
                '<span class="RaceList_Itemtime">16:30</span></li>'
                '<li class="RaceList_DataItem"><a href="?race_id=202608040901"></a>'
                '<span class="RaceList_Itemtime"> 9:55</span></li></ul>')
        df = parse_race_list(html, '20261025')
        self.assertEqual(df.values.tolist(), [
            ['202608040912', '2026-10-25', '2026-10-25T16:30:00+09:00'],
            ['202608040901', '2026-10-25', '2026-10-25T09:55:00+09:00']])


class RecordSplitTests(unittest.TestCase):
    @unittest.skipUnless(TRIAL.exists(), 'trial records not available')
    def test_concatenated_trial_records_round_trip(self):
        raws = [p.read_bytes() for p in sorted(TRIAL.glob('*.bin'))]
        raws = [r + b'\r\n' if len(r) == 960 else r for r in raws]
        parts = split_records(b''.join(raws))
        self.assertEqual(len(parts), len(raws))
        self.assertEqual(parse_o1(parts[0])['race_id'], '202610020211')

    def test_rejects_partial_blob(self):
        with self.assertRaises(ValueError):
            split_records(b'x' * 1000)


if __name__ == '__main__':
    unittest.main()
