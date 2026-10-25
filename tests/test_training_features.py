import unittest

import numpy as np
import pandas as pd

from modules.preparing._jv_training import parse_hc, parse_wc
from modules.preprocessing._training_features import load_workouts, training_features


class ParseTests(unittest.TestCase):
    def test_hc_layout(self):
        raw = b''.join([b'HC', b'1', b'20261017', b'1', b'20261017', b'0536', b'2016100126',
                        b'0558', b'163', b'0395', b'139', b'0256', b'127', b'129', b'\r\n'])
        self.assertEqual(len(raw), 60)
        r = parse_hc(raw)
        self.assertEqual((r['date'], r['time'], r['horse_id']), ('2026-10-17', '0536', '2016100126'))
        self.assertEqual((r['h4'], r['l4'], r['h3'], r['l1']), (55.8, 16.3, 39.5, 12.9))

    def test_wc_layout_missing_long_distances(self):
        raw = (b'WC120261017120261017080220181029692' + b'0' + b'0' + b'0000000' * 6
               + b'0676186' + b'0490176' + b'0314159' + b'155' + b'\r\n')
        self.assertEqual(len(raw), 105)
        r = parse_wc(raw)
        self.assertIsNone(r['h10'])
        self.assertEqual((r['course'], r['h4'], r['h3'], r['h2'], r['l1']), ('2', 67.6, 49.0, 31.4, 15.5))


class FeatureTests(unittest.TestCase):
    def setUp(self):
        rows = []
        # 同じ日・同じトレセンの比較相手（z を作るため）
        for i, t in enumerate([52.0, 54.0, 56.0]):
            rows.append(dict(record='HC', make_date='20260101', tresen='1', date='2026-01-05', time=f'07{i}0',
                             horse_id=f'x{i}', h4=t, l1=12.5))
        rows += [
            dict(record='HC', make_date='20260101', tresen='1', date='2026-01-05', time='0800', horse_id='h1', h4=52.0, l1=12.0),
            dict(record='HC', make_date='20260101', tresen='1', date='2026-01-10', time='0800', horse_id='h1', h4=56.0, l1=13.0),
            dict(record='HC', make_date='20260101', tresen='1', date='2026-01-10', time='0700', horse_id='x0', h4=54.0, l1=12.5),
            # 予測するレースの当日の調教（使わない）
            dict(record='HC', make_date='20260101', tresen='1', date='2026-01-12', time='0800', horse_id='h1', h4=50.0, l1=11.0),
        ]
        hill = pd.DataFrame(rows)
        wood = pd.DataFrame(columns=['record', 'make_date', 'tresen', 'date', 'time', 'horse_id', 'course', 'h3', 'l1'])
        self.w = load_workouts(hill, wood)

    def test_only_workouts_before_race_day_within_window(self):
        t = pd.DataFrame({'horse_id': ['h1', 'h1', 'h2'], 'date': ['2026-01-12', '2026-01-30', '2026-01-12']}, index=[5, 6, 7])
        f = training_features(self.w, t)
        self.assertEqual(list(f.index), [5, 6, 7])
        self.assertEqual(f.loc[5, 'hill_n14'], 2)          # 1/5 と 1/10。1/12（当日）は含めない
        self.assertEqual(f.loc[5, 'tr_days_since'], 2)
        self.assertLess(f.loc[5, 'hill_last_fast'], 0)      # 直近（1/10）は相手より遅い
        self.assertGreater(f.loc[5, 'hill_best_fast'], 0)   # 1/5 は速い
        self.assertEqual(f.loc[6, 'hill_n14'], 0)           # 1/30 から14日以内は 1/16 以降 → 該当なし
        self.assertEqual(f.loc[7, 'tr_n14'], 0)
        self.assertTrue(np.isnan(f.loc[7, 'hill_last_fast']))


if __name__ == '__main__':
    unittest.main()
