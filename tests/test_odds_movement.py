import unittest

import numpy as np
import pandas as pd

from tools.run_odds_movement import add_features, asof_from_quotes


def quotes():
    post = pd.Timestamp('2026-01-31 10:05', tz='Asia/Tokyo')
    rows = []
    for minutes, odds in [(900, (2.0, 4.0)), (31, (2.5, 3.0)), (11, (3.0, 2.5)), (6, (4.0, 2.0)), (1, (5.0, 1.5))]:
        for n, o in zip((1, 2), odds):
            rows.append({'race_id': 'r', 'published_at': post - pd.Timedelta(minutes=minutes), 'post_time': post,
                         'horse_number': n, 'win_odds': o})
    return pd.DataFrame(rows)


class AsofTests(unittest.TestCase):
    def test_latest_before_each_cutoff_and_open(self):
        a = asof_from_quotes(quotes(), [5, 10, 30, 0]).set_index('馬番')
        lq = lambda o1, o2: np.log((1 / o1) / (1 / o1 + 1 / o2))  # noqa: E731
        self.assertAlmostEqual(a.loc[1, 'lq_5'], lq(4.0, 2.0))     # T-6 の配信（T-1 は含まない）
        self.assertAlmostEqual(a.loc[1, 'lq_10'], lq(3.0, 2.5))    # T-11
        self.assertAlmostEqual(a.loc[1, 'lq_30'], lq(2.5, 3.0))    # T-31
        self.assertAlmostEqual(a.loc[1, 'lq_0'], lq(5.0, 1.5))     # 発走直前の最後の配信
        self.assertAlmostEqual(a.loc[1, 'lq_open'], lq(2.0, 4.0))  # 最初の配信

    def test_features_use_only_t6_and_earlier(self):
        a = asof_from_quotes(quotes(), [5, 10, 30, 120])
        d = pd.DataFrame({'race_id': ['r', 'r'], '馬番': [1, 2], 'log_q6': a.sort_values('馬番')['lq_5'].to_numpy(),
                          'log_q': [np.log(0.2), np.log(0.8)], 'h': [.5, .5], 'pm': [.5, .5], 'floored': [0, 1]})
        f = add_features(d, a).set_index('馬番')
        self.assertAlmostEqual(f.loc[1, 'mom_10'], f.loc[1, 'log_q6'] - f.loc[1, 'lq_10'])
        self.assertEqual(f.loc[1, 'mom_120'], f.loc[1, 'log_q6'] - f.loc[1, 'lq_open'])  # 120分前の配信は前日の最初の配信
        self.assertEqual(f.loc[2, 'place_gap'], 0.0)                                    # 最低払戻に張り付いた馬
        self.assertAlmostEqual(f.loc[1, 'move'], np.log(0.2) - f.loc[1, 'log_q6'])


if __name__ == '__main__':
    unittest.main()
