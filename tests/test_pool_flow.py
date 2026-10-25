import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from tests.test_odds_snapshots import make_o1
from tools import odds_snapshots as odds
from tools.build_pool_table import audit, pools_from_blob
from tools.jvlink_live_latency import jv_key, new_snapshots
from tools.run_pool_flow import flow_gap, pool_asof


def with_pools(raw: bytes, win: int, place: int, waku: int) -> bytes:
    buf = bytearray(raw)
    buf[927:960] = (f'{win:011d}{place:011d}{waku:011d}').encode('ascii')
    return bytes(buf)


class PoolParseTests(unittest.TestCase):
    def test_pool_totals_at_spec_positions(self):
        r = odds.parse_o1(with_pools(make_o1(), 1234567, 89, 0))
        self.assertEqual((r['win_pool'], r['place_pool'], r['wakuren_pool']), (1234567, 89, 0))

    def test_blank_pool_is_none(self):
        self.assertIsNone(odds.parse_o1(make_o1())['win_pool'])

    def test_table_and_audit(self):
        blob = (with_pools(make_o1(published='10181440'), 100, 120, 10) + with_pools(make_o1(published='10181445'), 90, 130, 11)
                + with_pools(make_o1(kind='4', published='00000000'), 200, 250, 20))
        df = pd.DataFrame(pools_from_blob(blob))
        self.assertEqual(list(df['win_pool']), [100, 90, 200])
        a = audit(df)
        self.assertEqual(a['races_with_decreasing_interim_win_pool'], 1)
        self.assertEqual(a['races_after_close_below_last_interim'], 0)


class FlowTests(unittest.TestCase):
    def test_flow_share_and_gap(self):
        # 2頭。a: W=100、q=(.5,.5) → 50/50。b: W=200、q=(.75,.25) → 150/50。入った額 100/0
        lq_a, lq_b = np.log([.5, .5]), np.log([.75, .25])
        g = flow_gap(lq_a, [100, 100], lq_b, [200, 200], lq_b, ['r', 'r'])
        self.assertAlmostEqual(g[0], np.log(1.0) - np.log(.75))
        self.assertAlmostEqual(g[1], np.log(1e-4) - np.log(.25))   # 0 の割り振りは下限で止める

    def test_horse_missing_earlier_counts_from_zero(self):
        g = flow_gap([np.nan, 0.0], [100, 100], np.log([.5, .5]), [200, 200], np.log([.5, .5]), ['r', 'r'])
        self.assertAlmostEqual(np.exp(g[0] + np.log(.5)), 100 / 100)   # 0→100 が入った額の全部

    def test_pool_asof_uses_only_published_before_cutoff(self):
        post = pd.DataFrame({'race_id': ['r'], 'post_time': [pd.Timestamp('2026-10-25 10:05', tz='Asia/Tokyo')]})
        pools = pd.DataFrame({'race_id': ['r'] * 5, 'data_kind': ['1', '1', '1', '1', '4'],
                              'published_at': ['2026-10-25T09:54:00+09:00', '2026-10-25T09:59:00+09:00',
                                               '2026-10-25T10:04:00+09:00', '2026-10-25T09:35:00+09:00', None],
                              'win_pool': [20, 25, 60, 10, 63]})
        a = pool_asof(pools, post, [5, 10, 30]).iloc[0]
        self.assertEqual((a['W_5'], a['W_10'], a['W_30'], a['W_after']), (25, 20, 10, 63))


class LiveTests(unittest.TestCase):
    def test_jv_key(self):
        self.assertEqual(jv_key('202605040911', '20261025'), '2026102505040911')

    def test_new_snapshot_lag_and_dedup(self):
        raw = make_o1(published='10181454', key='2026101805040511')
        seen = set()
        post = datetime.fromisoformat('2026-10-18T15:00:00+09:00')
        now = datetime.fromisoformat('2026-10-18T14:54:40+09:00')
        rows = new_snapshots([raw, raw], seen, '202605040511', post, now)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['lag_seconds'], 40.0)
        self.assertEqual(rows[0]['seconds_left_before_post'], 320.0)
        self.assertEqual(new_snapshots([raw], seen, '202605040511', post, now), [])


if __name__ == '__main__':
    unittest.main()
