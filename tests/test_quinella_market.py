"""Synthetic protocol fixtures only; no sample odds are published as real data."""
import itertools
import unittest

import numpy as np
import pandas as pd

from modules.evaluation import umaren_payouts
from tools.odds_snapshots import parse_o2
from tools.run_quinella_market import harville_pairs, t6_quinella


def make_o2(kind='1', published='10251004', key='2026102505040901', odds=None, pool=12345):
    """O2 レコード（2042バイト）。odds: {(a, b): '000123'}。指定の無い組は空白（登録なし）。"""
    buf = bytearray(b' ' * 2040 + b'\r\n')

    def put(offset, value):
        buf[offset:offset + len(value)] = value.encode('ascii')
    put(0, 'O2' + kind + '20261025')
    put(11, key)
    put(27, published)
    put(35, '04047')
    for i, ((a, b), o) in enumerate(sorted((odds or {}).items())):
        put(40 + 13 * i, f'{a:02d}{b:02d}{o}{i + 1:03d}')
    put(2029, f'{pool:011d}')
    return bytes(buf)


class ParseO2Tests(unittest.TestCase):
    def test_fields(self):
        r = parse_o2(make_o2(odds={(1, 2): '000123', (1, 3): '000000', (2, 3): '------'}))
        self.assertEqual(r['race_id'], '202605040901')
        self.assertEqual(r['source_published_at'], '2026-10-25T10:04:00+09:00')
        self.assertEqual((r['registered'], r['runners'], r['pool']), (4, 4, 12345))
        c = {(x['first'], x['second']): (x['odds'], x['status']) for x in r['combos']}
        self.assertEqual(c[(1, 2)], (12.3, 'quoted'))
        self.assertEqual(c[(1, 3)], (None, 'no_votes'))
        self.assertEqual(c[(2, 3)], (None, 'cancelled_before_sale'))

    def test_wrong_length(self):
        with self.assertRaises(ValueError):
            parse_o2(make_o2()[:-5])


class SelectionTests(unittest.TestCase):
    def test_latest_published_before_cutoff(self):
        post = pd.Timestamp('2026-10-25T10:10:00+09:00')
        blob = (make_o2(published='10251059'.replace('1059', '0959'), odds={(1, 2): '000050'})
                + make_o2(published='10251004', odds={(1, 2): '000060'})
                + make_o2(published='10251009', odds={(1, 2): '000070'})       # 発走1分前：使わない
                + make_o2(kind='4', published='00000000', odds={(1, 2): '000080'}))
        s = t6_quinella(blob, post)
        self.assertEqual(s['odds'][(1, 2)], 6.0)
        self.assertEqual(str(s['published_at']), '2026-10-25 10:04:00+09:00')


class HarvilleTests(unittest.TestCase):
    def test_pairs_match_brute_force(self):
        q = np.array([.4, .3, .2, .1])
        h = harville_pairs(q)
        for i, j in itertools.combinations(range(4), 2):
            brute = q[i] * q[j] / (1 - q[i]) + q[j] * q[i] / (1 - q[j])
            self.assertAlmostEqual(h[i, j], brute)
        self.assertAlmostEqual(np.triu(h, 1).sum(), 1.0)


class PayoutTests(unittest.TestCase):
    def test_dead_heat_pairs(self):
        rt = pd.DataFrame([['馬連', '8 - 9br8 - 12', '8,920br1,990', '38br13']], index=['r'])
        u = umaren_payouts(rt)
        self.assertEqual(list(zip(u['first'], u['second'], u['umaren_payout'])), [(8, 9, 8920.0), (8, 12, 1990.0)])

    def test_old_space_separated_format(self):
        rt = pd.DataFrame([['馬連', '7 14 14 15', '20,790br1,870', '40br5']], index=['r'])
        u = umaren_payouts(rt)
        self.assertEqual(list(zip(u['first'], u['second'])), [(7, 14), (14, 15)])


if __name__ == '__main__':
    unittest.main()
