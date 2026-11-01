"""Synthetic protocol fixtures only; no sample odds are published as real data."""
import json
import socket
import sqlite3
import tempfile
import unittest
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

from tools.decision_aid.core import RuleError, Store, latest_t6, load_rule, race_view
from tools.decision_aid.freeze_rule import rule_hash
from tools.odds_snapshots import JST, parse_o1

RULE = {'win_calibration': {'beta_log_q6': 1.0}, 'place_calibration': {'intercept': 0.0, 'slope_logit_harville': 1.0},
        'paper_rule': {'bet': '複勝', 'stake_yen': 100, 'threshold': 1.0, 'skip_after_min': 4,
                       'snapshot_published_at_or_before_min': 5},
        'backtest': {'bets': 1, 'roi': 1.0, 'roi_ci95': [0.5, 1.5]},
        'reference': {'tansho_by_t6_popularity': {}, 'fukusho_by_t6_popularity': {}, 'umaren_by_quinella_popularity': {}}}
RULE['rule_hash'] = rule_hash(RULE)
POST = datetime(2026, 10, 18, 15, 0, tzinfo=JST)


def make_o1(published='10181454', kind='1', horses=None):
    """horses: [(馬番, 単勝 '0055', 複勝下限 '0021', 上限 '0030'), ...]"""
    buf = bytearray(b' ' * 960 + b'\r\n')

    def put(offset, value):
        buf[offset:offset + len(value)] = value.encode('ascii')
    horses = horses or [(i + 1, f'{20 + 15 * i:04d}', f'{11 + 4 * i:04d}', f'{15 + 8 * i:04d}') for i in range(8)]
    put(0, 'O1' + kind + '20261018')
    put(11, '2026101805040511')
    put(27, published)
    put(35, f'{len(horses):02d}{len(horses):02d}7770')
    for i, (n, w, lo, hi) in enumerate(horses):
        put(43 + 8 * i, f'{n:02d}{w}{i + 1:02d}')
        put(267 + 12 * i, f'{n:02d}{lo}{hi}{i + 1:02d}')
    return parse_o1(bytes(buf))


class RuleTests(unittest.TestCase):
    def test_edited_rule_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'rule.json'
            p.write_text(json.dumps(RULE), encoding='utf-8')
            self.assertEqual(load_rule(p)['rule_hash'], RULE['rule_hash'])
            bad = dict(RULE, paper_rule=dict(RULE['paper_rule'], threshold=0.9))
            p.write_text(json.dumps(bad), encoding='utf-8')
            with self.assertRaises(RuleError):
                load_rule(p)


class ViewTests(unittest.TestCase):
    def test_probabilities(self):
        v = race_view(make_o1(), RULE)
        self.assertEqual(len(v), 8)
        self.assertAlmostEqual(sum(x['p_win'] for x in v), 1.0)
        self.assertAlmostEqual(sum(x['p_place'] for x in v), 3.0, delta=0.3)   # 補正の傾き1・切片0なら Harville の合計3に近い
        self.assertTrue(all(abs(x['ev_place'] - x['p_place'] * x['place_low']) < 1e-12 for x in v))

    def test_pick_threshold(self):
        horses = [(1, '0020', '0011', '0012'), (2, '0040', '0090', '0150')] + [(n, '0090', '0020', '0040') for n in range(3, 9)]
        v = {x['horse_number']: x for x in race_view(make_o1(horses=horses), RULE)}
        self.assertEqual(v[2]['pick'], 1)          # 複勝の下限が高い
        self.assertEqual(v[1]['pick'], 0)

    def test_latest_t6_excludes_t1(self):
        recs = [make_o1('10181449'), make_o1('10181454'), make_o1('10181459'), make_o1('00000000', kind='4')]
        self.assertEqual(latest_t6(recs, POST, 5)['source_published_at'], '2026-10-18T14:54:00+09:00')


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'db.sqlite')

    def tearDown(self):
        self.store.conn.close()
        self.tmp.cleanup()

    def picks_record(self):
        horses = [(1, '0020', '0011', '0012'), (2, '0040', '0090', '0150')] + [(n, '0090', '0020', '0040') for n in range(3, 9)]
        return make_o1(horses=horses)

    def test_decision_once_and_skip_after_t4(self):
        rec = self.picks_record()
        r = self.store.record_decision('r1', rec, POST - timedelta(seconds=260), POST - timedelta(seconds=260), POST, RULE)
        self.assertEqual((r['skipped'], r['picks']), (0, 1))
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.record_decision('r1', rec, POST - timedelta(seconds=250), POST - timedelta(seconds=250), POST, RULE)
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM paper_bets').fetchone()[0], 1)
        late = self.store.record_decision('r2', rec, POST - timedelta(seconds=230), POST - timedelta(seconds=230), POST, RULE)
        self.assertEqual((late['skipped'], late['picks']), (1, 0))
        none = self.store.record_decision('r3', None, None, POST - timedelta(seconds=230), POST, RULE)
        self.assertEqual(none['skipped'], 1)

    def test_settlement_payout_and_refund(self):
        import pandas as pd
        self.store.save_schedule('2026-10-18', pd.DataFrame({'race_id': ['r1', 'r2'], 'post_time': [POST, POST]}))
        for rid in ('r1', 'r2'):
            self.store.record_decision(rid, self.picks_record(), POST - timedelta(seconds=260), POST - timedelta(seconds=260),
                                       POST, RULE)
        self.store.record_settlement('r1', {2: 950.0, 5: 120.0}, set(), False, 'test', POST)
        self.store.record_settlement('r2', {}, {2}, False, 'test', POST)
        rows = dict(self.store.conn.execute('SELECT race_id, payout FROM bet_results').fetchall())
        self.assertEqual(rows, {'r1': 950.0, 'r2': 100.0})
        s = self.store.paper_summary()
        self.assertEqual((s['bets'], s['refunded'], s['hits']), (2, 1, 1))
        self.assertAlmostEqual(s['roi'], 9.5)
        self.assertEqual(self.store.unsettled('2026-10-18'), [])


class ServerTests(unittest.TestCase):
    def test_state_endpoint(self):
        import pandas as pd
        from tools.decision_aid.live import serve
        with tempfile.TemporaryDirectory() as d:
            store = Store(Path(d) / 'db.sqlite')
            store.save_schedule('2026-10-18', pd.DataFrame({'race_id': ['202605040511'], 'post_time': [POST]}))
            store.record_decision('202605040511', make_o1(), POST - timedelta(seconds=260), POST - timedelta(seconds=260), POST, RULE)
            with socket.socket() as s:
                s.bind(('127.0.0.1', 0))
                port = s.getsockname()[1]
            httpd = serve(store, RULE, '20261018', port, clock=lambda: POST - timedelta(minutes=10))
            try:
                body = json.loads(urllib.request.urlopen(f'http://127.0.0.1:{port}/api/state', timeout=5).read())
                page = urllib.request.urlopen(f'http://127.0.0.1:{port}/', timeout=5).read()
            finally:
                httpd.shutdown()
                httpd.server_close()
                store.conn.close()
        self.assertEqual(body['races'][0]['label'], '東京11R')
        self.assertEqual(body['races'][0]['status'], 'decided')
        self.assertEqual(len(body['races'][0]['horses']), 8)
        self.assertIn('paper_total', body)
        self.assertIn('競馬の判断メモ'.encode('utf-8'), page)
        self.assertTrue(np.isclose(sum(h['p_win'] for h in body['races'][0]['horses']), 1.0))


if __name__ == '__main__':
    unittest.main()
