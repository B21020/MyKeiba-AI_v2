import unittest

import numpy as np
import pandas as pd

from modules.evaluation import (
    FINAL_PERIOD_START, ConditionalLogit, FinalPeriodError, bootstrap_by_day, make_rolling_folds,
    paired_delta_r2, pseudo_r2, race_log_loss, race_softmax, tansho_payouts, top1_accuracy,
)


class FoldTests(unittest.TestCase):
    def test_segments_are_ordered_and_disjoint(self):
        (f,) = make_rolling_folds(['2025-01-01'])
        self.assertEqual(f.to_dict(), {
            'name': 'outer_202501', 'fit': ['2020-01-01', '2024-01-01'], 'select': ['2024-01-01', '2024-07-01'],
            'calib': ['2024-07-01', '2024-10-01'], 'betsel': ['2024-10-01', '2025-01-01'],
            'outer': ['2025-01-01', '2025-07-01']})
        dates = pd.Series(pd.date_range('2019-06-01', '2025-12-31', freq='D'))
        masks = [f.mask(dates, s) for s in ('fit', 'select', 'calib', 'betsel', 'outer')]
        self.assertTrue((sum(m.astype(int) for m in masks) <= 1).all())

    def test_final_period_is_refused(self):
        self.assertEqual(str(FINAL_PERIOD_START.date()), '2026-07-01')
        make_rolling_folds(['2026-01-01'])  # ends exactly at 2026-07-01
        with self.assertRaises(FinalPeriodError):
            make_rolling_folds(['2026-02-01'])
        make_rolling_folds(['2026-02-01'], allow_final=True)


class ConditionalLogitTests(unittest.TestCase):
    def test_recovers_coefficient(self):
        rng = np.random.default_rng(0)
        n_races, size, beta = 3000, 10, 1.6
        race = np.repeat(np.arange(n_races), size)
        x = rng.normal(size=n_races * size)
        p = race_softmax(beta * x, race)
        win = np.zeros_like(p)
        for r in range(n_races):
            sl = slice(r * size, (r + 1) * size)
            win[sl][rng.choice(size, p=p[sl])] = 1
        cl = ConditionalLogit().fit(pd.DataFrame({'x': x}), race, win)
        self.assertAlmostEqual(cl.coef_[0], beta, delta=0.1)
        pr = cl.predict_proba(pd.DataFrame({'x': x}), race)
        np.testing.assert_allclose(np.bincount(race, weights=pr), 1.0)

    def test_metrics_uniform_and_perfect(self):
        race = np.array([0, 0, 1, 1, 1])
        win = np.array([1, 0, 0, 1, 0])
        uniform = np.array([.5, .5, 1 / 3, 1 / 3, 1 / 3])
        self.assertAlmostEqual(pseudo_r2(uniform, race, win), 0.0)
        self.assertAlmostEqual(race_log_loss(uniform, race, win), (np.log(2) + np.log(3)) / 2)
        sharp = np.array([.9, .1, .05, .9, .05])
        self.assertGreater(pseudo_r2(sharp, race, win), 0.5)
        self.assertEqual(top1_accuracy(sharp, race, win), 1.0)
        d = paired_delta_r2(sharp, uniform, race, win, n=200)
        self.assertAlmostEqual(d['delta_r2'], pseudo_r2(sharp, race, win) - pseudo_r2(uniform, race, win))
        self.assertEqual(paired_delta_r2(uniform, uniform, race, win, n=50)['ci95'], [0.0, 0.0])


class PayoutAndBootstrapTests(unittest.TestCase):
    def test_dead_heat_payouts(self):
        rt = pd.DataFrame({0: ['単勝', '複勝', '単勝', '単勝'], 1: ['5br9', '1br2', '3', '4 11'],
                           2: ['490br1,040', '1br2', '120', '460br130'], 3: ['', '', '', '']},
                          index=['A', 'A', 'B', 'C'])
        p = tansho_payouts(rt)
        self.assertEqual(p.values.tolist(), [['A', 5, 490.0], ['A', 9, 1040.0], ['B', 3, 120.0],
                                             ['C', 4, 460.0], ['C', 11, 130.0]])

    def test_bootstrap_contains_point_estimate(self):
        rng = np.random.default_rng(1)
        d = pd.DataFrame({'stake': np.full(100, 1000.0), 'payout': rng.uniform(0, 2000, 100)})
        r = bootstrap_by_day(d, n=500)
        self.assertLess(r['roi_ci95'][0], r['roi'])
        self.assertGreater(r['roi_ci95'][1], r['roi'])


if __name__ == '__main__':
    unittest.main()
