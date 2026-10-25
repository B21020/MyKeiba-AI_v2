import itertools
import unittest

import numpy as np
import pandas as pd

from modules.evaluation import (discounted_place, fukusho_payouts, harville_place, place_market_probs, place_pool_shares,
                                place_slots, simulated_place)
from tools.run_place_market import paired_place_r2


def brute_force_place(p, k):
    out = np.zeros(len(p))
    for perm in itertools.permutations(range(len(p)), k):
        pr, left = 1.0, 1.0
        for i in perm:
            pr *= p[i] / left
            left -= p[i]
        out[list(perm)] += pr
    return out


def high_odds(x, slots):
    """売れた割合 x から、JRA の式で複勝オッズの上限を作る（相手は最も売れていない馬）。"""
    return np.array([1 + (0.8 - x[i] - np.sort(np.delete(x, i))[:slots - 1].sum()) / (slots * x[i]) for i in range(len(x))])


class PlaceProbabilityTests(unittest.TestCase):
    def test_slots(self):
        self.assertEqual([place_slots(n) for n in (4, 5, 7, 8, 18)], [0, 2, 2, 3, 3])

    def test_harville_matches_brute_force(self):
        p = np.array([.4, .25, .15, .1, .06, .04])
        for k in (1, 2, 3):
            np.testing.assert_allclose(harville_place(p, k), brute_force_place(p, k), atol=1e-12)
            self.assertAlmostEqual(harville_place(p, k).sum(), k)

    def test_pool_shares_recovered_from_high_odds(self):
        x = np.array([.40, .15, .12, .10, .08, .06, .05, .04])
        np.testing.assert_allclose(place_pool_shares(high_odds(x, 3), 3), x, atol=1e-6)
        x2 = np.array([.35, .25, .2, .12, .08])
        np.testing.assert_allclose(place_pool_shares(high_odds(x2, 2), 2), x2, atol=1e-6)

    def test_floored_favourite_takes_remainder_and_is_capped(self):
        x = np.array([.40, .15, .12, .10, .08, .06, .05, .04])
        hi = high_odds(x, 3)
        hi[0] = 1.1
        np.testing.assert_allclose(place_pool_shares(hi, 3), x, atol=1e-6)
        p = place_market_probs(hi, ['r'] * len(x), [3] * len(x))
        self.assertAlmostEqual(p[0], 0.95)
        np.testing.assert_allclose(p[1:], 3 * x[1:], atol=1e-5)


class PayoutTests(unittest.TestCase):
    def test_fukusho_with_dead_heat(self):
        rt = pd.DataFrame([['単勝', '5', '120', '1'], ['複勝', '5br8br6br2', '100br110br310br290', '1br2br6br5']],
                          index=['r1', 'r1'])
        f = fukusho_payouts(rt)
        self.assertEqual(list(f['馬番']), [5, 8, 6, 2])
        self.assertEqual(list(f['fukusho_payout']), [100.0, 110.0, 310.0, 290.0])


class PlaceR2Tests(unittest.TestCase):
    def test_better_probability_has_positive_delta(self):
        y = np.array([1, 0, 0, 0, 1, 0])
        r = ['a'] * 3 + ['b'] * 3
        good, bad, null = np.array([.8, .1, .1, .1, .8, .1]), np.full(6, .5), np.full(6, 1 / 3)
        self.assertGreater(paired_place_r2(good, bad, null, y, r, n=50)['delta_r2'], 0)


class DiscountTests(unittest.TestCase):
    p = np.array([.35, .2, .12, .09, .07, .05, .04, .03, .02, .015, .005]) / .99

    def test_lambda_one_is_harville(self):
        for k in (2, 3):
            np.testing.assert_allclose(discounted_place(self.p, k, 1.0, 1.0), harville_place(self.p, k), atol=1e-12)

    def test_discount_sums_to_slots_and_lifts_longshots(self):
        d = discounted_place(self.p, 3, 0.76, 0.62)
        self.assertAlmostEqual(d.sum(), 3.0, places=9)
        h = harville_place(self.p, 3)
        self.assertLess(d[0], h[0])
        self.assertGreater(d[-1], h[-1])

    def test_simulation_matches_win_probabilities(self):
        for model, shape in (('normal', 0.0), ('gamma', 4.0)):
            np.testing.assert_allclose(simulated_place(self.p, 1, model, shape), self.p, atol=0.003)

    def test_gamma_shape_one_is_harville(self):
        np.testing.assert_allclose(simulated_place(self.p, 3, 'gamma', 1.0, n_sims=200000), harville_place(self.p, 3), atol=0.005)


if __name__ == '__main__':
    unittest.main()
