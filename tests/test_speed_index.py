import unittest

import numpy as np
import pandas as pd

from modules.preprocessing._speed_index import fit_base_times, horse_speed_features, parse_history, speed_index
import modules.preprocessing._speed_index as si_mod


def history():
    rows = []
    # 基準を作るための多数の走（2020年）と、評価対象馬 h1 の走
    for i in range(40):
        rows.append(('x%d' % i, '2020/05/01', '1東京1', '芝1600', '1:34.0'))
    rows += [
        ('h1', '2020/06/01', '1東京1', '芝1600', '1:33.0'),   # 基準より1秒速い
        ('h1', '2021/06/01', '1東京1', '芝1600', '1:35.0'),   # 基準より1秒遅い（cutoff 後）
        ('h1', '2021/07/01', '1東京1', '障3000', '3:20.0'),   # 障害は対象外
        ('h1', '2021/08/01', '1東京1', '芝1600', '1:34.0'),   # 予測対象日と同じ日の走（使わない）
    ]
    df = pd.DataFrame(rows, columns=['horse_id', '日付', '開催', '距離', 'タイム']).set_index('horse_id')
    return df


class SpeedIndexTests(unittest.TestCase):
    def setUp(self):
        self._saved = (si_mod.MIN_BASE_RUNS, si_mod.MIN_DAY_RUNS)
        si_mod.MIN_BASE_RUNS = 30
        si_mod.MIN_DAY_RUNS = 1000  # 馬場差を0にして指数を確かめる

    def tearDown(self):
        si_mod.MIN_BASE_RUNS, si_mod.MIN_DAY_RUNS = self._saved

    def test_parse_drops_jump_and_missing_time(self):
        runs = parse_history(history())
        self.assertEqual(set(runs['surface']), {'芝'})
        self.assertAlmostEqual(runs['time_sec'].iloc[0], 94.0)

    def test_base_uses_only_runs_before_cutoff(self):
        runs = parse_history(history())
        base = fit_base_times(runs, '2021-01-01')
        self.assertEqual(int(base['base_n'].iloc[0]), 41)
        self.assertAlmostEqual(base['base_time'].iloc[0], 94.0)
        # cutoff を早めると件数が足りず基準タイムは作られない
        self.assertTrue(fit_base_times(runs, '2020-05-02').empty is False)
        self.assertTrue(fit_base_times(runs, '2020-05-01').empty)

    def test_features_use_only_runs_before_target_date(self):
        runs = speed_index(parse_history(history()), fit_base_times(parse_history(history()), '2021-01-01'))
        h1 = runs[runs['horse_id'] == 'h1'].sort_values('date')
        np.testing.assert_allclose(h1['si'].to_numpy()[:2], [1000 / 94, -1000 / 94], rtol=1e-6)
        targets = pd.DataFrame({'horse_id': ['h1', 'h1', 'h1'],
                                'date': pd.to_datetime(['2020-06-01', '2021-06-02', '2021-08-01'])}, index=[10, 11, 12])
        f = horse_speed_features(runs, targets)
        self.assertEqual(list(f.index), [10, 11, 12])
        self.assertTrue(np.isnan(f.loc[10, 'si_last']))            # 当日の走は使わない
        self.assertAlmostEqual(f.loc[11, 'si_last'], -1000 / 94, places=6)
        self.assertAlmostEqual(f.loc[11, 'si_mean3'], 0.0, places=6)
        self.assertAlmostEqual(f.loc[12, 'si_last'], -1000 / 94, places=6)  # 同日の走（2021-08-01）は含めない
        self.assertEqual(f.loc[12, 'si_n5'], 2)


if __name__ == '__main__':
    unittest.main()
