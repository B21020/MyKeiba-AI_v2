import unittest
from types import SimpleNamespace

import numpy as np
import pandas as pd

from tools.pipeline_compare import build_and_train as bt
from tools.pipeline_compare.prepare_inputs import prepare_results


class PrepareResultsTests(unittest.TestCase):
    def test_dedup_coalesce_tansho_and_filter(self):
        raw = pd.DataFrame({'着順': ['1', '1', '2', '取消', '1'],
                            '馬番': [1, 1, 2, 3, 1],
                            '単勝': ['2.5', '2.5', None, '---', '4.0'],
                            '単勝オッズ': [None, None, '7.1', None, None]},
                           index=pd.Index(['r1', 'r1', 'r1', 'r1', 'r2'], name='race_id'))
        df = prepare_results(raw, {'r1'})
        self.assertIsNone(df.index.name)
        self.assertEqual(list(df['馬番']), [1, 2, 3])        # 全列一致の重複を除き、r2 は対象外
        self.assertEqual(df['単勝'].iloc[0], 2.5)
        self.assertEqual(df['単勝'].iloc[1], 7.1)            # 新名の列で埋める
        self.assertTrue(np.isnan(df['単勝'].iloc[2]))        # '---' は欠損

    def test_same_horse_different_values_is_kept(self):
        raw = pd.DataFrame({'馬番': [1, 1], '単勝': ['2.5', '2.6']}, index=['r1', 'r1'])
        self.assertEqual(len(prepare_results(raw, {'r1'})), 2)


class FakeModel:
    def predict_proba(self, X):
        n = len(X)
        return np.column_stack([np.zeros(n), np.full(n, 0.5)])

    def get_params(self):
        return {}


class FakeFactory:
    seen = None

    @classmethod
    def create(cls, train):
        cls.seen = train
        ai = SimpleNamespace(datasets=SimpleNamespace(X_train=train.drop(bt.ORIG_DROP, axis=1)),
                             train_with_tuning=lambda: None)
        setattr(ai, '_KeibaAI__model_wrapper', SimpleNamespace(lgb_model=FakeModel()))
        return ai


class CutoffTests(unittest.TestCase):
    def test_train_before_cutoff_and_predict_horizon(self):
        dates = pd.to_datetime(['2025-12-31', '2026-01-01', '2026-04-30', '2026-05-01'])
        fd = pd.DataFrame({'date': dates, 'rank': [1, 0, 1, 0], '単勝': [2.0] * 4, '馬番': [1, 2, 3, 4], 'x': [0.1] * 4},
                          index=['a', 'b', 'c', 'd'])
        scores, info = bt.train_and_score('orig', SimpleNamespace(KeibaAIFactory=FakeFactory), fd, pd.Timestamp('2026-01-01'))
        self.assertEqual(list(FakeFactory.seen.index), ['a'])
        self.assertEqual(list(scores['race_id']), ['b', 'c'])   # cutoff 〜 cutoff+4か月（終わりは含まない）
        self.assertEqual(info['train_max_date'], '2025-12-31')

    def test_horizon_must_end_before_final_period(self):
        fd = pd.DataFrame({'date': pd.to_datetime(['2026-02-01']), 'rank': [1], '単勝': [2.0], '馬番': [1]})
        with self.assertRaises(AssertionError):
            bt.train_and_score('orig', SimpleNamespace(KeibaAIFactory=FakeFactory), fd, pd.Timestamp('2026-04-01'))


if __name__ == '__main__':
    unittest.main()
