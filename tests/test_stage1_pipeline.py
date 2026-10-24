import unittest

import numpy as np
import pandas as pd

from modules.preparing._horse_history_columns import LEGACY_COLUMNS, realign_public_horse_history
from modules.preprocessing import ResultsProcessor
from modules.preprocessing._results_processor import coalesce_result_columns, parse_finish_code
from modules.training import DataSplitter, FeatureSchemaError, align_to_schema, build_schema, split_race_ids_by_dates
from modules.training._win_data_splitter import WinDataSplitter


def raw_results():
    """旧名の列だけのレース(2020)と新名の列だけのレース(2019)。2019は行を丸ごと重複させる。"""
    old = pd.DataFrame({
        '着順': ['1', '2', '3(降)', '取消'], '枠番': [1, 2, 3, 4], '馬番': [1, 2, 3, 4], '斤量': [55, 55, 55, 55],
        '単勝': [2.0, 3.0, 9.0, None], '馬体重': ['450(0)', '460(+2)', '470(-2)', None], '性齢': ['牡3'] * 4,
        'horse_id': ['h1', 'h2', 'h3', 'h4'], 'jockey_id': ['j'] * 4, 'trainer_id': ['t'] * 4, 'owner_id': ['o'] * 4,
    }, index=['202001010101'] * 4)
    new = pd.DataFrame({
        '着順': ['1', '取', '除', '中'], '枠': [1, 2, 3, 4], '馬番': [1, 2, 3, 4], '斤量': [56, 56, 56, 56],
        '単勝オッズ': [1.5, 4.0, 8.0, 20.0], '馬体重(増減)': ['480(+4)', '490(0)', '500(-6)', '510(+2)'],
        '性齢': ['牝4'] * 4, 'horse_id': ['h5', 'h6', 'h7', 'h8'], 'jockey_id': ['j'] * 4,
        'trainer_id': ['t'] * 4, 'owner_id': ['o'] * 4,
    }, index=['201901010101'] * 4)
    return pd.concat([old, new, new])


class ResultsPreprocessTests(unittest.TestCase):
    def test_finish_codes(self):
        self.assertEqual(parse_finish_code('3'), 3)
        self.assertEqual(parse_finish_code('3(降)'), 3)
        for code in ('取消', '取', '除外', '除'):
            self.assertIsNone(parse_finish_code(code))
        for code in ('中止', '中', '失格', '失'):
            self.assertEqual(parse_finish_code(code), 0)

    def test_coalesce_fills_old_names_from_new(self):
        df = coalesce_result_columns(raw_results())
        self.assertEqual(df.loc['201901010101', '単勝'].tolist()[:4], [1.5, 4.0, 8.0, 20.0])
        self.assertEqual(df.loc['202001010101', '単勝'].tolist()[:3], [2.0, 3.0, 9.0])

    def test_processor_dedups_drops_withdrawn_and_keeps_values(self):
        out = ResultsProcessor(raw_data=raw_results()).preprocessed_data
        r19 = out[out['race_id'] == '201901010101']
        r20 = out[out['race_id'] == '202001010101']
        # 2019: 取・除 を除き、重複も除いて 2頭（1着と中止）
        self.assertEqual(sorted(r19['馬番'].tolist()), [1, 4])
        self.assertEqual(r19['win'].tolist(), [1, 0])
        self.assertFalse(r19['単勝'].isna().any())
        self.assertFalse(r19['体重'].isna().any())
        self.assertFalse(r19['枠番'].isna().any())
        self.assertEqual(set(r19['n_horses']), {2})
        # 2020: 取消を除く3頭。降着3着は複勝圏
        self.assertEqual(r20['rank'].tolist(), [1, 1, 1])


def featured(n_days=30, horses=3):
    rows = []
    for d in range(n_days):
        date = pd.Timestamp('2024-01-01') + pd.Timedelta(days=d)
        for h in range(horses):
            rows.append({'race_id': f'R{d:03d}', 'date': date, '馬番': h + 1, '単勝': 2.0 + h,
                         'x1': float(d + h), 'x2': float(h), 'rank': int(h == 0), 'win': int(h == 0)})
    return pd.DataFrame(rows)


class SplitTests(unittest.TestCase):
    DATES = {'train_start': '2024-01-01', 'valid_start': '2024-01-16', 'test_start': '2024-01-24', 'test_end': '2024-01-30'}

    def test_date_boundaries(self):
        fd = featured()
        per_race = fd.groupby('race_id')['date'].first()
        tr, va, te = split_race_ids_by_dates(per_race, self.DATES)
        self.assertEqual((len(tr), len(va), len(te)), (15, 8, 7))
        with self.assertRaises(ValueError):
            split_race_ids_by_dates(per_race, {**self.DATES, 'valid_start': '2024-01-30'})

    def test_x_train_excludes_valid_in_both_modes(self):
        fd = featured()
        for ds in (DataSplitter(fd, split_dates=self.DATES), DataSplitter(fd, test_size=0.2, valid_size=0.3)):
            train_races = set(fd.loc[ds.X_train.index, 'race_id'])
            valid_races = set(fd.loc[ds.X_valid.index, 'race_id'])
            self.assertTrue(train_races)
            self.assertFalse(train_races & valid_races)
            self.assertFalse(train_races & set(ds.test_race_ids))
            self.assertNotIn('単勝', ds.X_train.columns)
            self.assertNotIn('race_id', ds.X_train.columns)

    def test_win_splitter_x_train_excludes_valid(self):
        fd = featured()
        for ds in (WinDataSplitter(fd, split_dates=self.DATES), WinDataSplitter(fd, 0.2, 0.3)):
            self.assertFalse(set(fd.loc[ds.X_train.index, 'race_id']) & set(fd.loc[ds.X_valid.index, 'race_id']))


class SchemaTests(unittest.TestCase):
    def test_missing_column_stops_and_order_is_restored(self):
        X = pd.DataFrame({'a': [1.0, np.nan], 'b': [2.0, 3.0], 'c': pd.Categorical(['x', 'y'])})
        schema = build_schema(X)
        shuffled = X[['c', 'b', 'a']].assign(race_id=['r', 'r'])
        aligned = align_to_schema(shuffled, schema)
        self.assertEqual(list(aligned.columns), ['a', 'b', 'c'])
        self.assertTrue(np.isnan(aligned['a'].iloc[1]))  # NaN をゼロ埋めしない
        self.assertEqual(list(aligned['c'].cat.categories), ['x', 'y'])
        with self.assertRaises(FeatureSchemaError):
            align_to_schema(X[['a', 'c']], schema)


class HorseHistoryRealignTests(unittest.TestCase):
    def test_shifted_public_row_is_realigned(self):
        row = {c: None for c in ['日付', '着順', 'タイム', '着差', 'タイム指数', '通過', 'ペース', '上り', '馬体重',
                                 '厩舎コメント', '備考', '勝ち馬(2着馬)', '賞金', 'Unnamed:29', 'Unnamed:30',
                                 'Unnamed:31', 'Unnamed:32']}
        row.update({'日付': '2025/11/22', '着順': '1', 'タイム': '1:30.3', '着差': '0.5', 'タイム指数': '**',
                    '通過': '**', 'ペース': '**', '上り': '**', '馬体重': '**',
                    '厩舎コメント': '8-7-7-6', '備考': '35.1-34.9', '勝ち馬(2着馬)': '40.1', '賞金': '486(-4)',
                    'Unnamed:31': 'トウカイマシェリ', 'Unnamed:32': 750.0})
        out = realign_public_horse_history(pd.DataFrame([row], index=['h']))
        self.assertEqual(list(out.columns), LEGACY_COLUMNS)
        r = out.iloc[0]
        self.assertEqual((r['通過'], r['ペース'], r['上り'], r['馬体重']), ('8-7-7-6', '35.1-34.9', '40.1', '486(-4)'))
        self.assertEqual((r['勝ち馬(2着馬)'], r['賞金']), ('トウカイマシェリ', 750.0))
        self.assertIsNone(r['厩舎ｺﾒﾝﾄ'])

    def test_aligned_legacy_row_is_unchanged(self):
        row = {c: None for c in LEGACY_COLUMNS}
        row.update({'通過': '1-1', '馬体重': '450(0)', '賞金': 500.0})
        out = realign_public_horse_history(pd.DataFrame([row], index=['h']))
        self.assertEqual((out.iloc[0]['通過'], out.iloc[0]['馬体重'], out.iloc[0]['賞金']), ('1-1', '450(0)', 500.0))


if __name__ == '__main__':
    unittest.main()
