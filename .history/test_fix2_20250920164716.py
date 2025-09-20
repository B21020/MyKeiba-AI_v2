#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sys
import os

# modules を sys.path に追加
sys.path.insert(0, os.path.abspath('.'))

from modules.preprocessing._results_processor import ResultsProcessor
from modules.constants import ResultsCols as Cols

# テストを実行
try:
    # 最小限のテストデータを作成（ResultsProcessorのテスト用）
    test_data = pd.DataFrame({
        Cols.SEX_AGE: ['牡3', '牝4', 'an5', 'ｱﾝ2'],  # 'an5' と 'ｱﾝ2' が問題のあるデータ
        Cols.WEIGHT_AND_DIFF: ['500(+10)', '450(-5)', '計不', '480(0)'],
        Cols.TANSHO_ODDS: ['1.2', '3.4', '5.6', 'abc'],  # 'abc' が問題データ
        Cols.KINRYO: ['57.0', '54.0', '56.0', 'xx'],   # 'xx' が問題データ
        Cols.WAKUBAN: ['1', '2', '3', 'y'],             # 'y' が問題データ
        Cols.UMABAN: ['1', '2', '3', 'z'],              # 'z' が問題データ
        Cols.RANK: ['1', '2', '3', '4']
    })
    
    # インデックスを設定
    test_data.index = ['race1', 'race1', 'race1', 'race1']
    
    # pickleファイルとして保存
    test_filepath = 'test_data_minimal.pickle'
    test_data.to_pickle(test_filepath)
    
    print("テストデータを作成しました:")
    print(test_data)
    print()
    
    # カスタムテストクラスを作成
    class TestResultsProcessor(ResultsProcessor):
        def _select_columns(self, raw):
            # 最小限の列のみ選択
            return raw[['性', '年齢', '体重', '体重変化']].copy()
    
    # TestResultsProcessorでテスト
    print("TestResultsProcessorでテスト中...")
    processor = TestResultsProcessor(test_filepath)
    print("成功: エラーなく処理されました")
    print("処理結果:")
    print(processor.preprocessed_data)
    print("データ型:")
    print(processor.preprocessed_data.dtypes)
    
    # テストファイルを削除
    os.remove(test_filepath)
    
except Exception as e:
    print(f"エラーが発生しました: {e}")
    import traceback
    traceback.print_exc()
    
    # テストファイルを削除
    if os.path.exists(test_filepath):
        os.remove(test_filepath)