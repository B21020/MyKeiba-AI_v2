#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import sys
import os

# modules を sys.path に追加
sys.path.insert(0, os.path.abspath('.'))

from modules.preprocessing import ShutubaTableProcessor

# テストを実行
try:
    # 一時的なテストデータを作成
    test_data = pd.DataFrame({
        '性齢': ['牡3', '牝4', 'an5'],  # 'an5' が問題のあるデータ
        '馬体重': ['500(+10)', '450(-5)', '計不'],
        '単勝': ['1.2', '3.4', '5.6'],
        '斤量': ['57.0', '54.0', '56.0'],
        '枠番': ['1', '2', '3'],
        '馬番': ['1', '2', '3']
    })
    
    # pickleファイルとして保存
    test_filepath = 'test_data.pickle'
    test_data.to_pickle(test_filepath)
    
    print("テストデータを作成しました:")
    print(test_data)
    print()
    
    # ShutubaTableProcessorでテスト
    print("ShutubaTableProcessorでテスト中...")
    processor = ShutubaTableProcessor(test_filepath)
    print("成功: エラーなく処理されました")
    print("処理結果:")
    print(processor.preprocessed_data)
    
    # テストファイルを削除
    os.remove(test_filepath)
    
except Exception as e:
    print(f"エラーが発生しました: {e}")
    import traceback
    traceback.print_exc()
    
    # テストファイルを削除
    if os.path.exists(test_filepath):
        os.remove(test_filepath)