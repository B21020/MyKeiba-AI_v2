import pandas as pd
import re

from ._abstract_data_processor import AbstractDataProcessor
from modules.constants import Master
from modules.constants import HorseResultsCols as Cols


class HorseResultsProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        super().__init__(filepath)
    
    def _preprocess(self):
        df = self.raw_data

        # 不要な行を削除する
        # タイムが「x:xx.x」以外の行
        df = df.dropna(subset=[Cols.TIME])
        df = df[df[Cols.TIME].str.contains('\d{1}:\d{2}\.\d{1}')]
        
        # 着順に数字以外の文字列が含まれているものは、欠損値（NaN）に置き換える
        # サイト上のテーブルに存在する列名は、HorseResultsColsクラスで定数化している。
        df[Cols.RANK] = pd.to_numeric(df[Cols.RANK], errors='coerce')
        # 着順が欠損値（NaN）となったものを取り除く
        df.dropna(subset=[Cols.RANK], inplace=True)
        df[Cols.RANK] = df[Cols.RANK].astype(int)

        # 日付をdatetime型に設定
        df["date"] = pd.to_datetime(df[Cols.DATE])
        df.drop([Cols.DATE], axis=1, inplace=True)
        
        # 賞金のNaNを0で埋める
        df[Cols.PRIZE].fillna(0, inplace=True)
        
        # 1着の着差を0にする（xが0より小さい場合は、0、xが0以上の場合、xを返す）
        df[Cols.RANK_DIFF] = df[Cols.RANK_DIFF].map(lambda x: 0 if x<0 else x)
        
        # レース展開データ
        # n=1: 最初のコーナー位置, n=4: 最終コーナー位置
        def corner(x, n):
            if type(x) != str:
                return x
            elif n==4:
                return int(re.findall(r'\d+', x)[-1])
            elif n==1:
                return int(re.findall(r'\d+', x)[0])

        df['first_corner'] = df[Cols.CORNER].map(lambda x: corner(x, 1))
        df['final_corner'] = df[Cols.CORNER].map(lambda x: corner(x, 4))
        
        df['final_to_rank'] = df['final_corner'] - df[Cols.RANK]
        df['first_to_rank'] = df['first_corner'] - df[Cols.RANK]
        df['first_to_final'] = df['first_corner'] - df['final_corner']
        
        # 開催場所（数字以外の文字列を抽出）中央開催・地方開催・海外開催以外をその他（'99'）とする
        df[Cols.PLACE] = df[Cols.PLACE].str.extract(r'(\D+)')[0].map(Master.PLACE_DICT).fillna('99')
        
        # race_type（数字以外の文字列を抽出）
        df['race_type'] = df[Cols.RACE_TYPE_COURSE_LEN].str.extract(r'(\D+)')[0].map(Master.RACE_TYPE_DICT)
        # 距離は10の位を切り捨てる
        df['course_len'] = df[Cols.RACE_TYPE_COURSE_LEN].str.extract(r'(\d+)').astype(float) // 100
        # 距離列を削除
        df.drop([Cols.RACE_TYPE_COURSE_LEN], axis=1, inplace=True)
        
        # タイムの値を秒単位に変換
        base_time = pd.to_datetime("00:00.0", format="%M:%S.%f")
        df['time_seconds'] = pd.to_datetime(df[Cols.TIME], format="%M:%S.%f") - base_time
        df['time_seconds'] = df['time_seconds'].dt.total_seconds()
        # タイム列を削除
        df.drop([Cols.TIME], axis=1, inplace=True)

        # インデックス名を与える
        df.index.name = 'horse_id'
        
        return df
