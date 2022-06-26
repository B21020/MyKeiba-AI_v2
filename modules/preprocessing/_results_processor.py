import pandas as pd

from ._abstract_data_processor import AbstractDataProcessor
from modules.constants import ResultsCols as Cols


class ResultsProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        super().__init__(filepath)
    
    def _preprocess(self):
        df = self.raw_data.copy()
        
        df = self._preprocess_rank(df)
        
        # 性齢を性と年齢に分ける
        # サイト上のテーブルに存在する列名は、ResultsColsクラスで定数化している。
        df["性"] = df[Cols.SEX_AGE].map(lambda x: str(x)[0])
        df["年齢"] = df[Cols.SEX_AGE].map(lambda x: str(x)[1:]).astype(int)

        # 馬体重を体重と体重変化に分ける
        df["体重"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[0]
        df["体重変化"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[1].str[:-1]
        
        #errors='coerce'で、"計不"など変換できない時に欠損値にする
        df['体重'] = pd.to_numeric(df['体重'], errors='coerce')
        df['体重変化'] = pd.to_numeric(df['体重変化'], errors='coerce')

        # 各列を数値型に変換
        df[Cols.TANSHO_ODDS] = df[Cols.TANSHO_ODDS].astype(float)
        df[Cols.KINRYO] = df[Cols.KINRYO].astype(float)
        df[Cols.WAKUBAN] = df[Cols.WAKUBAN].astype(int)
        df[Cols.UMABAN] = df[Cols.UMABAN].astype(int)
        
        #6/6出走数追加
        df['n_horses'] = df.index.map(df.index.value_counts())
        
        df = self._select_columns(df)
        
        return df
        
        
    def _preprocess_rank(self, raw):
        df = raw.copy()
        # 着順に数字以外の文字列が含まれているものを取り除く
        df[Cols.RANK] = pd.to_numeric(df[Cols.RANK], errors='coerce')
        df.dropna(subset=[Cols.RANK], inplace=True)
        df[Cols.RANK] = df[Cols.RANK].astype(int)
        df['rank'] = df[Cols.RANK].map(lambda x:1 if x<4 else 0)
        return df
    
    def _select_columns(self, raw):
        df = raw.copy()[[
            Cols.WAKUBAN,Cols.UMABAN,Cols.KINRYO,Cols.TANSHO_ODDS,'horse_id','jockey_id',
            '性', '年齢','体重','体重変化','n_horses', 'rank'
            ]]
        return df
