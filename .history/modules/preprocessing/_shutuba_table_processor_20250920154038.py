import pandas as pd
from ._results_processor import ResultsProcessor
from modules.constants import ResultsCols as Cols

class ShutubaTableProcessor(ResultsProcessor):
    def __init__(self, filepath: str):
        super().__init__(filepath)

    def _preprocess(self):
        df = super()._preprocess()
        
        # 距離は10の位を切り捨てる
        df["course_len"] = df["course_len"].astype(float) // 100
        
        # 開催場所
        df['開催'] = df.index.map(lambda x:str(x)[4:6])
        
        # 日付型に変更
        df["date"] = pd.to_datetime(df["date"])
        
        # 数値型の確認と修正（予測時の型エラーを防ぐ）
        from modules.constants import ResultsCols as Cols
        
        # 馬番と枠番を確実に数値型に変換
        if Cols.UMABAN in df.columns:
            df[Cols.UMABAN] = pd.to_numeric(df[Cols.UMABAN], errors='coerce').astype('Int64')
        if Cols.WAKUBAN in df.columns:
            df[Cols.WAKUBAN] = pd.to_numeric(df[Cols.WAKUBAN], errors='coerce').astype('Int64')
            
        return df
    
    def _preprocess_rank(self, raw):
        return raw
    
    def _select_columns(self, raw):
        df = raw.copy()[[\
            Cols.WAKUBAN, # 枠番
            Cols.UMABAN, # 馬番
            Cols.KINRYO, # 斤量
            Cols.TANSHO_ODDS, # 単勝
            'horse_id',
            'jockey_id',
            'trainer_id',
            '性',
            '年齢',
            '体重',
            '体重変化',
            'n_horses',
            'course_len',
            'weather',
            'race_type',
            'ground_state',
            'date',
            'around',
            'race_class'
            ]]
        return df

