import pandas as pd
from ._results_processor import ResultsProcessor
from modules.constants import ResultsCols as Cols

class ShutubaTableProcessor(ResultsProcessor):
    def __init__(self, filepath: str):
        super().__init__(filepath)

    def _preprocess(self):
        df = super()._preprocess()
        
        # 馬番のクリーンアップを最初に実行（course_len処理より前）
        df = self._clean_umaban(df)
        
        # 距離は10の位を切り捨てる（不正データは0に変換）
        df["course_len"] = pd.to_numeric(df["course_len"], errors='coerce').fillna(0) // 100
        
        # 開催場所
        df['開催'] = df.index.map(lambda x:str(x)[4:6])
        
        # 日付型に変更
        df["date"] = pd.to_datetime(df["date"])
        
        return df
    
    def _clean_umaban(self, df):
        """
        馬番が数値（1-18の範囲）でないレコードを除去
        """
        def is_valid_umaban(umaban):
            try:
                if pd.isna(umaban) or str(umaban).strip() == '':
                    return False
                num = int(umaban)
                return 1 <= num <= 18
            except (ValueError, TypeError):
                return False
        
        # 有効な馬番のマスク
        valid_mask = df['馬番'].apply(is_valid_umaban)
        
        # 無効なレコード数をログ出力
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            print(f"ShutubaTableProcessor: {invalid_count}件の不正な馬番レコードを除去しました")
            invalid_umaban = df[~valid_mask]['馬番'].tolist()
            print(f"除去された馬番: {invalid_umaban}")
        
        # 有効なレコードのみ返す
        return df[valid_mask].copy().reset_index(drop=True)
    
    def _preprocess_rank(self, raw):
        return raw
    
    def _select_columns(self, raw):
        # 利用可能な列のみ選択
        required_cols = [
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
        ]
        
        # 存在する列のみ選択
        available_cols = [col for col in required_cols if col in raw.columns]
        df = raw.copy()[available_cols]
        return df

