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
        
        # 開催場所（race_id から安定的に作る）
        # ※ _clean_umaban で index を落とさない前提
        if 'race_id' in df.columns:
            df['開催'] = df['race_id'].astype(str).str[4:6]
        else:
            df['開催'] = df.index.map(lambda x: str(x)[4:6])
        
        # 日付型に変更
        df["date"] = pd.to_datetime(df["date"])
        
        return df
    
    def _clean_umaban(self, df):
        """
        馬番が数値（1-18の範囲）でないレコードを除去
        拡張版：より詳細なログとエラーハンドリング
        """
        def is_valid_umaban(umaban):
            """
            馬番の有効性を検証する拡張関数
            """
            try:
                if pd.isna(umaban):
                    return False, "NaN値"
                
                # 文字列に変換して前後の空白を除去
                str_umaban = str(umaban).strip()
                
                # 空文字や'None'文字列をチェック
                if str_umaban == '' or str_umaban.lower() == 'none':
                    return False, "空文字またはNone"
                    
                # 取消を示すキーワードをチェック
                cancel_keywords = ['取消', '除外', '--', 'キャンセル', 'cancel']
                if any(keyword in str_umaban for keyword in cancel_keywords):
                    return False, f"取消キーワード: {str_umaban}"
                
                # 数値に変換
                try:
                    num = int(str_umaban)
                except ValueError:
                    return False, f"数値変換不可: {str_umaban}"
                
                # 1-18の範囲チェック
                if not (1 <= num <= 18):
                    return False, f"範囲外: {num}"
                    
                return True, "有効"
                
            except Exception as e:
                return False, f"予期しないエラー: {str(e)}"
        
        print(f"ShutubaTableProcessor: 馬番クリーンアップ開始（{len(df)}件のレコード）")
        
        # 各馬番の検証結果を取得
        validation_results = df['馬番'].apply(lambda x: is_valid_umaban(x))
        valid_mask = validation_results.apply(lambda x: x[0])
        
        # 無効なレコード数をログ出力
        invalid_count = (~valid_mask).sum()
        
        if invalid_count > 0:
            print(f"ShutubaTableProcessor: {invalid_count}件の不正な馬番レコードを除去しました")
            
            # 詳細なエラー情報を出力
            invalid_data = df[~valid_mask]
            for idx, (_, record) in enumerate(invalid_data.iterrows()):
                validation_result = validation_results.iloc[~valid_mask.values][idx]
                print(f"  除去レコード {idx+1}: 馬番='{record['馬番']}' -> {validation_result[1]}")
        else:
            print("ShutubaTableProcessor: すべての馬番が有効です")
        
        # 有効なレコードのみ返す（index=race_id を維持する）
        cleaned_df = df[valid_mask].copy()
        
        if len(cleaned_df) == 0:
            print("警告: 有効な馬番のレコードが0件になりました。データに問題がある可能性があります。")
        
        return cleaned_df
    
    def _preprocess_rank(self, raw):
        return raw
    
    def _select_columns(self, raw):
        # 利用可能な列のみ選択
        required_cols = [
            Cols.WAKUBAN, # 枠番
            Cols.UMABAN, # 馬番
            Cols.KINRYO, # 斤量
            Cols.TANSHO_ODDS, # 単勝
            'race_id',
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

