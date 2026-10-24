import re

import pandas as pd

from ._abstract_data_processor import AbstractDataProcessor
from modules.constants import ResultsCols as Cols

# raw results には同じ情報が旧名・新名の2つの列名で入っている（2015〜2019年と2026年の一部は新名のみ）。
# 旧名が欠損している行を新名の値で埋める。段階0監査: prompts/reports/20261024_stage0_audit.md
LEGACY_COLUMN_ALIASES = {
    Cols.TANSHO_ODDS: '単勝オッズ',
    Cols.WAKUBAN: '枠',
    Cols.WEIGHT_AND_DIFF: '馬体重(増減)',
    Cols.TRAINER: '厩舎',
}
# 発走していない馬（出走取消・競走除外）。学習・評価から除く
WITHDRAWN_CODES = {'取消', '取', '除外', '除'}


def coalesce_result_columns(df: pd.DataFrame) -> pd.DataFrame:
    """旧名の列が欠損している行を、新名の列の値で埋める。"""
    df = df.copy()
    for old, new in LEGACY_COLUMN_ALIASES.items():
        if new not in df.columns:
            continue
        if old not in df.columns:
            df[old] = df[new]
        else:
            df[old] = df[old].where(df[old].notna(), df[new])
    return df


def parse_finish_code(code) -> int | None:
    """着順の表記を数値にする。

    - 数字: その着順
    - 'N(降)': 降着後の着順 N
    - 取消・除外（短縮表記を含む）: None（発走していない）
    - 中止・失格など: 0（出走したが着順なし）
    """
    s = str(code).strip()
    if s.isdigit():
        return int(s)
    m = re.fullmatch(r'(\d+)\(降\)', s)
    if m:
        return int(m[1])
    if s in WITHDRAWN_CODES:
        return None
    return 0


class ResultsProcessor(AbstractDataProcessor):
    def __init__(self, filepath=None, raw_data=None):
        """
        初期処理
        """
        super().__init__(filepath, raw_data=raw_data)
    
    def _preprocess(self):
        """
        前処理
        """
        df = self.raw_data.copy()

        # 全列が一致する重複行（同じレースの行が繰り返し連結されたもの）を集約する
        df = df[~df.reset_index().duplicated().to_numpy()]
        # 旧名・新名の列を統合する
        df = coalesce_result_columns(df)

        # 着順の前処理
        df = self._preprocess_rank(df)
        
        # 性齢を性と年齢に分ける
        # サイト上のテーブルに存在する列名は、ResultsColsクラスで定数化している。
        df["性"] = df[Cols.SEX_AGE].map(lambda x: str(x)[0])
        # 年齢の変換時にエラーが発生する場合は欠損値にする
        df["年齢"] = pd.to_numeric(df[Cols.SEX_AGE].map(lambda x: str(x)[1:]), errors='coerce')

        # 馬体重を体重と体重変化に分ける
        df["体重"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[0]
        df["体重変化"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[1].str[:-1]
        
        # errors='coerce'で、"計不"など変換できない時に欠損値にする
        df['体重'] = pd.to_numeric(df['体重'], errors='coerce')
        df['体重変化'] = pd.to_numeric(df['体重変化'], errors='coerce')

        # 各列を数値型に変換（errors='coerce'で不正データを欠損値に変換）
        df[Cols.TANSHO_ODDS] = pd.to_numeric(df[Cols.TANSHO_ODDS], errors='coerce')
        df[Cols.KINRYO] = pd.to_numeric(df[Cols.KINRYO], errors='coerce')
        df[Cols.WAKUBAN] = pd.to_numeric(df[Cols.WAKUBAN], errors='coerce')
        df[Cols.UMABAN] = pd.to_numeric(df[Cols.UMABAN], errors='coerce')
        
        # 6/6出走数追加
        df['n_horses'] = df.index.map(df.index.value_counts())
        # race_id列をインデックスからコピーして保持する
        df['race_id'] = df.index.astype(str)
        
        # カラム抽出
        df = self._select_columns(df)

        # 馬番順にソート
        df = self._sort(df)

        # --- 重複除去（重要） ---
        # 1レース内で (race_id, umaban) が重複すると、下流のマージ/集計で
        # 行が爆発（例: 18×18=324）して学習・評価が壊れる。
        # 基本的に 1レース内の馬番は一意なので、ここで強制的に一意化する。
        before = len(df)
        df = df.drop_duplicates(subset=['race_id', Cols.UMABAN], keep='first')
        after = len(df)
        if after != before:
            print(f"[ResultsProcessor] dropped duplicated rows: {before - after:,} (from {before:,} to {after:,})")

        # n_horses を重複除去後の値で再計算
        df['n_horses'] = df.index.map(df.index.value_counts())
        
        return df
        
        
    def _preprocess_rank(self, raw):
        """
        着順の前処理
        """
        df = raw.copy()
        # 取消・除外（短縮表記を含む）は発走していないので取り除く。
        # 降着は降着後の着順、中止・失格は0（着外）にする。
        finish = df[Cols.RANK].map(parse_finish_code)
        df = df[finish.notna()].copy()
        df[Cols.RANK] = finish[finish.notna()].astype(int)
        # place（複勝圏=3着以内）
        df['rank'] = df[Cols.RANK].map(lambda x: 1 if x > 0 and x < 4 else 0)
        # win（1着）
        df['win'] = df[Cols.RANK].map(lambda x: 1 if x == 1 else 0)
        return df

    def _sort(self, raw):
        """
        各レースを馬番順にソートする。
        ※ 各レース内のソート。レースの順序自体はrace_idの名前順になる。
        """
        df = raw.copy()
        df = df.reset_index().sort_values(['index', Cols.UMABAN]).set_index('index')
        df.index.name = None
        # NOTE:
        # df.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values(Cols.UMABAN))
        # と同じ。groupbyを使う方がやや遅い。
        return df
    
    def _select_columns(self, raw):
        """
        カラム抽出
        """
        df = raw.copy()[[
            #Cols.RANK, # 着順
            Cols.WAKUBAN, # 枠番
            Cols.UMABAN, # 馬番
            #Cols.HORSE_NAME, # 馬名
            #Cols.SEX_AGE, # 性齢
            Cols.KINRYO, # 斤量
            #Cols.JOCKEY, # 騎手
            #Cols.TIME # タイム
            #Cols.RANK_DIFF # 着差
            Cols.TANSHO_ODDS, # 単勝
            #Cols.POPULARITY, # 人気
            #Cols.WEIGHT_AND_DIFF, # 馬体重
            #Cols.TRAINER, # 調教師
            'race_id',
            'horse_id',
            'jockey_id',
            'trainer_id',
            'owner_id',
            '性',
            '年齢',
            '体重',
            '体重変化',
            'n_horses',
            'rank',
            'win'
            ]]
        return df
