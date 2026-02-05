import pandas as pd

from modules.constants import ResultsCols as Cols
from ._results_processor import ResultsProcessor


class WinResultsProcessor(ResultsProcessor):
    """単勝(1着)ラベル用の ResultsProcessor。

    既存の `ResultsProcessor` (3着内=1/それ以外=0 の `rank`) を壊さずに、
    追加で 1着=1/それ以外=0 の `win` ラベル列を生成する。

    - 取消("取消")は既存と同様に除外
    - 数字以外は 0 扱い（既存と同様）
    """

    def _preprocess_rank(self, raw):
        df = super()._preprocess_rank(raw)
        df['win'] = (df[Cols.RANK] == 1).astype(int)
        return df

    def _select_columns(self, raw):
        df = raw.copy()[[
            Cols.WAKUBAN,  # 枠番
            Cols.UMABAN,  # 馬番
            Cols.KINRYO,  # 斤量
            Cols.TANSHO_ODDS,  # 単勝
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
            'win',
        ]]
        return df
