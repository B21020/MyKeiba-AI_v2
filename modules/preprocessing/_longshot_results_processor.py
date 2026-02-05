import pandas as pd

from modules.constants import ResultsCols as Cols
from ._results_processor import ResultsProcessor


class LongshotResultsProcessor(ResultsProcessor):
    """穴馬(単勝>=15倍)×入着(3着内)ラベル用の ResultsProcessor。

    既存の `ResultsProcessor` (3着内=1/それ以外=0 の `rank`) を壊さずに、
    追加で `longshot` ラベル列を生成する。

    longshot = 1  <=>  (単勝オッズ >= 15) かつ (3着内)

    注意:
    - 単勝オッズは確定オッズ（結果テーブルの `単勝` 列）を利用する
    - 取消("取消")は既存と同様に除外
    """

    def _preprocess(self):
        df = super()._preprocess()

        # `rank` は ResultsProcessor が 3着内=1 の二値を作成済み
        odds = pd.to_numeric(df.get(Cols.TANSHO_ODDS), errors='coerce')
        in_place = df.get('rank') == 1

        df['longshot'] = ((odds >= 15) & in_place).astype(int)
        return df
