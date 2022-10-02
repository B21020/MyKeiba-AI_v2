from abc import ABCMeta, abstractstaticmethod
from typing import Callable

import pandas as pd

from modules.constants import ResultsCols


# const
SCORE = 'score'


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    score_table = X[[ResultsCols.UMABAN, ResultsCols.TANSHO_ODDS]].copy()
    score = model.predict_proba(X.drop([ResultsCols.TANSHO_ODDS], axis=1))[:, 1]
    score_table[SCORE] = score
    return score_table

def _apply_scaler(score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    return score.groupby(level=0).apply(scaler)


# scalers
_scaler_standard = lambda x: (x - x.mean()) / x.std(ddof=0)


# policies
class AbstractScorePolicy(metaclass=ABCMeta):
    @abstractstaticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

class BasicScorePolicy(AbstractScorePolicy):
    """
    LightGBMの出力をそのままscoreとして計算。
    """
    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        return _calc(model, X)

class StdScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価する。「レース内偏差値」のようなもの。
    """
    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score_table[SCORE] = _apply_scaler(score_table[SCORE], _scaler_standard)
        return score_table

class MinMaxScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価した後、全体を0~1にスケーリング。
    """
    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score = _apply_scaler(score_table[SCORE], _scaler_standard)
        # データ全体で0~1にスケーリング
        min_ = score.min()
        score_table[SCORE] = (score - min_) / (score.max() - min_)
        return score_table
