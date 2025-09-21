from abc import ABCMeta, abstractmethod
from typing import Callable

import pandas as pd

from modules.constants import ResultsCols
import numpy as np


# const
_SCORE = 'score'


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    # メタ列（例: 馬番）は結果用に保持し、予測には使わない
    uma = X[ResultsCols.UMABAN] if ResultsCols.UMABAN in X.columns else None
    score_table = pd.DataFrame(index=X.index)
    if uma is not None:
        score_table[ResultsCols.UMABAN] = uma

    # モデルが学習した列に強制整列（不足は0埋め、余剰は落とす）
    feature_names = getattr(model, 'feature_name_', None)
    if feature_names is None:
        try:
            feature_names = model.booster_.feature_name()
        except Exception:
            feature_names = None

    # モデル由来の列名とXの交差が小さい（例: f0,f1…のみなど）場合はXの列を採用
    x_cols = [c for c in X.columns if c != ResultsCols.UMABAN]
    if feature_names is None:
        selected_cols = x_cols
    else:
        inter = list(pd.Index(feature_names).intersection(x_cols))
        # 交差が0、または交差率が0.5未満ならXの列を優先（ノートブック側で学習列順に整列済み想定）
        if len(inter) == 0 or len(inter) / max(1, len(feature_names)) < 0.5:
            selected_cols = x_cols
        else:
            selected_cols = feature_names

    # 予測に使う入力を作成
    X_model = X.reindex(columns=selected_cols, fill_value=0)
    # 型の安全化（カテゴリ→コード、float化、inf/NaN対策）
    for col in X_model.columns:
        if getattr(X_model[col].dtype, 'name', '') == 'category':
            X_model[col] = X_model[col].cat.codes
    X_model = X_model.astype(float).replace([np.inf, -np.inf], 0).fillna(0)

    # 予測
    score = model.predict_proba(X_model)[:, 1]
    score_table[_SCORE] = score
    return score_table

def _apply_scaler(score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    return score.groupby(level=0, group_keys=False).apply(scaler)


# scalers
def _scaler_standard(x: pd.Series) -> pd.Series:
    m = x.mean()
    s = x.std(ddof=0)
    if not np.isfinite(s) or s == 0:
        # 全て同値・分散0の場合は標準化せず、そのまま返す（NaN回避）
        return x.fillna(0)
    z = (x - m) / s
    return z.replace([np.inf, -np.inf], 0).fillna(0)
_scaler_relative_proba = lambda x: x / x.sum()


# policies
class AbstractScorePolicy(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
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
        # 安全な標準化（分散0や全同一値でNaN回避）
        s = score_table[_SCORE]
        g = s.groupby(level=0)
        mean_ = g.transform('mean')
        std_ = g.transform('std')
        nuniq = g.transform('nunique')
        
        # 手動テストで確認したロジックを正確に実装
        std_replaced = std_.replace(0, np.nan)
        z = (s - mean_) / std_replaced
        z_finite = z.where(np.isfinite(z), s)
        z_final = z_finite.where(nuniq > 1, s).fillna(0)
        
        score_table[_SCORE] = z_final.astype(float)
        return score_table

class MinMaxScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価した後、全体を0~1にスケーリング。
    """
    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score = _apply_scaler(score_table[_SCORE], _scaler_standard)
        # データ全体で0~1にスケーリング
        min_ = score.min()
        score_table[_SCORE] = (score - min_) / (score.max() - min_)
        return score_table

class RelativeProbaScorePolicy(AbstractScorePolicy):
    """
    レース内での相対確率。
    """
    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを相対確率化
        score_table[_SCORE] = _apply_scaler(score_table[_SCORE], _scaler_relative_proba)
        return score_table
