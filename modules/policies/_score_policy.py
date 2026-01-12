from abc import ABCMeta, abstractmethod
from typing import Callable

import pandas as pd

from modules.constants import ResultsCols
import numpy as np
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_datetime64tz_dtype,
    is_timedelta64_dtype,
    is_bool_dtype,
)
import warnings


# const
_SCORE = 'score'


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    # メタ列（例: race_id, 馬番）は結果用に保持し、予測には使わない
    uma = X[ResultsCols.UMABAN] if ResultsCols.UMABAN in X.columns else None
    race = X['race_id'] if 'race_id' in X.columns else None

    score_table = pd.DataFrame(index=X.index)
    if race is not None:
        score_table['race_id'] = race
    if uma is not None:
        score_table[ResultsCols.UMABAN] = uma

    # 特徴量として使う列を決定（メタ列を除外）
    meta_cols = []
    if ResultsCols.UMABAN in X.columns:
        meta_cols.append(ResultsCols.UMABAN)
    if 'race_id' in X.columns:
        meta_cols.append('race_id')
    x_cols = [c for c in X.columns if c not in meta_cols]
    # モデルが期待する特徴量数を取得
    n_model = getattr(model, 'n_features_', None)
    if n_model is None:
        booster = getattr(model, 'booster_', None)
        if booster is not None:
            try:
                n_model = booster.num_feature()
            except Exception:
                n_model = None

    # 互換性対応:
    # 過去に UMABAN も含めて学習したモデルでは
    #   ・学習時の特徴量数 == 入力Xの列数
    # となっているため、その場合は UMABAN も特徴量として残す
    # ただし race_id は常に特徴量から除外する
    if n_model is not None and n_model == X.shape[1] and ResultsCols.UMABAN in X.columns:
        x_cols = [c for c in X.columns if c != 'race_id']

    # モデルが学習した列に整列（不足は0埋め、余剰は落とす）
    # ToDo4: KeibaAI側で注入する学習時列（mykeiba_feature_columns_）を最優先で使う
    mykeiba_cols = getattr(model, 'mykeiba_feature_columns_', None)

    feature_names = getattr(model, 'feature_name_', None)
    if feature_names is None:
        try:
            feature_names = model.booster_.feature_name()
        except Exception:
            feature_names = None

    # モデルの期待特徴量数
    n_model = getattr(model, 'n_features_in_', None)
    if n_model is None:
        n_model = getattr(model, 'n_features_', None)
    if n_model is None and feature_names is not None:
        try:
            n_model = len(feature_names)
        except Exception:
            n_model = None
    if n_model is None:
        booster = getattr(model, 'booster_', None)
        if booster is not None:
            try:
                n_model = booster.num_feature()
            except Exception:
                n_model = None

    # 予測入力の作り方を選ぶ
    # - 列名の一致が十分 → 学習時列順でreindex
    # - 一致がほぼ無い/少ない → Xの列順を使って numpy 配列で shape だけ合わせる（全0化を避ける）
    selected_name_cols = None
    overlap = 0

    # 1) MyKeibaの学習時列（あれば最優先）
    if isinstance(mykeiba_cols, (list, tuple)) and len(mykeiba_cols) > 0:
        try:
            overlap_my = len(pd.Index(mykeiba_cols).intersection(x_cols))
        except Exception:
            overlap_my = 0
        if overlap_my > 0:
            selected_name_cols = list(mykeiba_cols)
            overlap = overlap_my

    # 2) fallback: LightGBM/boosterのfeature名
    if selected_name_cols is None and feature_names is not None:
        try:
            overlap_fn = len(pd.Index(feature_names).intersection(x_cols))
            if overlap_fn >= max(1, int(0.5 * len(feature_names))):
                selected_name_cols = list(feature_names)
                overlap = overlap_fn
        except Exception:
            pass

    if selected_name_cols is not None:
        X_model = X.reindex(columns=selected_name_cols, fill_value=0)
        X_matrix_mode = False
    else:
        # 列名が合っていない可能性が高い
        # → Xの情報を捨てないように、数値行列としてモデルに渡す（必要ならpad/truncate）
        X_model = X.reindex(columns=x_cols, fill_value=0)
        X_matrix_mode = True
    
    # 型の安全化（datetime/timedelta→数値、bool→int、inf/NaN対策）
    # ⚠️ category型は変換しない（LightGBMが学習時のcategory情報を必要とするため）
    for col in X_model.columns:
        s = X_model[col]
        
        # category型はそのまま保持（LightGBMが内部で処理する）
        if getattr(s.dtype, 'name', '') == 'category':
            continue

        # datetime は float に直接 cast できないので、epoch秒へ変換
        if is_datetime64tz_dtype(s) or is_datetime64_any_dtype(s):
            dt = pd.to_datetime(s, errors='coerce')
            # NaT は int64 の最小値になるため NaN に戻す
            i64 = dt.view('int64').astype('float64')
            i64[i64 == float(np.iinfo('int64').min)] = np.nan
            X_model[col] = i64 / 1e9  # ns -> seconds
            continue

        # timedelta も同様に秒へ
        if is_timedelta64_dtype(s):
            td = pd.to_timedelta(s, errors='coerce')
            i64 = td.view('int64').astype('float64')
            i64[i64 == float(np.iinfo('int64').min)] = np.nan
            X_model[col] = i64 / 1e9
            continue

        # bool は 0/1 へ
        if is_bool_dtype(s):
            X_model[col] = s.astype('int64')

    # float化: category型以外のカラムのみ
    non_cat_cols = [col for col in X_model.columns if X_model[col].dtype.name != 'category']
    if non_cat_cols:
        X_model[non_cat_cols] = X_model[non_cat_cols].astype(float).replace([np.inf, -np.inf], 0).fillna(0)

    # 予測
    if not X_matrix_mode:
        score = model.predict_proba(X_model)[:, 1]
    else:
        mat = X_model.to_numpy(dtype=float, copy=False)
        if n_model is not None:
            if mat.shape[1] > n_model:
                mat = mat[:, :n_model]
            elif mat.shape[1] < n_model:
                pad = np.zeros((mat.shape[0], n_model - mat.shape[1]), dtype=float)
                mat = np.concatenate([mat, pad], axis=1)
        # 行が全て同じだと予測も同一になりやすいので警告
        if mat.shape[0] >= 2 and np.allclose(mat, mat[0], equal_nan=True):
            warnings.warn(
                'All rows in prediction features are identical. Scores will be identical within the race. '
                'This usually indicates feature engineering/merge issues or severe feature-name mismatch.',
                RuntimeWarning,
            )
        score = model.predict_proba(mat)[:, 1]

    if feature_names is not None and selected_name_cols is None:
        warnings.warn(
            f'Low overlap between model feature names and X columns (overlap={overlap}). '
            'Falling back to matrix-based prediction; verify that training/prediction feature columns match.',
            RuntimeWarning,
        )
    score_table[_SCORE] = score
    return score_table

def _group_keys_for_race(score_table: pd.DataFrame, score: pd.Series):
    if isinstance(score_table, pd.DataFrame) and 'race_id' in score_table.columns:
        return score_table['race_id']
    if getattr(score.index, 'nlevels', 1) > 1:
        return score.index.get_level_values(0)
    return None


def _apply_scaler(score_table: pd.DataFrame, score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    keys = _group_keys_for_race(score_table, score)
    if keys is None:
        return scaler(score)
    return score.groupby(keys, group_keys=False).apply(scaler)


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
        s = score_table[_SCORE]

        keys = _group_keys_for_race(score_table, s)
        if keys is None:
            # グルーピングキーが無い場合は全体を1グループとして標準化
            mean_val = s.mean()
            std_val = s.std(ddof=0)
            if np.isfinite(std_val) and std_val > 0:
                score_table[_SCORE] = ((s - mean_val) / std_val).astype(float)
            else:
                score_table[_SCORE] = s.astype(float)
            return score_table

        g = s.groupby(keys)
        mean_ = g.transform('mean')
        std_ = g.transform('std')
        nuniq = g.transform('nunique')
        
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
        score = _apply_scaler(score_table, score_table[_SCORE], _scaler_standard)
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
        score_table[_SCORE] = _apply_scaler(score_table, score_table[_SCORE], _scaler_relative_proba)
        return score_table
