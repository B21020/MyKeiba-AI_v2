import lightgbm as lgb
import pandas as pd

try:
    import optuna.integration.lightgbm as lgb_o
except ModuleNotFoundError:
    lgb_o = None

from modules.constants import ResultsCols
from modules.training._data_splitter import split_race_ids_by_dates


class WinDataSplitter:
    """`win`(1着=1/それ以外=0) を目的変数にする DataSplitter。

    既存の `DataSplitter` は `rank` 固定のため、既存コードを崩さず
    単勝特化モデル用に同等のインターフェースを持つ Splitter を追加する。
    """

    def __init__(self, featured_data, test_size=None, valid_size=None, split_dates: dict | None = None) -> None:
        """split_dates を渡すと日付の境界で分割する（DataSplitter と同じキー）。"""
        if split_dates is None and (test_size is None or valid_size is None):
            raise ValueError('Either split_dates or both test_size and valid_size must be given')
        self.__featured_data = featured_data
        self.__split_dates = split_dates
        self.__lgb_train_optuna = None
        self.__lgb_valid_optuna = None
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        if self.__split_dates is not None:
            df = self.__featured_data
            race_keys = df['race_id'] if 'race_id' in df.columns else self.__get_race_keys(df.index)
            date_per_race = df['date'].groupby(race_keys).first()
            fit_ids, valid_ids, test_ids = split_race_ids_by_dates(date_per_race, self.__split_dates)
            self.__train_data_optuna = self.__select_by_race_ids(df, fit_ids)
            self.__valid_data_optuna = self.__select_by_race_ids(df, valid_ids)
            self.__train_data = pd.concat([self.__train_data_optuna, self.__valid_data_optuna])
            self.__test_data = self.__select_by_race_ids(df, test_ids)
        else:
            self.__train_data, self.__test_data = self.__split_by_date(self.__featured_data, test_size=test_size)
            self.__train_data_optuna, self.__valid_data_optuna = self.__split_by_date(self.__train_data, test_size=valid_size)

        # 学習・評価に用いる特徴量からは、目的変数・日付・オッズ・馬番・race_id を除外する
        # - `rank` は結果由来なのでリーク防止のため除外
        # - `win` は本 Splitter の目的変数
        # - `longshot` は結果由来のラベルなので除外
        drop_cols = ['rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']

        # Optuna/LightGBM用のDatasetはチューニング時のみ必要。
        self.__lgb_train_optuna = None
        self.__lgb_valid_optuna = None

        # X_train は検証期間（early stopping・校正用）を含まない
        self.__X_train = self.__train_data_optuna.drop(drop_cols, axis=1, errors='ignore')
        self.__y_train = self.__train_data_optuna['win']
        self.__X_test = self.__test_data.drop(drop_cols, axis=1, errors='ignore')
        self.__y_test = self.__test_data['win']

        self.__X_train_optuna = self.__train_data_optuna.drop(drop_cols, axis=1, errors='ignore')
        self.__y_train_optuna = self.__train_data_optuna['win']
        self.__X_valid_optuna = self.__valid_data_optuna.drop(drop_cols, axis=1, errors='ignore')
        self.__y_valid_optuna = self.__valid_data_optuna['win']

    def __build_lgb_dataset(self, X, y):
        dataset_cls = lgb_o.Dataset if lgb_o is not None else lgb.Dataset

        try:
            categorical_cols = X.select_dtypes(include='category').columns.tolist()
        except Exception:
            categorical_cols = []

        return dataset_cls(
            X,
            y,
            feature_name=list(X.columns),
            categorical_feature=categorical_cols if categorical_cols else 'auto',
            free_raw_data=True,
        )

    @staticmethod
    def __get_race_keys(index) -> pd.Index:
        if isinstance(index, pd.MultiIndex):
            return index.get_level_values(0)
        return index

    @staticmethod
    def __select_by_race_ids(df: pd.DataFrame, race_id_list) -> pd.DataFrame:
        if 'race_id' in df.columns:
            sliced = df[df['race_id'].isin(race_id_list)]
            if not sliced.empty:
                return sliced
            # 互換: 旧pickleでは race_id_list が index のキー（行ID）になっている場合がある
            try:
                return df.loc[race_id_list]
            except Exception:
                return sliced
        return df.loc[race_id_list]

    def __split_by_date(self, df, test_size):
        date_series = df['date']
        if 'race_id' in df.columns:
            race_keys = df['race_id']
        else:
            race_keys = self.__get_race_keys(df.index)
        date_per_race = date_series.groupby(race_keys).first()
        sorted_id_list = date_per_race.sort_values().index
        train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1 - test_size))]
        test_id_list = sorted_id_list[round(len(sorted_id_list) * (1 - test_size)) :]
        train = self.__select_by_race_ids(df, train_id_list)
        test = self.__select_by_race_ids(df, test_id_list)
        return train, test

    @property
    def featured_data(self):
        return self.__featured_data

    @property
    def train_data(self):
        return self.__train_data

    @property
    def test_data(self):
        return self.__test_data

    @property
    def train_data_optuna(self):
        return self.__train_data_optuna

    @property
    def valid_data_optuna(self):
        return self.__valid_data_optuna

    @property
    def lgb_train_optuna(self):
        if self.__lgb_train_optuna is None:
            self.__lgb_train_optuna = self.__build_lgb_dataset(self.__X_train_optuna, self.__y_train_optuna)
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
        if self.__lgb_valid_optuna is None:
            self.__lgb_valid_optuna = self.__build_lgb_dataset(self.__X_valid_optuna, self.__y_valid_optuna)
        return self.__lgb_valid_optuna

    @property
    def X_train(self):
        return self.__X_train

    @property
    def y_train(self):
        return self.__y_train

    @property
    def X_test(self):
        return self.__X_test

    @property
    def y_test(self):
        return self.__y_test

    @property
    def X_valid(self):
        """校正（calibration）用の検証データ。"""
        return self.__X_valid_optuna

    @property
    def y_valid(self):
        """校正（calibration）用の検証ラベル。"""
        return self.__y_valid_optuna

    @property
    def tansho_odds_test(self):
        return self.__test_data[ResultsCols.TANSHO_ODDS]
