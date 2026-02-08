import lightgbm as lgb
import pandas as pd

try:
    import optuna.integration.lightgbm as lgb_o
except ModuleNotFoundError:
    # 推論（モデルロード/スコア計算）ではoptuna連携は不要。
    # チューニング実行時のみ optuna-integration[lightgbm] を要求する。
    lgb_o = None

from modules.constants import ResultsCols


class DataSplitter:
    def __init__(self, featured_data, test_size, valid_size) -> None:
        self.__featured_data = featured_data
        self.__lgb_train_optuna = None
        self.__lgb_valid_optuna = None
        self.__X_test = None
        self.__y_test = None
        self.__tansho_odds_test = None
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        訓練データとテストデータに分ける。さらに訓練データをoptuna用の訓練データと検証データに分ける。
        """
        df = self.__featured_data

        # 学習・評価に用いる特徴量からは、目的変数・日付・オッズ・馬番を除外する
        # NOTE: 目的変数候補（例: win/longshot）が混入するとリークするため除外する
        drop_cols = ['rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']
        self.__feature_cols = [c for c in df.columns if c not in drop_cols]

        # race_id単位の代表日付（小さいSeries）を作ってからID分割する
        date_series = df['date']
        if isinstance(df.index, pd.MultiIndex):
            race_keys = df.index.get_level_values(0)
        else:
            race_keys = df.index
        date_per_race = date_series.groupby(race_keys).first()

        # id分割（巨大DataFrameの実体コピーを作らない）
        train_id_list, test_id_list = self.__split_id_by_date(date_per_race, test_size=test_size)
        self.__train_id_list = train_id_list
        self.__test_id_list = test_id_list

        # train/valid（optuna用）も id で分割（必要時にDatasetへ変換）
        train_optuna_ids, valid_optuna_ids = self.__split_id_by_date(date_per_race.loc[train_id_list], test_size=valid_size)
        self.__train_optuna_id_list = train_optuna_ids
        self.__valid_optuna_id_list = valid_optuna_ids

        # 学習に必要な最小限だけ先に作る（test側は後段で必要なら作る）
        self.__X_train = df.loc[train_id_list, self.__feature_cols]
        self.__y_train = df.loc[train_id_list, 'rank']

        # optuna用も遅延（Dataset作成時に生成）
        self.__X_train_optuna = None
        self.__y_train_optuna = None
        self.__X_valid_optuna = None
        self.__y_valid_optuna = None

    def __build_lgb_dataset(self, X, y):
        dataset_cls = lgb_o.Dataset if lgb_o is not None else lgb.Dataset

        # category列はLightGBMに明示しておく（pandas category を保持する前提）
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

    def __split_id_by_date(self, df_or_series, test_size):
        """
        時系列に沿って訓練IDとテストIDに分ける関数。test_sizeは0~1。
        """
        # df_or_series は race_id -> date のSeries（推奨）または date列を含むDataFrame
        if isinstance(df_or_series, pd.Series):
            date_per_race = df_or_series
        else:
            date_series = df_or_series['date']
            if isinstance(df_or_series.index, pd.MultiIndex):
                race_keys = df_or_series.index.get_level_values(0)
            else:
                race_keys = df_or_series.index
            date_per_race = date_series.groupby(race_keys).first()

        sorted_id_list = date_per_race.sort_values().index
        train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1 - test_size))]
        test_id_list = sorted_id_list[round(len(sorted_id_list) * (1 - test_size)) :]
        return train_id_list, test_id_list

    @property
    def featured_data(self):
        return self.__featured_data

    @property
    def train_data(self):
        return self.__featured_data.loc[self.__train_id_list]

    @property
    def test_data(self):
        return self.__featured_data.loc[self.__test_id_list]

    @property
    def train_data_optuna(self):
        return self.__featured_data.loc[self.__train_optuna_id_list]

    @property
    def valid_data_optuna(self):
        return self.__featured_data.loc[self.__valid_optuna_id_list]

    @property
    def lgb_train_optuna(self):
        if self.__lgb_train_optuna is None:
            if self.__X_train_optuna is None:
                df = self.__featured_data
                self.__X_train_optuna = df.loc[self.__train_optuna_id_list, self.__feature_cols]
                self.__y_train_optuna = df.loc[self.__train_optuna_id_list, 'rank']
            self.__lgb_train_optuna = self.__build_lgb_dataset(self.__X_train_optuna, self.__y_train_optuna)
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
        if self.__lgb_valid_optuna is None:
            if self.__X_valid_optuna is None:
                df = self.__featured_data
                self.__X_valid_optuna = df.loc[self.__valid_optuna_id_list, self.__feature_cols]
                self.__y_valid_optuna = df.loc[self.__valid_optuna_id_list, 'rank']
            self.__lgb_valid_optuna = self.__build_lgb_dataset(self.__X_valid_optuna, self.__y_valid_optuna)
        return self.__lgb_valid_optuna

    @property
    def X_train(self):
        return self.__X_train

    @property
    def y_train(self):
        return self.__y_train

    @property
    def X_valid(self):
        """early stopping 用の検証データ（train内のvalid split）。"""
        if self.__X_valid_optuna is None:
            df = self.__featured_data
            self.__X_valid_optuna = df.loc[self.__valid_optuna_id_list, self.__feature_cols]
            self.__y_valid_optuna = df.loc[self.__valid_optuna_id_list, 'rank']
        return self.__X_valid_optuna

    @property
    def y_valid(self):
        if self.__y_valid_optuna is None:
            _ = self.X_valid
        return self.__y_valid_optuna

    @property
    def X_test(self):
        if self.__X_test is None:
            self.__X_test = self.__featured_data.loc[self.__test_id_list, self.__feature_cols]
        return self.__X_test

    @property
    def y_test(self):
        if self.__y_test is None:
            self.__y_test = self.__featured_data.loc[self.__test_id_list, 'rank']
        return self.__y_test

    @property
    def tansho_odds_test(self):
        if self.__tansho_odds_test is None:
            self.__tansho_odds_test = self.__featured_data.loc[self.__test_id_list, ResultsCols.TANSHO_ODDS]
        return self.__tansho_odds_test
