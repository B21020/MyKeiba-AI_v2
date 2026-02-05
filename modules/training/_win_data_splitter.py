import lightgbm as lgb

try:
    import optuna.integration.lightgbm as lgb_o
except ModuleNotFoundError:
    lgb_o = None

from modules.constants import ResultsCols


class WinDataSplitter:
    """`win`(1着=1/それ以外=0) を目的変数にする DataSplitter。

    既存の `DataSplitter` は `rank` 固定のため、既存コードを崩さず
    単勝特化モデル用に同等のインターフェースを持つ Splitter を追加する。
    """

    def __init__(self, featured_data, test_size, valid_size) -> None:
        self.__featured_data = featured_data
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        self.__train_data, self.__test_data = self.__split_by_date(self.__featured_data, test_size=test_size)
        self.__train_data_optuna, self.__valid_data_optuna = self.__split_by_date(self.__train_data, test_size=valid_size)

        dataset_cls = lgb_o.Dataset if lgb_o is not None else lgb.Dataset

        # 学習・評価に用いる特徴量からは、目的変数・日付・オッズ・馬番・race_id を除外する
        # - `rank` は結果由来なのでリーク防止のため除外
        # - `win` は本 Splitter の目的変数
        drop_cols = ['rank', 'win', 'date', ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']

        self.__lgb_train_optuna = dataset_cls(
            self.__train_data_optuna.drop(drop_cols, axis=1, errors='ignore').values,
            self.__train_data_optuna['win'],
        )
        self.__lgb_valid_optuna = dataset_cls(
            self.__valid_data_optuna.drop(drop_cols, axis=1, errors='ignore').values,
            self.__valid_data_optuna['win'],
        )

        self.__X_train = self.__train_data.drop(drop_cols, axis=1, errors='ignore')
        self.__y_train = self.__train_data['win']
        self.__X_test = self.__test_data.drop(drop_cols, axis=1, errors='ignore')
        self.__y_test = self.__test_data['win']

    def __split_by_date(self, df, test_size):
        sorted_id_list = df.sort_values('date').index.unique()
        train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1 - test_size))]
        test_id_list = sorted_id_list[round(len(sorted_id_list) * (1 - test_size)) :]
        train = df.loc[train_id_list]
        test = df.loc[test_id_list]
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
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
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
    def tansho_odds_test(self):
        return self.__test_data[ResultsCols.TANSHO_ODDS]
