import pandas as pd
from modules.policies import AbstractBetPolicy
from ._model_wrapper import ModelWrapper
from ._data_splitter import DataSplitter
from modules.policies import AbstractScorePolicy
from modules.constants import ResultsCols

class KeibaAI:
    """
    モデルの訓練や読み込み、実際に賭けるなどの処理を実行するクラス。
    """
    def __init__(self, datasets: DataSplitter):
        self.__datasets = datasets
        self.__model_wrapper = ModelWrapper()

    @property
    def datasets(self):
        return self.__datasets

    def train_with_tuning(self):
        """
        optunaでのチューニング後、訓練させる。
        """
        self.__model_wrapper.tune_hyper_params(self.__datasets)
        self.__model_wrapper.train(self.__datasets)

    def train_without_tuning(self):
        """
        ハイパーパラメータチューニングをスキップして訓練させる。
        """
        self.__model_wrapper.train(self.__datasets)

    def get_params(self):
        """
        ハイパーパラメータを取得
        """
        return self.__model_wrapper.params

    def set_params(self, params):
        """
        ハイパーパラメータを外部から設定。
        """
        self.__model_wrapper.set_params(params)

    def feature_importance(self, num_features=20):
        return self.__model_wrapper.feature_importance[:num_features]

    def calc_score(self, X: pd.DataFrame, score_policy: AbstractScorePolicy):
        """score_policyを元に、馬の「勝ちやすさスコア」を計算する。

        注意点:
            - 学習時は DataSplitter 側で馬番(ResultsCols.UMABAN)を特徴量から除外している。
            - しかしシミュレーション用の score_table では、馬番列が必要になる。

        対応:
            - 引数 X に馬番列が無く、かつ self.__datasets.test_data のインデックスと一致する場合、
              test_data 側から馬番列を付与してから score を計算する。
        """
        if ResultsCols.UMABAN not in X.columns:
            try:
                test_df = self.__datasets.test_data
            except Exception:
                test_df = None

            if isinstance(test_df, pd.DataFrame) and ResultsCols.UMABAN in test_df.columns:
                # インデックスが一致している場合にのみ、安全に馬番列を補完する
                if X.index.equals(test_df.index):
                    X = X.copy()
                    X[ResultsCols.UMABAN] = test_df[ResultsCols.UMABAN]

        return score_policy.calc(self.__model_wrapper.lgb_model, X)

    def decide_action(self, score_table: pd.DataFrame,
        bet_policy: AbstractBetPolicy, **params) -> dict:
        """
        bet_policyを元に、賭ける馬券を決定する。paramsにthresholdを入れる。
        """
        actions = bet_policy.judge(score_table, **params)

        return actions
