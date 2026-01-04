import pandas as pd
from modules.policies import AbstractBetPolicy
from ._model_wrapper import ModelWrapper
from ._data_splitter import DataSplitter
from modules.policies import AbstractScorePolicy
import re
from typing import Optional, Sequence
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

    @staticmethod
    def __looks_like_default_feature_names(feature_names) -> bool:
        if feature_names is None:
            return False
        if isinstance(feature_names, str):
            names = [feature_names]
        else:
            try:
                names = list(feature_names)
            except Exception:
                return False
        if len(names) == 0:
            return False

        # LightGBM sklearn が列名を知らないと Column_0, Column_1... になりがち
        default_patterns = (r"f\d+", r"Column_\d+", r"column_\d+")
        return all(any(re.fullmatch(p, str(n)) is not None for p in default_patterns) for n in names)

    def __get_training_feature_names(self) -> Optional[list]:
        try:
            return list(self.__datasets.X_train.columns)
        except Exception:
            return None

    @staticmethod
    def __get_expected_num_features(model) -> Optional[int]:
        expected = getattr(model, 'n_features_in_', None)
        if isinstance(expected, int) and expected > 0:
            return expected
        try:
            expected = model.booster_.num_feature()
            if isinstance(expected, int) and expected > 0:
                return expected
        except Exception:
            pass
        return None

    def prepare_for_inference(self) -> None:
        """
        推論時に列名不整合でLightGBMが落ちるのを防ぐため、
        学習時の特徴量名をモデル側に復元する。

        - 古いモデル: booster_のfeature_nameが f0,f1,... になっていることがある
        - その場合でも、KeibaAIが保持するdatasets.X_train.columnsから復元できる
        """
        model = self.__model_wrapper.lgb_model
        current = getattr(model, 'feature_name_', None)
        expected_n = self.__get_expected_num_features(model)

        training = self.__get_training_feature_names()

        # 既に妥当なfeature_name_が入っているなら何もしない
        if current is not None:
            try:
                current_len_ok = (expected_n is None) or (len(current) == expected_n)
            except Exception:
                current_len_ok = False
            if current_len_ok and not self.__looks_like_default_feature_names(current):
                # 列名が学習時列名とほぼ一致するならOK
                if training:
                    try:
                        inter = len(set(map(str, current)) & set(map(str, training)))
                        if inter / max(1, len(training)) >= 0.5:
                            return
                    except Exception:
                        return
                else:
                    return

        if training and (expected_n is None or len(training) == expected_n):
            try:
                model.feature_name_ = training
            except Exception:
                pass

    def calc_score(self, X: pd.DataFrame, score_policy: AbstractScorePolicy):
        """score_policyを元に、馬の「勝ちやすさスコア」を計算する。

        注意点:
            - 学習時は DataSplitter 側で馬番(ResultsCols.UMABAN)を特徴量から除外している。
            - しかしシミュレーション用の score_table では、馬番列が必要になる。

        対応:
            - 引数 X に馬番列が無く、かつ self.__datasets.test_data のインデックスと一致する場合、
              test_data 側から馬番列を付与してから score を計算する。
        """
        # 必要に応じて馬番・race_id列を補完する
        need_umaban = ResultsCols.UMABAN not in X.columns
        need_race_id = 'race_id' not in X.columns

        if need_umaban or need_race_id:
            try:
                test_df = self.__datasets.test_data
            except Exception:
                test_df = None

            if isinstance(test_df, pd.DataFrame):
                # インデックスが一致している場合にのみ、安全にメタ列を補完する
                if X.index.equals(test_df.index):
                    X = X.copy()
                    if need_umaban and ResultsCols.UMABAN in test_df.columns:
                        X[ResultsCols.UMABAN] = test_df[ResultsCols.UMABAN]
                    if need_race_id and 'race_id' in test_df.columns:
                        X['race_id'] = test_df['race_id']

        model = self.__model_wrapper.lgb_model

        # --- ToDo4: 学習時の特徴量列名をモデルに保持（ロード済みモデルでも有効） ---
        # 前日予測などで「モデルのfeature名」と「Xの列名」が一致しない場合、
        # 列整列で全0化→全馬同一スコアになり得る。
        # KeibaAIはDataSplitterを保持しているため、学習時の列名をここで注入する。
        try:
            train_cols = list(self.__datasets.X_train.columns)
            if train_cols and not hasattr(model, 'mykeiba_feature_columns_'):
                setattr(model, 'mykeiba_feature_columns_', train_cols)
        except Exception:
            pass

            # 推論時にモデル側のfeature名を復元（ロード済みモデルでも安定）
            self.prepare_for_inference()

            return score_policy.calc(model, X)

    def decide_action(self, score_table: pd.DataFrame,
        bet_policy: AbstractBetPolicy, **params) -> dict:
        """
        bet_policyを元に、賭ける馬券を決定する。paramsにthresholdを入れる。
        """
        actions = bet_policy.judge(score_table, **params)

        return actions
