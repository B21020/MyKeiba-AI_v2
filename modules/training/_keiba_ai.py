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
                # X_test は test_data から drop して作られるため、index が同一のことが多い。
                # index が重複している場合、reindex は失敗することがあるため、まず equals を優先。
                aligned = None
                if X.index.equals(test_df.index):
                    aligned = test_df
                else:
                    try:
                        aligned = test_df.reindex(X.index)
                    except Exception:
                        # 最終手段: 長さが同一なら位置で合わせる（順序が維持されている前提）
                        if len(test_df) == len(X):
                            aligned = test_df

                if isinstance(aligned, pd.DataFrame):
                    X = X.copy()
                    if need_umaban and ResultsCols.UMABAN in aligned.columns:
                        X[ResultsCols.UMABAN] = aligned[ResultsCols.UMABAN].to_numpy()
                    if need_race_id and 'race_id' in aligned.columns:
                        X['race_id'] = aligned['race_id'].to_numpy()

        model = self.__model_wrapper.lgb_model

        # --- ToDo?: カテゴリ列のカテゴリ順序を学習時に合わせる ---
        # LightGBMは pandas category の「カテゴリ一覧と順序」まで一致している必要がある。
        # （集合として同じでも順序が違うと ValueError になる）
        try:
            X_train = self.__datasets.X_train
            train_cat_cols = X_train.select_dtypes(include='category').columns.tolist()
        except Exception:
            train_cat_cols = []
            X_train = None

        # ロード済みモデルでも推論できるよう、学習時カテゴリ情報をモデルにも保持
        try:
            if train_cat_cols and not hasattr(model, 'mykeiba_categorical_columns_'):
                setattr(model, 'mykeiba_categorical_columns_', list(train_cat_cols))
                if isinstance(X_train, pd.DataFrame):
                    setattr(
                        model,
                        'mykeiba_categorical_categories_',
                        {c: list(X_train[c].cat.categories) for c in train_cat_cols},
                    )
        except Exception:
            pass

        # 推論対象Xのカテゴリ列を学習時カテゴリに強制整合
        # - カテゴリ順序の不一致
        # - object化されている
        # - 未存在列（学習時にあって推論時に無い）
        # をまとめて吸収する。
        if train_cat_cols and isinstance(X_train, pd.DataFrame) and isinstance(X, pd.DataFrame):
            X = X.copy()
            for c in train_cat_cols:
                train_categories = list(X_train[c].cat.categories)
                if c not in X.columns:
                    X[c] = pd.Categorical([pd.NA] * len(X), categories=train_categories)
                    continue
                s = X[c]
                if getattr(s.dtype, 'name', '') != 'category':
                    X[c] = pd.Categorical(s, categories=train_categories)
                else:
                    X[c] = s.cat.set_categories(train_categories)

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

        score_table = score_policy.calc(model, X)

        # 返却する score_table には、購入判断に必要なメタ列を保持する
        if isinstance(score_table, pd.DataFrame):
            if ResultsCols.UMABAN not in score_table.columns and ResultsCols.UMABAN in X.columns:
                score_table[ResultsCols.UMABAN] = X[ResultsCols.UMABAN]
            if 'race_id' not in score_table.columns and 'race_id' in X.columns:
                score_table['race_id'] = X['race_id']

        return score_table

    def decide_action(self, score_table: pd.DataFrame,
        bet_policy: AbstractBetPolicy, **params) -> dict:
        """
        bet_policyを元に、賭ける馬券を決定する。paramsにthresholdを入れる。
        """
        actions = bet_policy.judge(score_table, **params)

        return actions
