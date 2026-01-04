import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score

try:
    import optuna.integration.lightgbm as lgb_o
except ModuleNotFoundError:
    # 推論では不要。チューニング実行時のみ optuna-integration[lightgbm] を要求する。
    lgb_o = None

from ._data_splitter import DataSplitter


class ModelWrapper:
    """
    モデルのハイパーパラメータチューニング・学習の処理が記述されたクラス。
    """
    def __init__(self):
        self.__lgb_model = lgb.LGBMClassifier(objective='binary')
        self.__feature_importance = None

    def tune_hyper_params(self, datasets: DataSplitter):
        """
        optunaによるチューニングを実行。
        """

        if lgb_o is None:
            raise ModuleNotFoundError(
                "Could not find `optuna-integration` for `lightgbm`. "
                "Please run `pip install optuna-integration[lightgbm]` to enable tuning."
            )

        params = {'objective': 'binary'}

        # チューニング実行
        lgb_clf_o = lgb_o.train(
            params,
            datasets.lgb_train_optuna,
            valid_sets=(datasets.lgb_train_optuna, datasets.lgb_valid_optuna),
            callbacks=[
                lgb.callback.log_evaluation(period=100),  # 100イテレーションごとに評価結果を出力
                lgb.callback.early_stopping(stopping_rounds=10)  # 早期停止パラメータ、verboseはデフォルトtrueのため指定不要
                ],
            optuna_seed=100 # optunaのseed固定
            )

        # num_iterationsとearly_stopping_roundは今は使わないので削除
        tunedParams = {
            k: v for k, v in lgb_clf_o.params.items() if k not in ['num_iterations', 'early_stopping_round']
            }

        self.__lgb_model.set_params(**tunedParams)

    @property
    def params(self):
        return self.__lgb_model.get_params()

    def set_params(self, ex_params):
        """
        外部からハイパーパラメータを設定する場合。
        """
        self.__lgb_model.set_params(**ex_params)

    def train(self, datasets: DataSplitter):
        # 学習
        # DataFrameでfitして、列名をモデル側に残す（推論時の列整列に必須）
        self.__lgb_model.fit(datasets.X_train, datasets.y_train.values)
        # 学習時の列名をモデルに保持（predict時の整列に利用）
        try:
            self.__lgb_model.feature_name_ = list(datasets.X_train.columns)
        except Exception:
            pass
        # AUCを計算して出力
        # 予測時も列名順に整列（念のため）
        auc_train = roc_auc_score(
            datasets.y_train, self.__lgb_model.predict_proba(datasets.X_train.reindex(columns=datasets.X_train.columns))[:, 1]
            )
        auc_test = roc_auc_score(
            datasets.y_test,
            self.__lgb_model.predict_proba(datasets.X_test.reindex(columns=datasets.X_train.columns, fill_value=0))[:, 1]
            )
        # 特徴量の重要度を記憶しておく
        self.__feature_importance = pd.DataFrame({
            "features": datasets.X_train.columns,
            "importance": self.__lgb_model.feature_importances_
            }).sort_values("importance", ascending=False)
        print('AUC: {:.3f}(train), {:.3f}(test)'.format(auc_train, auc_test))

    @property
    def feature_importance(self):
        return self.__feature_importance

    @property
    def lgb_model(self):
        return self.__lgb_model

    @lgb_model.setter
    def lgb_model(self, loaded):
        self.__lgb_model = loaded
