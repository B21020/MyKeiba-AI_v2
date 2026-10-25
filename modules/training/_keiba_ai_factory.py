import datetime
import os
import dill
import pandas as pd
from ._keiba_ai import KeibaAI
from ._data_splitter import DataSplitter
from ._win_data_splitter import WinDataSplitter


class KeibaAIFactory:
    """
    KeibaAIのインスタンスを作成するためのクラス
    """
    @staticmethod
    def create(
        featured_data,
        test_size: float = 0.3,
        valid_size: float = 0.3,
        use_race_normalized_weight: bool = False,
        time_decay_half_life_days: float | None = None,
    ) -> KeibaAI:
        # NOTE: NotebookでFeatureEngineeringインスタンスが既に作られている場合でも、
        # ここでdtype最適化をかけてメモリ使用量を抑える。
        if isinstance(featured_data, pd.DataFrame):
            try:
                float64_cols = featured_data.select_dtypes(include=['float64']).columns
                if len(float64_cols) > 0:
                    featured_data[float64_cols] = featured_data[float64_cols].astype('float32')

                int64_cols = featured_data.select_dtypes(include=['int64']).columns
                for c in int64_cols:
                    featured_data[c] = pd.to_numeric(featured_data[c], downcast='integer')

                uint64_cols = featured_data.select_dtypes(include=['uint64']).columns
                for c in uint64_cols:
                    featured_data[c] = pd.to_numeric(featured_data[c], downcast='unsigned')
            except Exception:
                pass
        datasets = DataSplitter(
            featured_data,
            test_size,
            valid_size,
            use_race_normalized_weight=use_race_normalized_weight,
            time_decay_half_life_days=time_decay_half_life_days,
        )
        return KeibaAI(datasets)

    @staticmethod
    def create_win(
        featured_data,
        test_size: float = 0.3,
        valid_size: float = 0.3,
    ) -> KeibaAI:
        """単勝（1着）目的のKeibaAIを作る。"""
        if isinstance(featured_data, pd.DataFrame):
            try:
                float64_cols = featured_data.select_dtypes(include=['float64']).columns
                if len(float64_cols) > 0:
                    featured_data[float64_cols] = featured_data[float64_cols].astype('float32')

                int64_cols = featured_data.select_dtypes(include=['int64']).columns
                for c in int64_cols:
                    featured_data[c] = pd.to_numeric(featured_data[c], downcast='integer')

                uint64_cols = featured_data.select_dtypes(include=['uint64']).columns
                for c in uint64_cols:
                    featured_data[c] = pd.to_numeric(featured_data[c], downcast='unsigned')
            except Exception:
                pass
        datasets = WinDataSplitter(featured_data, test_size, valid_size)
        return KeibaAI(datasets)

    @staticmethod
    def save(keibaAI: KeibaAI, version_name: str) -> None:
        """
        日付やバージョン、パラメータ、データなどを保存。
        保存先はmodels/(yyyymmdd)/(version_name).pickle。
        """
        yyyymmdd = datetime.date.today().strftime('%Y%m%d')
        # ディレクトリ作成
        os.makedirs(os.path.join('models', yyyymmdd), exist_ok=True)
        filepath_pickle = os.path.join('models', yyyymmdd, '{}.pickle'.format(version_name))
        with open(filepath_pickle, mode='wb') as f:
            dill.dump(keibaAI, f)
    
    @staticmethod
    def load(filepath: str) -> KeibaAI:
        with open(filepath, mode='rb') as f:
            keiba_ai = dill.load(f)
        # 古いモデルでも推論できるよう、学習時列名を復元
        try:
            keiba_ai.prepare_for_inference()
        except Exception:
            pass
        return keiba_ai
