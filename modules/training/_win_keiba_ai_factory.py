import datetime
import os
import dill

from ._keiba_ai import KeibaAI
from ._win_data_splitter import WinDataSplitter


class WinKeibaAIFactory:
    """単勝特化(1着=1)モデル用の Factory。

    既存の `KeibaAIFactory` を崩さず、
    - Splitter を win 用に差し替える
    - 保存パスを win 用に分離する
    を行う。
    """

    @staticmethod
    def create(featured_data, test_size=0.3, valid_size=0.3) -> KeibaAI:
        datasets = WinDataSplitter(featured_data, test_size, valid_size)
        return KeibaAI(datasets)

    @staticmethod
    def save(keibaAI: KeibaAI, version_name: str) -> None:
        """保存先: models/(yyyymmdd)/win/(version_name).pickle"""
        yyyymmdd = datetime.date.today().strftime('%Y%m%d')
        out_dir = os.path.join('models', yyyymmdd, 'win')
        os.makedirs(out_dir, exist_ok=True)
        filepath_pickle = os.path.join(out_dir, f'{version_name}.pickle')
        with open(filepath_pickle, mode='wb') as f:
            dill.dump(keibaAI, f)

    @staticmethod
    def load(filepath: str) -> KeibaAI:
        with open(filepath, mode='rb') as f:
            keiba_ai = dill.load(f)
        try:
            keiba_ai.prepare_for_inference()
        except Exception:
            pass
        return keiba_ai
