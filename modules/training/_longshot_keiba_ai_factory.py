import datetime
import os
import dill

from ._keiba_ai import KeibaAI
from ._longshot_data_splitter import LongshotDataSplitter


class LongshotKeibaAIFactory:
    """穴馬(単勝>=15倍 かつ 3着内)モデル用の Factory。

    既存のモデルを崩さずに、
    - Splitter を longshot 用に差し替える
    - 保存パスを longshot 用に分離する
    を行う。
    """

    @staticmethod
    def create(featured_data, test_size=0.3, valid_size=0.3) -> KeibaAI:
        datasets = LongshotDataSplitter(featured_data, test_size, valid_size)
        return KeibaAI(datasets)

    @staticmethod
    def save(keibaAI: KeibaAI, version_name: str) -> None:
        """保存先: models/(yyyymmdd)/longshot/(version_name).pickle"""
        yyyymmdd = datetime.date.today().strftime('%Y%m%d')
        out_dir = os.path.join('models', yyyymmdd, 'longshot')
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
