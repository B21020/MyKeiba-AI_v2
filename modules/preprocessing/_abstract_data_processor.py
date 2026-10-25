import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import Optional
import copy

class AbstractDataProcessor(metaclass=ABCMeta):
    def __init__(self, filepath: Optional[str] = None, raw_data: Optional[pd.DataFrame] = None):
        if raw_data is None:
            if filepath is None:
                raise ValueError("Either filepath or raw_data must be provided")
            self.__raw_data = pd.read_pickle(filepath)
        else:
            self.__raw_data = raw_data

        self.__preprocessed_data = self._preprocess()

    @abstractmethod
    def _preprocess(self):
        pass
    
    @property
    def raw_data(self):
        # NOTE: deep copyは巨大pickleでメモリを圧迫しやすいため、shallow copyにする
        # （各Processor側での列追加・加工は通常ブロックコピーが走る）
        return self.__raw_data.copy(deep=False)

    @property
    def preprocessed_data(self):
        data = self.__preprocessed_data
        if isinstance(data, pd.DataFrame):
            return data.copy(deep=False)
        # dict/list 等は shallow copy を返す（deep=False は未対応）
        try:
            return data.copy()
        except Exception:
            return copy.copy(data)

    #rawデータを一つのファイルにまとめる運用に変更したため、以下は不要
    """def _delete_duplicate(self, old, new):
        filtered_old = old[~old.index.isin(new.index)]
        return pd.concat([filtered_old, new])

    def _read_pickle(self, path_list):
        df = pd.read_pickle(path_list[0])
        for path in path_list[1:]:
            df = self._delete_duplicate(df, pd.read_pickle(path))
        return df"""
