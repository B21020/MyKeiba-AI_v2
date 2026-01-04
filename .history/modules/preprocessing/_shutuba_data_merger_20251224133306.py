import os
import pandas as pd

from ._data_merger import DataMerger
from modules.preprocessing import ShutubaTableProcessor
from modules.preprocessing import HorseResultsProcessor
from modules.preprocessing import HorseInfoProcessor
from modules.preprocessing import PedsProcessor
from modules.constants import LocalPaths

class ShutubaDataMerger(DataMerger):
    def __init__(self,
                 shutuba_table_processor: ShutubaTableProcessor, 
                 horse_results_processor: HorseResultsProcessor,
                 horse_info_processor: HorseInfoProcessor,
                 peds_processor: PedsProcessor, 
                 target_cols: list, 
                 group_cols: list
                 ):
        """
        初期処理
        """
        # レース結果テーブル（前処理後）
        self._results = shutuba_table_processor.preprocessed_data
        # 馬の過去成績テーブル（前処理後）
        self._horse_results = horse_results_processor.preprocessed_data
        # 馬の基本情報テーブル（前処理後）
        self._horse_info = horse_info_processor.preprocessed_data
        # 血統テーブル（前処理後）
        self._peds = peds_processor.preprocessed_data
        # 集計対象列
        self._target_cols = target_cols
        # horse_idと一緒にターゲットエンコーディングしたいカテゴリ変数
        self._group_cols = group_cols
        # 全てのマージが完了したデータ
        self._merged_data = pd.DataFrame()
        # 日付(date列)ごとに分かれたレース結果
        self._separated_results_dict = {}
        # レース結果データのdateごとに分かれた馬の過去成績
        self._separated_horse_results_dict = {}
        
    def merge(self):
        """
        マージ処理
        """
        self._merge_horse_results()
        self._merge_horse_info()
        self._merge_peds()
        self._merge_jockey_stats()

    def _merge_jockey_stats(self):
        """\
        騎手成績特徴量テーブルのマージ（出馬表ベース）

        data/tmp/jockey_stats.pickle を (race_id, horse_id) 単位でマージする。
        ファイルが存在しない場合は何もしない。
        """
        stats_path = LocalPaths.JOCKEY_STATS_PATH
        if not os.path.isfile(stats_path):
            return

        stats = pd.read_pickle(stats_path)
        if not {'race_id', 'horse_id'}.issubset(stats.index.names):
            stats = stats.reset_index()
        else:
            stats = stats.reset_index()

        base = self._merged_data.reset_index().rename(columns={'index': 'race_id'})

        base = base.merge(
            stats,
            on=['race_id', 'horse_id'],
            how='left'
        )

        base = base.set_index('race_id')
        self._merged_data = base