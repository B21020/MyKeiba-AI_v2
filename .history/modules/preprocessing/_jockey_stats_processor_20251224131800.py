import pandas as pd

from ._abstract_data_processor import AbstractDataProcessor


class JockeyStatsProcessor(AbstractDataProcessor):
    """騎手ごとの直近レース複勝率を計算する前処理クラス。

    入力:
        - filepath: pd.read_pickle で読み込める馬の成績データ（horse_results）のパス。
          少なくとも以下の情報を含む DataFrame を想定する。
            - インデックス: horse_id
            - カラム例:
                - race_id: レースID（インデックス or カラムのいずれか）
                - jockey_id もしくは 騎手を一意に識別できるIDカラム
                - date: レース日付 (datetime 型推奨)
                - rank: 着順 (int)

    出力 (preprocessed_data プロパティ):
        - インデックス: race_id, horse_id
        - カラム:
            - jockey_id
            - jockey_plc_rate_10_all:  直近最大10レースの複勝率（1〜3着を1とした平均）
            - jockey_rides_10_all:     直近最大10レースの騎乗数
            - jockey_plc_rate_50_all:  直近最大50レースの複勝率
            - jockey_rides_50_all:     直近最大50レースの騎乗数
            - jockey_has_history_flag: 過去レースが1件以上あれば1、なければ0

        ※すべて "対象レースより前のレースのみ" を用いて計算する前提。
        実装時には、jockey_id ごとに日付・レース順にソートし、
        shift(1) + rolling(window) でリークを防ぐ。
    """

    def __init__(self, filepath: str):
        super().__init__(filepath)

    def _preprocess(self) -> pd.DataFrame:
        """騎手ごとの直近複勝率特徴量を計算して返す。

        現時点では仕様定義のみ行い、実装は後続ステップで追加する。
        """
        df = self.raw_data
        # TODO: 実装時に、horse_results から jockey_id / race_id / date / rank を利用して
        #       騎手ごとの直近Nレース複勝率を計算するロジックを追加する。
        # プレースホルダとして、空の DataFrame を返しておく。
        return pd.DataFrame()
