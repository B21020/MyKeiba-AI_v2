import pandas as pd

from ._abstract_data_processor import AbstractDataProcessor
from modules.constants import HorseResultsCols as Cols


class JockeyStatsProcessor(AbstractDataProcessor):
    """騎手ごとの直近レース複勝率を計算する前処理クラス。

    入力:
        - filepath: pd.read_pickle で読み込める馬の成績データ（horse_results）のパス。
          少なくとも以下の情報を含む DataFrame を想定する。
            - インデックス: horse_id（想定。実際には後続処理で列として保持する）
            - カラム例:
                - race_id: レースID
                - jockey_id もしくは 騎手を一意に識別できるID/名称カラム
                - 日付列（HorseResultsCols.DATE）
                - 着順列（HorseResultsCols.RANK）

    出力 (preprocessed_data プロパティ):
        - インデックス: race_id, horse_id
        - カラム:
            - jockey_id
            - jockey_plc_rate_10_all:  直近最大10レースの複勝率（1〜3着を1とした平均）
            - jockey_rides_10_all:     直近最大10レースの騎乗数
            - jockey_plc_rate_50_all:  直近最大50レースの複勝率
            - jockey_rides_50_all:     直近最大50レースの騎乗数
            - jockey_has_history_flag: 過去レースが1件以上あれば1、なければ0

        すべて "対象レースより前のレースのみ" を用いて計算する。
        具体的には jockey ごとに日付・レース順にソートし、
        shift(1) + rolling(window) でリークを防いでいる。
    """

    def __init__(self, filepath: str):
        super().__init__(filepath)

    def _preprocess(self) -> pd.DataFrame:
        """騎手ごとの直近複勝率特徴量を計算して返す。"""
        # 元データをコピー
        df = self.raw_data.copy()

        # 必須カラムの存在チェック
        required_cols = [Cols.DATE, Cols.RANK, 'race_id']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"JockeyStatsProcessor: 必須カラムが不足しています: {missing}")

        # 騎手IDに利用するキー列を決定（あれば jockey_id、なければ 騎手名）
        if 'jockey_id' in df.columns:
            jockey_key_col = 'jockey_id'
        elif Cols.JOCKEY in df.columns:
            jockey_key_col = Cols.JOCKEY
        else:
            raise ValueError(
                "JockeyStatsProcessor: 'jockey_id' もしくは 騎手名カラム(HorseResultsCols.JOCKEY) が必要です。"
            )

        # horse_id を列として保持（インデックスが horse_id であることを想定）
        if 'horse_id' in df.columns:
            # すでに列として存在する場合はそのまま使う
            horse_id_series = df['horse_id']
        else:
            # インデックス名が horse_id の場合はインデックスを使用
            if df.index.name == 'horse_id':
                horse_id_series = df.index.to_series()
            else:
                raise ValueError(
                    "JockeyStatsProcessor: horse_results のインデックス名が 'horse_id' ではなく、horse_id カラムも存在しません。"
                )
        df['horse_id'] = horse_id_series.astype(str)

        # 日付を datetime 型に変換
        df['date'] = pd.to_datetime(df[Cols.DATE])

        # 着順を数値化し、複勝フラグを作成（1〜3着を1、それ以外を0）
        rank_numeric = pd.to_numeric(df[Cols.RANK], errors='coerce')
        df = df[~rank_numeric.isna()].copy()
        df['rank_numeric'] = rank_numeric.astype(int)
        df['plc_flag'] = ((df['rank_numeric'] >= 1) & (df['rank_numeric'] <= 3)).astype(int)

        # 騎手キー列を追加
        df['_jockey_key'] = df[jockey_key_col].astype(str)

        # 騎手ごとに日付・race_id でソート
        df = df.sort_values(['_jockey_key', 'date', 'race_id'])

        # 騎手ごとに直近10/50レースの複勝率・騎乗数を計算（リーク防止のため shift(1) 後に rolling）
        def _calc_group(group: pd.DataFrame) -> pd.DataFrame:
            # 現在レースを含めないために1行シフト
            plc_shifted = group['plc_flag'].shift(1)
            # 有効な過去レースの有無（NaNでないものをカウント）
            ride_flag = (~plc_shifted.isna()).astype(int)

            # 直近10レース
            rides_10 = ride_flag.rolling(window=10, min_periods=1).sum()
            rate_10 = plc_shifted.rolling(window=10, min_periods=1).mean()

            # 直近50レース
            rides_50 = ride_flag.rolling(window=50, min_periods=1).sum()
            rate_50 = plc_shifted.rolling(window=50, min_periods=1).mean()

            group['jockey_rides_10_all'] = rides_10.fillna(0).astype(int)
            group['jockey_plc_rate_10_all'] = rate_10
            group['jockey_rides_50_all'] = rides_50.fillna(0).astype(int)
            group['jockey_plc_rate_50_all'] = rate_50

            # 過去レースが1件以上あるかどうか（ここでは50レース窓を基準に判定）
            group['jockey_has_history_flag'] = (rides_50 > 0).astype(int)
            return group

        df = df.groupby('_jockey_key', group_keys=False).apply(_calc_group)

        # 出力用の DataFrame 整形
        # jockey_id 列がない場合は _jockey_key をそのまま用いる
        if 'jockey_id' not in df.columns:
            df['jockey_id'] = df['_jockey_key']

        # race_id, horse_id をインデックスにした特徴量テーブルに変換
        out = df.set_index(['race_id', 'horse_id'])[
            [
                'jockey_id',
                'jockey_plc_rate_10_all',
                'jockey_rides_10_all',
                'jockey_plc_rate_50_all',
                'jockey_rides_50_all',
                'jockey_has_history_flag',
            ]
        ].sort_index()

        return out
