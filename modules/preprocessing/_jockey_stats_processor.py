import numpy as np
import pandas as pd

from ._abstract_data_processor import AbstractDataProcessor
from modules.constants import HorseResultsCols as Cols


class JockeyStatsProcessor(AbstractDataProcessor):
    """騎手ごとの直近レース複勝率を計算する前処理クラス。

    入力:
        - filepath: pd.read_pickle で読み込める馬の成績データ（horse_results）のパス。
          少なくとも以下の情報を含む DataFrame を想定する。
            - インデックス: horse_id 相当（インデックス名は問わない）
            - カラム例:
                - jockey_id もしくは 騎手を一意に識別できるID/名称カラム
                - 日付列（HorseResultsCols.DATE）
                - 着順列（HorseResultsCols.RANK）

    出力 (preprocessed_data プロパティ):
        - インデックス: date, horse_id
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
        required_cols = [Cols.DATE, Cols.RANK]
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

        # horse_id を列として保持
        if 'horse_id' in df.columns:
            # すでに列として存在する場合はそのまま使う
            horse_id_series = df['horse_id']
        else:
            # インデックス名は問わず、そのまま horse_id 相当として扱う
            horse_id_series = df.index.to_series()
        df['horse_id'] = horse_id_series.astype(str)

        # 日付を datetime 型に変換
        df['date'] = pd.to_datetime(df[Cols.DATE])

        # 着順を数値化し、複勝フラグを作成（1〜3着を1、それ以外を0）
        rank_numeric = pd.to_numeric(df[Cols.RANK], errors='coerce')
        # 数値化できなかった着順（"除外", "中止" など）は NaN になるので除外する
        valid_mask = ~rank_numeric.isna()
        df = df[valid_mask].copy()
        # NaN 行を除いた rank_numeric だけを int 変換する
        df['rank_numeric'] = rank_numeric[valid_mask].astype(int)
        df['plc_flag'] = ((df['rank_numeric'] >= 1) & (df['rank_numeric'] <= 3)).astype(int)

        # 騎手キー列を追加
        df['_jockey_key'] = df[jockey_key_col].astype(str)

        # 騎手ごとに日付順でソート
        # NOTE: 同日複数騎乗がある場合、単純な shift(1) だと「同日の別レース結果」が混入し得る。
        #       ここでは「その日より前のレースのみ」を使うよう、日付単位で一括計算する。
        df = df.sort_values(['_jockey_key', 'date'])

        def _calc_group_strict(group: pd.DataFrame) -> pd.DataFrame:
            group = group.copy()

            # 日付昇順（安定ソート）
            group = group.sort_values(['date'], kind='mergesort')

            dates = group['date'].to_numpy()
            flags = group['plc_flag'].to_numpy(dtype=np.int8, copy=False)

            out_rate_10 = np.empty(len(group), dtype='float64')
            out_rides_10 = np.empty(len(group), dtype='int32')
            out_rate_50 = np.empty(len(group), dtype='float64')
            out_rides_50 = np.empty(len(group), dtype='int32')

            last10: list[int] = []
            last50: list[int] = []
            sum10 = 0
            sum50 = 0

            i = 0
            while i < len(group):
                d = dates[i]
                j = i
                while j < len(group) and dates[j] == d:
                    j += 1

                # この日の全行に「前日まで」の値を付与
                rides10 = len(last10)
                rides50 = len(last50)
                rate10 = (sum10 / rides10) if rides10 > 0 else np.nan
                rate50 = (sum50 / rides50) if rides50 > 0 else np.nan

                out_rate_10[i:j] = rate10
                out_rides_10[i:j] = rides10
                out_rate_50[i:j] = rate50
                out_rides_50[i:j] = rides50

                # 当日の結果を履歴に追加（当日内の順序は特徴量に影響しない）
                for k in range(i, j):
                    f = int(flags[k])

                    last10.append(f)
                    sum10 += f
                    if len(last10) > 10:
                        sum10 -= last10.pop(0)

                    last50.append(f)
                    sum50 += f
                    if len(last50) > 50:
                        sum50 -= last50.pop(0)

                i = j

            group['jockey_plc_rate_10_all'] = out_rate_10
            group['jockey_rides_10_all'] = out_rides_10.astype(int)
            group['jockey_plc_rate_50_all'] = out_rate_50
            group['jockey_rides_50_all'] = out_rides_50.astype(int)
            group['jockey_has_history_flag'] = (out_rides_50 > 0).astype(int)
            return group

        # apply は騎手ごとに独立計算（同日リークを遮断）
        df = df.groupby('_jockey_key', group_keys=False, sort=False).apply(_calc_group_strict)

        # 出力用の DataFrame 整形
        # jockey_id 列がない場合は _jockey_key をそのまま用いる
        if 'jockey_id' not in df.columns:
            df['jockey_id'] = df['_jockey_key']

        # date, horse_id をインデックスにした特徴量テーブルに変換
        out = df.set_index(['date', 'horse_id'])[
            [
                'jockey_id',
                'jockey_plc_rate_10_all',
                'jockey_rides_10_all',
                'jockey_plc_rate_50_all',
                'jockey_rides_50_all',
                'jockey_has_history_flag',
            ]
        ].sort_index()

        # マージ時にキーが一意になるよう、(date, horse_id) が重複している行は後ろを優先して残す
        out = out[~out.index.duplicated(keep='last')]

        return out
