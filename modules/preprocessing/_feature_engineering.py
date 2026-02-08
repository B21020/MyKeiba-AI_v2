import os
import pandas as pd
import numpy as np

from ._data_merger import DataMerger
from modules.constants import LocalPaths, HorseResultsCols, Master

class FeatureEngineering:
    """
    使うテーブルを全てマージした後の処理をするクラス。
    新しい特徴量を作りたいときは、メソッド単位で追加していく。
    各メソッドは依存関係を持たないよう注意。
    """
    def __init__(self, data_merger: DataMerger):
        self.__data = data_merger.merged_data.copy()
        self.__dtypes_optimized = False
        
    @property
    def featured_data(self):
        # NOTE: Notebook側でFeatureEngineeringインスタンスが既に作られていても、
        # 学習直前のアクセスでdtype最適化が効くよう遅延実行する。
        if not self.__dtypes_optimized:
            self.optimize_dtypes()
        return self.__data

    def optimize_dtypes(self):
        """学習用データのdtypeを軽量化してメモリを削減する。

        - float64 -> float32
        - int64/uint64 -> 可能なら int32/uint32 以下へ downcast
        - category/object/datetime は維持
        """

        df = self.__data

        # 1) float64 -> float32（最も効果が大きい）
        float64_cols = df.select_dtypes(include=['float64']).columns
        if len(float64_cols) > 0:
            df[float64_cols] = df[float64_cols].astype('float32')

        # 2) pandas nullable float (Float64) -> float32
        nullable_float_cols = [c for c in df.columns if str(df[c].dtype) == 'Float64']
        for c in nullable_float_cols:
            # pd.NA は np.nan になり、通常のfloat32列として扱える
            df[c] = df[c].astype('float32')

        # 3) int64/uint64 を可能な範囲でdowncast
        int64_cols = df.select_dtypes(include=['int64']).columns
        for c in int64_cols:
            df[c] = pd.to_numeric(df[c], downcast='integer')

        uint64_cols = df.select_dtypes(include=['uint64']).columns
        for c in uint64_cols:
            df[c] = pd.to_numeric(df[c], downcast='unsigned')

        # 4) pandas nullable integers (Int64/UInt64) は範囲チェックして縮小
        for c in df.columns:
            dtype_str = str(df[c].dtype)
            if dtype_str == 'Int64':
                s = df[c]
                if s.notna().any():
                    vmin = int(s.min(skipna=True))
                    vmax = int(s.max(skipna=True))
                    if np.iinfo(np.int32).min <= vmin and vmax <= np.iinfo(np.int32).max:
                        df[c] = s.astype('Int32')
            elif dtype_str == 'UInt64':
                s = df[c]
                if s.notna().any():
                    vmax = int(s.max(skipna=True))
                    if vmax <= np.iinfo(np.uint32).max:
                        df[c] = s.astype('UInt32')

        self.__data = df
        self.__dtypes_optimized = True
        return self
    
    def add_interval(self):
        """
        前走からの経過日数
        """
        self.__data['interval'] = (self.__data['date'] - self.__data['latest']).dt.days
        self.__data.drop('latest', axis=1, inplace=True)
        return self

    def add_agedays(self):
        """
        レース出走日から日齢を算出
        """
        # 日齢を算出
        self.__data['age_days'] = (self.__data['date'] - self.__data['birthday']).dt.days
        self.__data.drop('birthday', axis=1, inplace=True)
        return self
    
    def dumminize_weather(self):
        """
        weatherカラムをダミー変数化する
        """
        self.__data['weather'] = pd.Categorical(self.__data['weather'], Master.WEATHER_LIST)
        self.__data = pd.get_dummies(self.__data, columns=['weather'])
        return self
    
    def dumminize_race_type(self):
        """
        race_typeカラムをダミー変数化する
        """
        self.__data['race_type'] = pd.Categorical(
            self.__data['race_type'], list(Master.RACE_TYPE_DICT.values())
            )
        self.__data = pd.get_dummies(self.__data, columns=['race_type'])
        return self
    
    def dumminize_ground_state(self):
        """
        ground_stateカラムをダミー変数化する
        """
        self.__data['ground_state'] = pd.Categorical(
            self.__data['ground_state'], Master.GROUND_STATE_LIST
            )
        self.__data = pd.get_dummies(self.__data, columns=['ground_state'])
        return self
    
    def dumminize_sex(self):
        """
        sexカラムをダミー変数化する
        """
        self.__data['性'] = pd.Categorical(self.__data['性'], Master.SEX_LIST)
        self.__data = pd.get_dummies(self.__data, columns=['性'])
        return self
    
    def __label_encode(self, target_col: str):
        """
        引数で指定されたID（horse_id/jockey_id/trainer_id/owner_id/breeder_id）を
        ラベルエンコーディングして、Categorical型に変換する。
        """
        csv_path = os.path.join(LocalPaths.MASTER_DIR, target_col + '.csv')
        # ファイルが存在しない場合、空のDataFrameを作成
        if not os.path.isfile(csv_path):
            target_master = pd.DataFrame(columns=[target_col, 'encoded_id'])
        else:
            target_master = pd.read_csv(csv_path, dtype=object)

        # マスタ整形: NaN/重複を除去し、型を保証
        if len(target_master) > 0:
            target_master = target_master.dropna(subset=[target_col])
            target_master = target_master.drop_duplicates(subset=[target_col], keep='first')
            target_master['encoded_id'] = pd.to_numeric(target_master['encoded_id'], errors='coerce')
            target_master = target_master.dropna(subset=['encoded_id'])
            target_master['encoded_id'] = target_master['encoded_id'].astype(int)
        else:
            target_master = pd.DataFrame(columns=[target_col, 'encoded_id'])

        # 現在データ側の抽出（NaN除去）
        curr_vals = self.__data[[target_col]].dropna(subset=[target_col]).copy()

        # masterに存在しない、新しい情報を抽出
        if len(target_master) > 0:
            known = set(target_master[target_col].astype(str).tolist())
            new_target = curr_vals[~curr_vals[target_col].astype(str).isin(known)]
        else:
            new_target = curr_vals
        new_target = new_target.drop_duplicates(subset=[target_col], keep='first')

        # 新しい情報に連番を付与
        if len(target_master) > 0:
            start = int(target_master['encoded_id'].max()) + 1
            new_target = new_target.assign(encoded_id=range(start, start + len(new_target)))
        else:  # まだ1行も登録されていない場合
            new_target = new_target.assign(encoded_id=range(len(new_target)))

        # マスタ更新（インデックスが一意になるよう整理）
        updated_master = pd.concat([target_master, new_target], ignore_index=True)
        updated_master = updated_master.dropna(subset=[target_col])
        updated_master = updated_master.drop_duplicates(subset=[target_col], keep='first')
        updated_master[[target_col, 'encoded_id']].to_csv(csv_path, index=False)

        # マッピングSeries（インデックス一意）
        mapping = updated_master.set_index(target_col)['encoded_id']
        mapping = mapping[~mapping.index.duplicated(keep='first')]

        # ラベルエンコーディング実行
        self.__data[target_col] = pd.Categorical(self.__data[target_col].map(mapping))
        return self
    
    def encode_horse_id(self):
        """
        horse_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode('horse_id')
        return self
    
    def encode_jockey_id(self):
        """
        jockey_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode('jockey_id')
        return self
    
    def encode_trainer_id(self):
        """
        trainer_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode('trainer_id')
        return self

    def encode_owner_id(self):
        """
        owner_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode('owner_id')
        return self

    def encode_breeder_id(self):
        """
        breeder_idをラベルエンコーディングして、Categorical型に変換する。
        """
        self.__label_encode('breeder_id')
        return self

    def dumminize_kaisai(self):
        """
        開催カラムをダミー変数化する
        """
        self.__data[HorseResultsCols.PLACE] = pd.Categorical(
            self.__data[HorseResultsCols.PLACE], list(Master.PLACE_DICT.values())
            )
        self.__data = pd.get_dummies(self.__data, columns=[HorseResultsCols.PLACE])
        return self

    def dumminize_around(self):
        """
        aroundカラムをダミー変数化する
        """
        self.__data['around'] = pd.Categorical(self.__data['around'], Master.AROUND_LIST)
        self.__data = pd.get_dummies(self.__data, columns=['around'])
        return self

    def dumminize_race_class(self):
        """
        race_classカラムをダミー変数化する
        """
        self.__data['race_class'] = pd.Categorical(self.__data['race_class'], Master.RACE_CLASS_LIST)
        self.__data = pd.get_dummies(self.__data, columns=['race_class'])
        return self