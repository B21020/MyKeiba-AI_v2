import os
import pandas as pd

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
        
    @property
    def featured_data(self):
        return self.__data
    
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

    def feature_engineering(self):
        """従来Notebook向けのまとめ処理（互換用）。

        以前のノートブックでは
        `FeatureEngineering(...).feature_engineering().categorical_processing()`
        という呼び出しを前提としているため、その互換APIを提供する。
        """
        return (
            self
            .add_interval()
            .add_agedays()
        )

    def categorical_processing(self):
        """従来Notebook向けのカテゴリ処理まとめ（互換用）。

        ダミー変数化 + ID系ラベルエンコードをまとめて実行する。
        """
        return (
            self
            .dumminize_ground_state()
            .dumminize_race_type()
            .dumminize_sex()
            .dumminize_weather()
            .encode_horse_id()
            .encode_jockey_id()
            .encode_trainer_id()
            .encode_owner_id()
            .encode_breeder_id()
            .dumminize_kaisai()
            .dumminize_around()
            .dumminize_race_class()
        )