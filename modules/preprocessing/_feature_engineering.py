import os
import pandas as pd

from ._data_merger import DataMerger
from modules.constants import Master
from modules.constants import HorseResultsCols

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
    
    def encode_horse_id(self):
        """
        horse_idをラベルエンコーディングして、Categorical型に変換する。
        """
        csv_path = 'data/master/horse_id.csv'
        if not os.path.isfile(csv_path):
            # ファイルが存在しない場合、空のDataFrameを作成
            horse_master = pd.DataFrame(columns=['horse_id', 'encoded_id'])
        else:
            horse_master = pd.read_csv(csv_path, dtype=object)
        # masterに存在しない、新しい馬を抽出
        new_horses = self.__data[['horse_id']][
            ~self.__data['horse_id'].isin(horse_master['horse_id'])
            ].drop_duplicates(subset=['horse_id'])
        # 新しい馬を登録
        if len(horse_master) > 0:
            new_horses['encoded_id'] = [
                i+max(horse_master['encoded_id']) for i in range(1, len(new_horses)+1)
                ]
        else: # まだ1行も登録されていない場合の処理
            new_horses['encoded_id'] = [i for i in range(len(new_horses))]
        # 元のマスタと繋げる
        new_horse_master = pd.concat([horse_master, new_horses]).set_index('horse_id')['encoded_id']
        # マスタファイルを更新
        new_horse_master.to_csv(csv_path)
        # ラベルエンコーディング実行
        self.__data['horse_id'] = pd.Categorical(self.__data['horse_id'].map(new_horse_master))
        return self
    
    def encode_jockey_id(self):
        """
        jockey_idをラベルエンコーディングして、Categorical型に変換する。
        """
        csv_path = 'data/master/jockey_id.csv'
        if not os.path.isfile(csv_path):
            # ファイルが存在しない場合、空のDataFrameを作成
            jockey_master = pd.DataFrame(columns=['jockey_id', 'encoded_id'])
        else:
            jockey_master = pd.read_csv(csv_path, dtype=object)
        # masterに存在しない、新しい騎手を抽出
        new_jockeys = self.__data[['jockey_id']][
            ~self.__data['jockey_id'].isin(jockey_master['jockey_id'])
            ].drop_duplicates(subset=['jockey_id'])
        # 新しい騎手を登録
        if len(jockey_master) > 0:
            new_jockeys['encoded_id'] = [
                i+max(jockey_master['encoded_id']) for i in range(1, len(new_jockeys)+1)
                ]
        else: # まだ1行も登録されていない場合の処理
            new_jockeys['encoded_id'] = [i for i in range(len(new_jockeys))]
        # 元のマスタと繋げる
        new_jockey_master = pd.concat([jockey_master, new_jockeys]).set_index('jockey_id')['encoded_id']
        # マスタファイルを更新
        new_jockey_master.to_csv(csv_path)
        # ラベルエンコーディング実行
        self.__data['jockey_id'] = pd.Categorical(self.__data['jockey_id'].map(new_jockey_master))
        return self
    
    def dumminize_kaisai(self):
        self.__data[HorseResultsCols.PLACE] = pd.Categorical(
            self.__data[HorseResultsCols.PLACE], list(Master.PLACE_DICT.values())
            )
        self.__data = pd.get_dummies(self.__data, columns=[HorseResultsCols.PLACE])
        return self