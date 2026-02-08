import pandas as pd
from ._abstract_data_processor import AbstractDataProcessor

class ReturnProcessor(AbstractDataProcessor):
    @staticmethod
    def _drop_exact_duplicate_rows_per_race(df: pd.DataFrame) -> pd.DataFrame:
        """同一 race_id 内の完全重複行を除去する。

        - index（通常は race_id）も重複判定に含める
        - 別 race_id で値が同じ行は消さない
        """
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return df

        # reset_index すると、index列が先頭に並ぶ（MultiIndexなら複数列）
        tmp = df.reset_index()
        index_cols = list(tmp.columns[: df.index.nlevels])
        data_cols = [c for c in tmp.columns if c not in index_cols]
        subset = index_cols + data_cols
        tmp = tmp.drop_duplicates(subset=subset, keep='first')

        if len(index_cols) == 1:
            out = tmp.set_index(index_cols[0])
            out.index.name = df.index.name
            return out
        else:
            out = tmp.set_index(index_cols)
            out.index.names = df.index.names
            return out

    def __init__(self, filepath):
        """
        初期処理
        """
        super().__init__(filepath)
    
    def _preprocess(self):
        """
        前処理
        """
        return_dict = {}
        return_dict['tansho'] = self.__tansho()
        return_dict['fukusho'] = self.__fukusho()
        return_dict['umaren'] = self.__umaren()
        return_dict['umatan'] = self.__umatan()
        return_dict['wide'] = self.__wide()
        return_dict['sanrentan'] = self.__sanrentan()
        return_dict['sanrenpuku'] = self.__sanrenpuku()        
        return return_dict
    
    def __tansho(self):
        """
        単勝
        """
        tansho = self.raw_data[self.raw_data[0]=='単勝'][[1,2]]
        tansho.columns = ['win', 'return']
        
        for column in tansho.columns:
            tansho[column] = pd.to_numeric(tansho[column], errors='coerce')

        return self._drop_exact_duplicate_rows_per_race(tansho)
    
    def __fukusho(self):
        """
        複勝
        """
        fukusho = self.raw_data[self.raw_data[0]=='複勝'][[1,2]]
        wins = fukusho[1].str.split('br', expand=True)[[0,1,2]]
        
        wins.columns = ['win_0', 'win_1', 'win_2']
        returns = fukusho[2].str.split('br', expand=True)[[0,1,2]]
        returns.columns = ['return_0', 'return_1', 'return_2']
        
        df = pd.concat([wins, returns], axis=1)
        for column in df.columns:
            df[column] = df[column].str.replace(',', '')
        df = df.fillna(0).astype(int)
        return self._drop_exact_duplicate_rows_per_race(df)
    
    
    def __umaren(self):
        """
        馬連
        """
        umaren = self.raw_data[self.raw_data[0]=='馬連'][[1,2]]
        wins = umaren[1].str.split('-', expand=True)[[0,1]].add_prefix('win_')
        return_ = umaren[2].rename('return')  
        df = pd.concat([wins, return_], axis=1)        
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        return self._drop_exact_duplicate_rows_per_race(df)
    
    
    def __umatan(self):
        """
        馬単
        """
        umatan = self.raw_data[self.raw_data[0]=='馬単'][[1,2]]
        wins = umatan[1].str.split('→', expand=True)[[0,1]].add_prefix('win_')
        return_ = umatan[2].rename('return')  
        df = pd.concat([wins, return_], axis=1)        
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        return self._drop_exact_duplicate_rows_per_race(df)
    
    
    def __wide(self):
        """
        ワイド
        """
        wide = self.raw_data[self.raw_data[0]=='ワイド'][[1,2]]
        wins = wide[1].str.split('br', expand=True)[[0,1,2]]
        wins = wins.stack().str.split('-', expand=True).add_prefix('win_')
        return_ = wide[2].str.split('br', expand=True)[[0,1,2]]
        return_ = return_.stack().rename('return')
        df = pd.concat([wins, return_], axis=1)
        df = df.apply(lambda x: pd.to_numeric(x.str.replace(',',''), errors='coerce'))
        return self._drop_exact_duplicate_rows_per_race(df)
    
    
    def __sanrentan(self):
        """
        三連単
        """
        rentan = self.raw_data[self.raw_data[0]=='三連単'][[1,2]]
        wins = rentan[1].str.split('→', expand=True)[[0,1,2]].add_prefix('win_')
        return_ = rentan[2].rename('return')
        df = pd.concat([wins, return_], axis=1) 
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        return self._drop_exact_duplicate_rows_per_race(df)
    
    
    def __sanrenpuku(self):
        """
        三連複
        """
        renpuku = self.raw_data[self.raw_data[0]=='三連複'][[1,2]]
        wins = renpuku[1].str.split('-', expand=True)[[0,1,2]].add_prefix('win_')
        return_ = renpuku[2].rename('return')
        df = pd.concat([wins, return_], axis=1) 
        df = df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
        return self._drop_exact_duplicate_rows_per_race(df)
    