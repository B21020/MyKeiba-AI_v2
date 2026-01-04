from modules.preprocessing import ReturnProcessor
from itertools import permutations
from scipy.special import comb
import numpy as np
import pandas as pd

class BettingTickets:
    """
    馬券の買い方と、賭けた時のリターンを計算する。
    """
    def __init__(self, returnProcessor: ReturnProcessor) -> None:
        self.__returnTables = returnProcessor.preprocessed_data
        self.__returnTablesTansho = self.__returnTables['tansho']
        self.__returnTablesFukusho = self.__returnTables['fukusho']
        self.__returnTablesUmaren = self.__returnTables['umaren']
        self.__returnTablesUmatan = self.__returnTables['umatan']
        self.__returnTablesWide = self.__returnTables['wide']
        self.__returnTablesSanrenpuku = self.__returnTables['sanrenpuku']
        self.__returnTablesSanrentan = self.__returnTables['sanrentan']

    def bet_tansho(self, race_id: str, umaban: list, amount: float):
        """
        race_id: レースid。
        umaban: 賭けたい馬番をリストで入れる。一頭のみ賭けたい場合もリストで入れる。
        amount: 1枚に賭ける額。
        """
        # 賭ける枚数
        n_bets = len(umaban)
        if n_bets == 0:
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount
            # 賭けるレースidに絞った単勝の払い戻し表
            table_1R = self.__returnTablesTansho.loc[race_id]
            # table_1R が Series（1行）か DataFrame（複数行）かに応じて安全に処理
            if isinstance(table_1R, pd.Series):
                win_vals = np.array([table_1R['win']])
                ret_vals = np.array([table_1R['return']])
            else:
                # DataFrame 想定
                win_vals = table_1R['win'].to_numpy()
                ret_vals = table_1R['return'].to_numpy()

            # 勝ち馬番号が賭けた馬番リストに含まれる行を抽出
            mask = np.isin(win_vals, umaban)
            # 払戻金をスカラーで計算（複数行あれば合算）
            return_amount = float((ret_vals[mask] * amount / 100).sum())
            return n_bets, bet_amount, return_amount

    def bet_fukusho(self, race_id: str, umaban: list, amount: float):
        """
        引数の考え方は単勝と同様。
        """
        # 賭ける枚数
        n_bets = len(umaban)
        if n_bets == 0:
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount
            # 賭けるレースidに絞った複勝の払い戻し表
            table_1R = self.__returnTablesFukusho.loc[race_id]
            # table_1R が Series（1行）か DataFrame（複数行）かに応じて安全に処理
            if isinstance(table_1R, pd.Series):
                table_df = table_1R.to_frame().T
            else:
                table_df = table_1R

            win_cols = ['win_0', 'win_1', 'win_2']
            ret_cols = ['return_0', 'return_1', 'return_2']

            win_vals = table_df[win_cols].to_numpy()
            ret_vals = table_df[ret_cols].to_numpy()

            # 勝ち馬（複勝対象）のいずれかに賭けていれば的中
            mask = np.isin(win_vals, umaban).astype(float)

            # 払戻金をスカラーで計算（該当セルが複数あっても全て合算）
            # ret_vals は object の可能性があるため float 化しておく
            return_amount = float((mask * ret_vals.astype(float) * amount / 100).sum())
            return n_bets, bet_amount, return_amount

    def bet_umaren_box(self, race_id: str, umaban: list, amount: float):
        """
        馬連BOX馬券。1枚のみ買いたい場合もこの関数を使う。
        """
        # 賭ける枚数
        # 例）4C2（4コンビネーション2）
        n_bets = comb(len(umaban), 2, exact=True)
        if n_bets == 1:
            #print('例外')
            return 0, 0, 0
        else:
            # 賭けた合計額
            bet_amount = n_bets * amount
            # 賭けるレースidに絞った馬連払い戻し表
            table_1R = self.__returnTablesUmaren.loc[race_id]
            if isinstance(table_1R, pd.Series):
                table_df = table_1R.to_frame().T
            else:
                table_df = table_1R

            # 的中判定（行ごとに win_0, win_1 が両方含まれるか）
            win_vals = table_df[['win_0', 'win_1']].to_numpy()
            hits_row = np.isin(win_vals, umaban).all(axis=1).astype(float)

            # 払い戻し合計額（複数行あっても合算し、必ずスカラーで返す）
            ret_vals = table_df['return'].to_numpy()
            return_amount = float((hits_row * ret_vals * amount / 100).sum())
        return int(n_bets), bet_amount, return_amount

    def _bet_umatan(self, race_id: str, umaban: list, amount: float):
        """
        馬単を一枚のみ賭ける場合の関数。umabanは[1着予想, 2着予想]の形で馬番を入れる。
        """
        #len(umaban) != 2の時の例外処理
        if len(umaban) != 2:
            print('例外')
            return 0, 0, 0

        # 賭けるレースidに絞った馬単払い戻し表
        table_1R = self.__returnTablesUmatan.loc[race_id]
        if isinstance(table_1R, pd.Series):
            table_df = table_1R.to_frame().T
        else:
            table_df = table_1R

        # 的中判定（行ごとに順序一致）
        hits_row = (
            table_df['win_0'].eq(umaban[0]) &
            table_df['win_1'].eq(umaban[1])
        ).to_numpy().astype(float)

        # 払い戻し合計額（複数行あっても合算し、必ずスカラーで返す）
        ret_vals = table_df['return'].to_numpy()
        return_amount = float((hits_row * ret_vals * amount / 100).sum())
        return 1, amount, return_amount

    def bet_umatan_box(self, race_id: str, umaban: list, amount: float):
        """
        馬単をBOX馬券で賭ける場合の関数。
        """
        n_bets = 0
        bet_amount = 0
        return_amount = 0
        for pair in permutations(umaban, 2):
            n_bets_single, bet_amount_single, return_amount_single \
                = self._bet_umatan(race_id, list(pair), amount)
            # 賭ける枚数
            n_bets += n_bets_single
            # 賭けた合計額
            bet_amount += bet_amount_single
            # 払い戻し合計額
            return_amount += return_amount_single
        return n_bets, bet_amount, return_amount

    def bet_wide_box(self, race_id: str, umaban: list, amount: float):
        """
        ワイドをBOX馬券で賭ける関数。1枚のみ賭ける場合もこの関数を使う。
        """
        # 賭ける枚数
        n_bets = comb(len(umaban), 2, exact=True)
        # 賭けた合計額
        bet_amount = n_bets * amount
        # 賭けるレースidに絞ったワイド払い戻し表
        table_1R = self.__returnTablesWide.loc[race_id]
        if isinstance(table_1R, pd.Series):
            table_df = table_1R.to_frame().T
        else:
            table_df = table_1R

        # 的中判定（行ごとに win_0, win_1 が両方含まれるか）
        hits_row = (
            table_df['win_0'].isin(umaban) &
            table_df['win_1'].isin(umaban)
        ).to_numpy().astype(float)

        # 払い戻し合計額（複数行あっても合算し、必ずスカラーで返す）
        ret_vals = table_df['return'].to_numpy()
        return_amount = float((hits_row * ret_vals * amount / 100).sum())
        return int(n_bets), bet_amount, return_amount

    def bet_sanrenpuku_box(self, race_id: str, umaban: list, amount: float):
        """
        三連複BOX馬券。1枚のみ買いたい場合もこの関数を使う。
        """
        # 賭ける枚数
        n_bets = comb(len(umaban), 3, exact=True)
        # 賭けた合計額
        bet_amount = n_bets * amount
        # 賭けるレースidに絞った三連複払い戻し表
        table_1R = self.__returnTablesSanrenpuku.loc[race_id]
        if isinstance(table_1R, pd.Series):
            table_df = table_1R.to_frame().T
        else:
            table_df = table_1R

        # 的中判定（行ごとに 3 着すべて含まれるか）
        win_vals = table_df[['win_0', 'win_1', 'win_2']].to_numpy()
        hits_row = np.isin(win_vals, umaban).all(axis=1).astype(float)

        # 払い戻し合計額（複数行あっても合算し、必ずスカラーで返す）
        ret_vals = table_df['return'].to_numpy()
        return_amount = float((hits_row * ret_vals * amount / 100).sum())
        return int(n_bets), bet_amount, return_amount

    def _bet_sanrentan(self, race_id: str, umaban: list, amount: float):
        """
        三連単を一枚のみ賭ける場合の関数。umabanは[1着予想, 2着予想, 3着予想]の形で馬番を入れる。
        """
        # len(umaban) != 3 の時は不正入力として0返却
        if len(umaban) != 3:
            return 0, 0, 0

        # 賭けるレースidに絞った三連単払い戻し表
        table_1R = self.__returnTablesSanrentan.loc[race_id]

        # table_1R が Series（1行）か DataFrame（複数行）かに応じて安全に処理
        if isinstance(table_1R, pd.Series):
            table_df = table_1R.to_frame().T
        else:
            table_df = table_1R

        # 的中判定（行ごとに 3 着すべて一致しているか）
        hits = (
            table_df['win_0'].eq(umaban[0]) &
            table_df['win_1'].eq(umaban[1]) &
            table_df['win_2'].eq(umaban[2])
        )

        # 払戻金をスカラーで計算（該当行が複数あっても合算）
        hit_array = hits.to_numpy().astype(float)
        return_array = table_df['return'].to_numpy()
        return_amount = float((hit_array * return_array * amount / 100).sum())

        # 1点買いとしてカウント
        return 1, amount, return_amount

    def bet_sanrentan_box(self, race_id: str, umaban: list, amount: float):
        """
        三連単をBOX馬券で賭ける場合の関数。
        """
        n_bets = 0
        bet_amount = 0
        return_amount = 0
        for pair in permutations(umaban, 3):
            n_bets_single, bet_amount_single, return_amount_single \
                = self._bet_sanrentan(race_id, list(pair), amount)
            # 賭ける枚数
            n_bets += n_bets_single
            # 賭けた合計額
            bet_amount += bet_amount_single
            # 払い戻し合計額
            return_amount += return_amount_single
        return n_bets, bet_amount, return_amount

    def others(self, race_id: str, umaban: list, amount: float):
        """
        その他、フォーメーション馬券や流し馬券の定義
        """
        pass
