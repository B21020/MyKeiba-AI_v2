from abc import ABCMeta, abstractstaticmethod
from typing import Optional
import pandas as pd

from modules.constants import ResultsCols

class AbstractBetPolicy(metaclass=ABCMeta):
    """
    クラスの型を決めるための抽象クラス。
    """
    @abstractstaticmethod
    def judge(score_table, **params):
        """
        bet_dictは{race_id: {馬券の種類: 馬番のリスト}}の形式で返す。

        例)
        {'202101010101': {'tansho': [6, 8], 'fukusho': [4, 5]},
        '202101010102': {'tansho': [1], 'fukusho': [4]},
        '202101010103': {'tansho': [6], 'fukusho': []},
        '202101010104': {'tansho': [5], 'fukusho': [11]},
        ...}
        """
        pass

class BetPolicyTansho:
    """
    thresholdを超えた馬に単勝で賭ける戦略。
    """
    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        # race_id列があればそれをキーに、無ければ従来通りインデックスでグループ化
        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'tansho'}).T.to_dict()
        return bet_dict

class BetPolicyFukusho:
    """
    thresholdを超えた馬に複勝で賭ける戦略。
    """
    @staticmethod
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'fukusho'}).T.to_dict()
        return bet_dict

class BetPolicyUmarenBox:
    """
    thresholdを超えた馬に馬連BOXで賭ける戦略。
    """
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'umaren'}).T.to_dict()
        return bet_dict


class BetPolicyUmarenNagashi:
    """ 
    threshold1を超えた馬を軸にし、threshold2を超えた馬を相手にして馬連の流しで賭ける。

    actions には `umaren_nagashi` を返す（Simulator側で解釈して計算）。

    - anchor_strategy: 'top1' なら軸は1頭（スコア最大）。'all' なら軸は全て。
    - partner_top_k: 相手候補をスコア上位K頭に制限（Noneなら制限なし）
    """

    @staticmethod
    def judge(
        score_table: pd.DataFrame,
        anchor_threshold: float,
        partner_threshold: float,
        anchor_strategy: str = 'top1',
        partner_top_k: Optional[int] = None,
    ) -> dict:
        if anchor_strategy not in ('top1', 'all'):
            raise ValueError("anchor_strategy must be 'top1' or 'all'")

        if partner_top_k is not None:
            partner_top_k = int(partner_top_k)
            if partner_top_k <= 0:
                raise ValueError('partner_top_k must be a positive int or None')

        bet_dict: dict = {}

        if 'race_id' in score_table.columns:
            grouped = score_table.groupby('race_id', sort=False)
        else:
            grouped = score_table.groupby(level=0, sort=False)

        for race_id, table in grouped:
            anchors_df = table[table['score'] >= float(anchor_threshold)]
            if anchors_df.empty:
                continue

            if anchor_strategy == 'top1':
                anchors = (
                    anchors_df[[ResultsCols.UMABAN, 'score']]
                    .sort_values('score', ascending=False)
                    .head(1)[ResultsCols.UMABAN]
                    .tolist()
                )
            else:
                anchors = anchors_df[ResultsCols.UMABAN].drop_duplicates().tolist()

            if not anchors:
                continue

            partners_df = table[table['score'] >= float(partner_threshold)]
            if partner_top_k is not None and not partners_df.empty:
                partners_df = partners_df.sort_values('score', ascending=False).head(partner_top_k)

            partners = partners_df[ResultsCols.UMABAN].drop_duplicates().tolist()
            # 相手から軸を除外
            partners = [p for p in partners if p not in set(anchors)]
            if not partners:
                continue

            bet_dict[str(race_id)] = {'umaren_nagashi': {'anchor': anchors, 'partners': partners}}

        return bet_dict

class BetPolicyUmatanBox:
    """
    thresholdを超えた馬に馬単BOXで賭ける戦略。
    """
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'umatan'}).T.to_dict()
        return bet_dict

class BetPolicyWideBox:
    """
    thresholdを超えた馬にワイドBOXで賭ける戦略。
    """
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 2]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'wide'}).T.to_dict()
        return bet_dict

class BetPolicySanrenpukuBox:
    """
    thresholdを超えた馬に三連複BOXで賭ける戦略。
    """
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 3]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'sanrenpuku'}).T.to_dict()
        return bet_dict

class BetPolicySanrentanBox:
    """
    thresholdを超えた馬に三連単BOXで賭ける戦略。
    """
    def judge(score_table: pd.DataFrame, threshold: float) -> dict:
        filtered_table = score_table[score_table['score'] >= threshold]

        if 'race_id' in filtered_table.columns:
            bet_df = filtered_table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = filtered_table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) >= 3]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'sanrentan'}).T.to_dict()
        return bet_dict

class BetPolicyUmatanNagashi:
    """
    threshold1を超えた馬を軸にし、threshold2を超えた馬を相手にして馬単で賭ける。（未実装）
    """
    def judge(score_table: pd.DataFrame, threshold1: float, threshold2: float) -> dict:
        bet_dict = {}
        filtered_table = score_table.query('score >= @threshold2')
        filtered_table['flg'] = filtered_table['score'].map(lambda x: 'jiku' if x >= threshold1 else 'aite')
        for race_id, table in filtered_table.groupby(level=0):
            bet_dict_1R = {}
            bet_dict_1R['tansho'] = list(table.query('flg == "tansho"')[ResultsCols.UMABAN])
            bet_dict_1R['fukusho'] = list(table.query('flg == "fukusho"')[ResultsCols.UMABAN])
            bet_dict[race_id] = bet_dict_1R
        return bet_dict


class BetPolicyUmarenNagashiDualScore:
    """2モデル併用: 軸と相手で別スコアを使う馬連流し。

    想定ユースケース:
    - 既存モデル(score_anchor)で「堅い軸」を選ぶ
    - 穴馬モデル(score_partner)で「入着しそうな穴馬」を相手に選ぶ

    入力 score_table は以下の列を想定:
    - ResultsCols.UMABAN
    - race_id（推奨。無ければ index(level=0) を race_id とみなす）
    - score_anchor: 軸用スコア
    - score_partner: 相手用スコア

    返り値 actions 形式は Simulator が解釈できる `umaren_nagashi`。
    """

    @staticmethod
    def judge(
        score_table: pd.DataFrame,
        anchor_threshold: float,
        partner_threshold: float,
        anchor_strategy: str = 'top1',
        partner_top_k: Optional[int] = None,
        score_anchor_col: str = 'score_anchor',
        score_partner_col: str = 'score_partner',
    ) -> dict:
        if anchor_strategy not in ('top1', 'all'):
            raise ValueError("anchor_strategy must be 'top1' or 'all'")

        if partner_top_k is not None:
            partner_top_k = int(partner_top_k)
            if partner_top_k <= 0:
                raise ValueError('partner_top_k must be a positive int or None')

        required = {ResultsCols.UMABAN, score_anchor_col, score_partner_col}
        missing = [c for c in required if c not in score_table.columns]
        if missing:
            raise KeyError(f'missing required columns: {missing}')

        bet_dict: dict = {}

        if 'race_id' in score_table.columns:
            grouped = score_table.groupby('race_id', sort=False)
        else:
            grouped = score_table.groupby(level=0, sort=False)

        for race_id, table in grouped:
            anchors_df = table[table[score_anchor_col] >= float(anchor_threshold)]
            if anchors_df.empty:
                continue

            if anchor_strategy == 'top1':
                anchors = (
                    anchors_df[[ResultsCols.UMABAN, score_anchor_col]]
                    .sort_values(score_anchor_col, ascending=False)
                    .head(1)[ResultsCols.UMABAN]
                    .tolist()
                )
            else:
                anchors = anchors_df[ResultsCols.UMABAN].drop_duplicates().tolist()

            if not anchors:
                continue

            partners_df = table[table[score_partner_col] >= float(partner_threshold)]
            if partners_df.empty:
                continue
            if partner_top_k is not None:
                partners_df = partners_df.sort_values(score_partner_col, ascending=False).head(partner_top_k)

            partners = partners_df[ResultsCols.UMABAN].drop_duplicates().tolist()
            partners = [p for p in partners if p not in set(anchors)]
            if not partners:
                continue

            bet_dict[str(race_id)] = {'umaren_nagashi': {'anchor': anchors, 'partners': partners}}

        return bet_dict
