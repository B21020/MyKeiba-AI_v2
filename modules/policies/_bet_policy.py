from abc import ABCMeta, abstractstaticmethod
import pandas as pd

from modules.constants import ResultsCols

class AbstractBetPolicy(metaclass=ABCMeta):
    """
    クラスの型を決めるための抽象クラス。
    """
    @abstractstaticmethod

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
    馬単の流し。

    `BetPolicyUmarenNagashi` と同じ設計で、actions には `umatan_nagashi` を返す。

    - anchor_threshold: 軸候補の閾値
    - partner_threshold: 相手候補の閾値
    - anchor_strategy: 'top1' なら軸は1頭（スコア最大）。'all' なら軸は全て。
    - partner_top_k: 相手候補をスコア上位K頭に制限（Noneなら制限なし）

    互換のため、旧引数名 threshold1/threshold2 でも受け付ける。
    """

    @staticmethod
    def judge(
        score_table: pd.DataFrame,
        anchor_threshold: Optional[float] = None,
        partner_threshold: Optional[float] = None,
        anchor_strategy: str = 'top1',
        partner_top_k: Optional[int] = None,
        threshold1: Optional[float] = None,
        threshold2: Optional[float] = None,
    ) -> dict:
        if anchor_threshold is None and threshold1 is not None:
            anchor_threshold = threshold1
        if partner_threshold is None and threshold2 is not None:
            partner_threshold = threshold2
        if anchor_threshold is None or partner_threshold is None:
            raise TypeError('anchor_threshold and partner_threshold are required')

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
            partners = [p for p in partners if p not in set(anchors)]
            if not partners:
                continue

            bet_dict[str(race_id)] = {'umatan_nagashi': {'anchor': anchors, 'partners': partners}}

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
        return bet_dict

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

class BetPolicyEVTansho:
    """期待値（EV）ベースの単勝戦略。

    期待回収率の近似として EV = p(1着) * odds を用い、
    EV >= 1 + margin を満たす馬に単勝を購入する。

    - score_table['score'] を p(1着) とみなす（ScorePolicyの出力）
    - オッズは score_table 内の `odds_col`（デフォルト: ResultsCols.TANSHO_ODDS='単勝'）
      または引数 `tansho_odds`（score_table と同一indexにalign可能な Series）で与える
    - max_bets_per_race を指定すると、レースごとに EV 上位K頭に制限する
    """

    @staticmethod
    def judge(
        score_table: pd.DataFrame,
        margin: float = 0.0,
        tansho_odds: Optional[pd.Series] = None,
        odds_col: str = ResultsCols.TANSHO_ODDS,
        max_bets_per_race: Optional[int] = None,
        min_odds: Optional[float] = None,
    ) -> dict:
        if not isinstance(score_table, pd.DataFrame):
            raise TypeError('score_table must be a pandas DataFrame')

        required = {'score', ResultsCols.UMABAN}
        missing = [c for c in required if c not in score_table.columns]
        if missing:
            raise KeyError(f'missing required columns: {missing}')

        table = score_table.copy()

        # odds を確保（列 or 外部Series）
        if odds_col not in table.columns:
            if tansho_odds is None:
                raise KeyError(
                    f"missing '{odds_col}' in score_table and tansho_odds is None"
                )
            if isinstance(tansho_odds, pd.Series):
                # index align を優先
                table[odds_col] = tansho_odds.reindex(table.index)
            else:
                if len(tansho_odds) != len(table):
                    raise ValueError('tansho_odds length must match score_table')
                table[odds_col] = list(tansho_odds)

        # numeric 化
        table['score'] = pd.to_numeric(table['score'], errors='coerce')
        table[odds_col] = pd.to_numeric(table[odds_col], errors='coerce')
        table[ResultsCols.UMABAN] = pd.to_numeric(table[ResultsCols.UMABAN], errors='coerce')

        # 不正値を除外
        table = table.dropna(subset=['score', odds_col, ResultsCols.UMABAN])
        table = table[(table['score'] >= 0) & (table['score'] <= 1) & (table[odds_col] > 0)]
        if min_odds is not None:
            table = table[table[odds_col] >= float(min_odds)]

        # EV = p * odds
        ev_threshold = 1.0 + float(margin)
        table = table.assign(ev=table['score'] * table[odds_col])
        table = table[table['ev'] >= ev_threshold]

        if table.empty:
            return {}

        # レース単位に購入点数を制限（任意）
        if max_bets_per_race is not None:
            k = int(max_bets_per_race)
            if k <= 0:
                raise ValueError('max_bets_per_race must be a positive int or None')

            if 'race_id' in table.columns:
                table = (
                    table.sort_values(['race_id', 'ev'], ascending=[True, False])
                    .groupby('race_id', sort=False)
                    .head(k)
                )
            else:
                # index(level=0) を race_id とみなす
                race_keys = table.index.get_level_values(0)
                table = (
                    table.assign(_race_key=race_keys)
                    .sort_values(['_race_key', 'ev'], ascending=[True, False])
                    .groupby('_race_key', sort=False)
                    .head(k)
                    .drop(columns=['_race_key'])
                )

        # actions 形式へ
        if 'race_id' in table.columns:
            bet_df = table.groupby('race_id')[ResultsCols.UMABAN].apply(list).to_frame()
        else:
            bet_df = table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

        # 馬番を int に正規化
        def _to_int_list(xs):
            s = pd.to_numeric(pd.Series(xs), errors='coerce')
            return s.dropna().astype(int).drop_duplicates().tolist()

        bet_df[ResultsCols.UMABAN] = bet_df[ResultsCols.UMABAN].apply(_to_int_list)
        bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) > 0]
        bet_dict = bet_df.rename(columns={ResultsCols.UMABAN: 'tansho'}).T.to_dict()
        return bet_dict

