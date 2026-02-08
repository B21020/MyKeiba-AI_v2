import numpy as np
import pandas as pd

from modules.preprocessing import ReturnProcessor
from ._betting_tickets import BettingTickets


class Simulator:
    """
    賭けた馬券を元に、成績を記録していくクラス。
    """
    def __init__(self, return_processor: ReturnProcessor) -> None:
        self.betting_tickets = BettingTickets(return_processor)

    def calc_returns_per_race(self, actions: dict) -> pd.DataFrame:
        """
        KeibaAI.decideActionの出力を入れると、レースごとに

        - n_bets: そのレースで賭けた馬券の枚数
        - bet_amount: そのレースで賭けた金額
        - return_amount: そのレースでの払戻金
        - hit_or_not: 的中したかどうか

        が返ってくる。
        """
        returns_per_race_dict = {}
        for race_id in actions:
            n_bets_race = 0
            bet_amount_race = 0
            return_amount_race = 0
            try:
                for action in actions[race_id]:
                    #actionの定義を別途した方が良いかも
                    if action == 'tansho':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_tansho(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'fukusho':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_fukusho(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'umaren':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_umaren_box(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'umaren_pairs':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_umaren_pairs(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'umaren_nagashi':
                        payload = actions[race_id][action]

                        # 互換: {'anchor': x, 'partners': [...]} / (x, [...]) / [x, ...partners]
                        anchor = None
                        partners = []

                        if isinstance(payload, dict):
                            anchor = payload.get('anchor')
                            partners = payload.get('partners', [])
                        elif isinstance(payload, (tuple, list)) and len(payload) == 2 and isinstance(payload[1], (list, tuple, set)):
                            anchor, partners = payload[0], list(payload[1])
                        elif isinstance(payload, (list, tuple)):
                            if len(payload) >= 2:
                                anchor = payload[0]
                                partners = list(payload[1:])
                            else:
                                anchor = payload
                        else:
                            anchor = payload

                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_umaren_nagashi(
                            race_id, anchor, partners, 1
                        )
                    elif action == 'umatan_nagashi':
                        payload = actions[race_id][action]

                        # 互換: {'anchor': x, 'partners': [...]} / (x, [...]) / [x, ...partners]
                        anchor = None
                        partners = []

                        if isinstance(payload, dict):
                            anchor = payload.get('anchor')
                            partners = payload.get('partners', [])
                        elif isinstance(payload, (tuple, list)) and len(payload) == 2 and isinstance(payload[1], (list, tuple, set)):
                            anchor, partners = payload[0], list(payload[1])
                        elif isinstance(payload, (list, tuple)):
                            if len(payload) >= 2:
                                anchor = payload[0]
                                partners = list(payload[1:])
                            else:
                                anchor = payload
                        else:
                            anchor = payload

                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_umatan_nagashi(
                            race_id, anchor, partners, 1
                        )
                    elif action == 'umatan':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_umatan_box(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'wide':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_wide_box(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'sanrenpuku':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_sanrenpuku_box(
                            race_id, actions[race_id][action], 1
                            )
                    elif action == 'sanrentan':
                        n_bets, bet_amount, return_amount = self.betting_tickets.bet_sanrentan_box(
                            race_id, actions[race_id][action], 1
                            )
                    n_bets_race += n_bets
                    bet_amount_race += bet_amount
                    return_amount_race += return_amount
                    returns_per_race_dict[race_id] = {
                        'n_bets': n_bets_race,
                        'bet_amount': bet_amount_race,
                        'return_amount': return_amount_race,
                        'hit_or_not': int(return_amount_race > 0)
                    }
            except KeyError:
                # 払戻テーブルに存在しないレースIDは、比較プロットのため一旦スキップする
                continue
        return pd.DataFrame.from_dict(returns_per_race_dict, orient='index')

    def calc_returns(self, actions: dict) -> dict:
        """
        self.calc_returns_per_race(actions)の結果を集計する
        """
        returns_dict = {}
        if len(actions) != 0:
            returns_per_race = self.calc_returns_per_race(actions)
            # 払戻テーブルに存在するレースIDが一つも無い場合は、全て0として返す
            if returns_per_race.empty:
                returns_dict['n_bets'] = 0
                returns_dict['n_races'] = 0
                returns_dict['n_hits'] = 0
                returns_dict['total_bet_amount'] = 0
                returns_dict['return_rate'] = 0
                returns_dict['std'] = 0
                return returns_dict

            returns_dict['n_bets'] = returns_per_race['n_bets'].sum()
            returns_dict['n_races'] = returns_per_race.index.nunique()
            returns_dict['n_hits'] = returns_per_race['hit_or_not'].sum()
            returns_dict['total_bet_amount'] = returns_per_race['bet_amount'].sum()

            if returns_dict['total_bet_amount'] == 0:
                returns_dict['return_rate'] = 0
            else:
                returns_dict['return_rate'] = returns_per_race['return_amount'].sum() \
                    / returns_dict['total_bet_amount']

            if returns_dict['total_bet_amount'] == 0:
                returns_dict['std'] = 0
            else:
                returns_dict['std'] = returns_per_race['return_amount'].std() * np.sqrt(returns_dict['n_races']) \
                    / returns_dict['total_bet_amount']
        return returns_dict
