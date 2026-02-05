import sys
from pathlib import Path

import pandas as pd


# tools/ 配下から直接実行しても import が通るようにプロジェクトルートを追加
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from modules.simulation import Simulator


class DummyReturnProcessor:
    """BettingTickets が参照する最小限の払戻テーブルを持つスタブ。"""

    def __init__(self, umaren_table: pd.DataFrame):
        empty = pd.DataFrame().set_index(pd.Index([], name="race_id"))
        self.preprocessed_data = {
            "tansho": empty,
            "fukusho": empty,
            "umaren": umaren_table,
            "umatan": empty,
            "wide": empty,
            "sanrenpuku": empty,
            "sanrentan": empty,
        }


def main() -> None:
    race_id = "TEST_RACE_001"

    # 払戻テーブル（馬連）: win_0, win_1, return
    umaren = pd.DataFrame(
        [{"race_id": race_id, "win_0": 1, "win_1": 3, "return": 520}]
    ).set_index("race_id")

    sim = Simulator(DummyReturnProcessor(umaren))

    cases = {
        "dict_payload": {race_id: {"umaren_nagashi": {"anchor": 1, "partners": [2, 3, 4]}}},
        "tuple_payload": {race_id: {"umaren_nagashi": (1, [2, 3, 4])}},
        "list_payload": {race_id: {"umaren_nagashi": [1, 2, 3, 4]}},
        "multi_anchor": {race_id: {"umaren_nagashi": {"anchor": [1, 5], "partners": [2, 3]}}},
    }

    for name, actions in cases.items():
        per_race = sim.calc_returns_per_race(actions)
        row = per_race.loc[race_id]
        print(
            name,
            "n_bets=",
            int(row["n_bets"]),
            "bet=",
            float(row["bet_amount"]),
            "return=",
            float(row["return_amount"]),
        )

    # 期待値（amount=1固定なので分かりやすい）
    # anchor=1, partners=[2,3,4] -> 3点買い, 的中は(1,3)のみ -> 520/100=5.2
    expected_return = 520 / 100
    expected_bets = 3

    per_race = sim.calc_returns_per_race(cases["dict_payload"])
    row = per_race.loc[race_id]
    assert int(row["n_bets"]) == expected_bets
    assert abs(float(row["return_amount"]) - expected_return) < 1e-9

    print("OK")


if __name__ == "__main__":
    main()
