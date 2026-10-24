"""払戻表から単勝の払戻（100円あたり）を取り出す。1着同着は両馬に払戻がある。"""
from __future__ import annotations

import re

import pandas as pd


def _split(value) -> list[str]:
    # 同着の区切りは新しい表記で 'br'、古い表記では空白（例: '4 11'）
    return [s for s in re.split(r'br|\s+', str(value)) if s]


def tansho_payouts(return_tables: pd.DataFrame) -> pd.DataFrame:
    """columns: race_id, 馬番, tansho_payout（100円あたりの払戻額）。"""
    t = return_tables[return_tables[0] == '単勝']
    rows = []
    for race_id, r in t.iterrows():
        nums, pays = _split(r[1]), _split(r[2])
        if len(nums) != len(pays):
            raise ValueError(f'tansho payout mismatch in {race_id}: {r[1]!r} / {r[2]!r}')
        for n, p in zip(nums, pays):
            rows.append((str(race_id), int(n), float(p.replace(',', ''))))
    return pd.DataFrame(rows, columns=['race_id', '馬番', 'tansho_payout'])
