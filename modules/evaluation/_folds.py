"""日付境界による5区分の fold（再開計画 5.1）。

区分（外側評価の開始日 S を基準）:
  fit     学習本体                [S - fit_years - 1年, S - 1年)
  select  モデル選択・early stop  [S - 1年, S - 6か月)
  calib   確率補正・市場との結合  [S - 6か月, S - 3か月)
  betsel  購入条件の選択          [S - 3か月, S)
  outer   外側評価                [S, S + 6か月)
最終確認期間（FINAL_PERIOD_START 以降）に触れる fold は、明示しない限り作らない。
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# 計画 5.2 の初期案。ユーザーの決定まで、ここ以降は開発で読まない
FINAL_PERIOD_START = pd.Timestamp('2026-07-01')
SEGMENTS = ('fit', 'select', 'calib', 'betsel', 'outer')


class FinalPeriodError(ValueError):
    """最終確認期間に触れようとした。"""


@dataclass(frozen=True)
class Fold:
    name: str
    fit: tuple[pd.Timestamp, pd.Timestamp]
    select: tuple[pd.Timestamp, pd.Timestamp]
    calib: tuple[pd.Timestamp, pd.Timestamp]
    betsel: tuple[pd.Timestamp, pd.Timestamp]
    outer: tuple[pd.Timestamp, pd.Timestamp]

    def segment(self, name: str) -> tuple[pd.Timestamp, pd.Timestamp]:
        return getattr(self, name)

    def to_dict(self) -> dict:
        """区分ごとの [開始, 終了) を日付文字列で返す。"""
        out = {'name': self.name}
        for s in SEGMENTS:
            a, b = self.segment(s)
            out[s] = [str(a.date()), str(b.date())]
        return out

    def mask(self, dates: pd.Series, segment: str) -> pd.Series:
        a, b = self.segment(segment)
        d = pd.to_datetime(dates)
        return (d >= a) & (d < b)


def make_rolling_folds(outer_starts: list[str], fit_years: int = 4, outer_months: int = 6,
                       allow_final: bool = False) -> list[Fold]:
    folds = []
    for s in outer_starts:
        S = pd.Timestamp(s)
        outer_end = S + pd.DateOffset(months=outer_months)
        if not allow_final and outer_end > FINAL_PERIOD_START:
            raise FinalPeriodError(
                f'fold {s} reaches {outer_end.date()}, beyond the final-check period start {FINAL_PERIOD_START.date()}')
        y1 = S - pd.DateOffset(years=1)
        folds.append(Fold(
            name=f'outer_{S.strftime("%Y%m")}',
            fit=(y1 - pd.DateOffset(years=fit_years), y1),
            select=(y1, S - pd.DateOffset(months=6)),
            calib=(S - pd.DateOffset(months=6), S - pd.DateOffset(months=3)),
            betsel=(S - pd.DateOffset(months=3), S),
            outer=(S, outer_end),
        ))
    return folds
