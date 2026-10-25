"""netkeiba の馬の戦績表（AJAX 断片）の列ずれを直す。

公開版の戦績表は、有料の列（「**」で伏せた4列）をデータ行にだけ含むため、
pd.read_html で読むと「通過」以降の値が4列右にずれる。
  通過 → 厩舎コメント、ペース → 備考、上り → 勝ち馬(2着馬)、馬体重 → 賞金、
  勝ち馬(2着馬) → Unnamed:31、賞金 → Unnamed:32
旧スクレイパー（data/html/horse の HTML）の表はずれていない（29列）。
"""
from __future__ import annotations

import pandas as pd

CORNER_RE = r'^\d+(?:-\d+)*$'
PACE_RE = r'^\d+\.\d-\d+\.\d$'
WEIGHT_RE = r'^(?:\d{3}\([+\-]?\d+\)|\d{3}|計不)$'

# ずれた列 -> 本来の列
SHIFTED_TO_TRUE = {
    '厩舎コメント': '通過',
    '備考': 'ペース',
    '勝ち馬(2着馬)': '上り',
    '賞金': '馬体重',
    'Unnamed:31': '勝ち馬(2着馬)',
    'Unnamed:32': '賞金',
}
# 正規化(NFKC)後の列名 -> 旧スクレイパー由来の raw の列名
TO_LEGACY_NAMES = {'タイム指数': 'ﾀｲﾑ指数', '厩舎コメント': '厩舎ｺﾒﾝﾄ'}
LEGACY_COLUMNS = [
    '日付', '開催', '天気', 'R', 'レース名', '映像', '頭数', '枠番', '馬番', 'オッズ', '人気', '着順', '騎手',
    '斤量', '距離', '水分量', '馬場', '馬場指数', 'タイム', '着差', 'ﾀｲﾑ指数', '通過', 'ペース', '上り',
    '馬体重', '厩舎ｺﾒﾝﾄ', '備考', '勝ち馬(2着馬)', '賞金',
]


def _matches(s: pd.Series, pattern: str) -> pd.Series:
    return s.astype('string').str.strip().str.fullmatch(pattern).fillna(False).astype(bool)


def shifted_rows(df: pd.DataFrame) -> pd.Series:
    """値が4列右にずれている行。"""
    if 'Unnamed:31' not in df.columns:
        return pd.Series(False, index=df.index)
    empty = pd.Series(None, index=df.index, dtype=object)
    comment = df['厩舎コメント'] if '厩舎コメント' in df.columns else df.get('厩舎ｺﾒﾝﾄ', empty)
    return (
        _matches(comment, CORNER_RE)
        | _matches(df.get('備考', empty), PACE_RE)
        | _matches(df.get('賞金', empty), WEIGHT_RE)
        | df['Unnamed:31'].notna()
    )


def realign_public_horse_history(df: pd.DataFrame) -> pd.DataFrame:
    """ずれた行を本来の列に戻し、旧 raw と同じ列名・列順にする。

    ずれた行では、伏せ字の列（通過〜馬体重の位置にある「**」）と、本来の厩舎コメント・備考は取得できないので欠損にする。
    """
    out = df.rename(columns={v: k for k, v in TO_LEGACY_NAMES.items()})
    mask = shifted_rows(out)
    if mask.any():
        moved = {true: out.loc[mask, src] for src, true in SHIFTED_TO_TRUE.items() if src in out.columns}
        for col in ('通過', 'ペース', '上り', '馬体重', '厩舎コメント', '備考', '勝ち馬(2着馬)', '賞金'):
            if col in out.columns:
                out[col] = out[col].astype(object)
                out.loc[mask, col] = None
        for true, values in moved.items():
            out.loc[mask, true] = values
    out = out.rename(columns=TO_LEGACY_NAMES)
    for c in LEGACY_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out.loc[:, LEGACY_COLUMNS]
