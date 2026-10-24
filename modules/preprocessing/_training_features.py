"""調教データ（坂路 HC・ウッドチップ WC）からレース前に分かる特徴量を作る。

- 同じ馬・日付・時刻・トレセンの重複はデータ作成日が新しいものを使う（修正レコードを反映）
- 時計の速さは、同じトレセン・同じ日（ウッドは同じコースも）の中の z（小さいほど速い時計を、符号を反転して「速いほど正」に）
  にそろえる（その日の馬場や風の影響を除くため）
- 予測するレースの日付より前（当日を含まない）WINDOW_DAYS 日以内の調教だけを使う
"""
from __future__ import annotations

import numpy as np
import pandas as pd

WINDOW_DAYS = 14
TRAINING_FEATURES = ['tr_n14', 'tr_days_since', 'hill_n14', 'hill_last_fast', 'hill_best_fast', 'hill_last_lap1_fast',
                     'wood_n14', 'wood_last_fast', 'wood_last_lap1_fast']


def _dedup(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    df = df.sort_values('make_date')
    return df.drop_duplicates(keys, keep='last')


def _fast_z(df: pd.DataFrame, col: str, group: list[str]) -> pd.Series:
    g = df.groupby(group)[col]
    z = (df[col] - g.transform('mean')) / g.transform('std')
    return -z  # 速いほど正


def load_workouts(hill: pd.DataFrame, wood: pd.DataFrame) -> pd.DataFrame:
    """坂路とウッドを1つの表（1行=1本の調教）にする。"""
    keys = ['horse_id', 'date', 'time', 'tresen']
    h = _dedup(hill.assign(horse_id=hill['horse_id'].astype(str)), keys)
    w = _dedup(wood.assign(horse_id=wood['horse_id'].astype(str)), keys)
    for d in (h, w):
        d['date'] = pd.to_datetime(d['date'])
    h = h.dropna(subset=['h4'])
    h = h.assign(kind='hill', fast=_fast_z(h, 'h4', ['tresen', 'date']), lap1_fast=_fast_z(h, 'l1', ['tresen', 'date']))
    w = w.dropna(subset=['h3'])
    w = w.assign(kind='wood', fast=_fast_z(w, 'h3', ['tresen', 'date', 'course']),
                 lap1_fast=_fast_z(w, 'l1', ['tresen', 'date', 'course']))
    cols = ['horse_id', 'date', 'time', 'kind', 'fast', 'lap1_fast']
    return pd.concat([h[cols], w[cols]], ignore_index=True)


def training_features(workouts: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """targets（horse_id, date）ごとに、その日付より前 WINDOW_DAYS 日以内の調教から特徴量を作る。

    戻り値は targets と同じ index で TRAINING_FEATURES の列を持つ（調教が無ければ件数0、他は欠損）。
    """
    t = targets[['horse_id', 'date']].copy()
    t['horse_id'] = t['horse_id'].astype(str)
    t['date'] = pd.to_datetime(t['date'])
    t['_row'] = np.arange(len(t))
    m = t.merge(workouts, on='horse_id', how='inner', suffixes=('', '_w'))
    m = m[(m['date_w'] < m['date']) & (m['date_w'] >= m['date'] - pd.Timedelta(days=WINDOW_DAYS))]
    m = m.sort_values(['_row', 'date_w', 'time'])
    out = pd.DataFrame(index=np.arange(len(t)))
    g = m.groupby('_row')
    out['tr_n14'] = g.size()
    out['tr_days_since'] = (g['date'].first() - g['date_w'].max()).dt.days
    for kind in ('hill', 'wood'):
        k = m[m['kind'] == kind]
        gk = k.groupby('_row')
        out[f'{kind}_n14'] = gk.size()
        out[f'{kind}_last_fast'] = gk['fast'].last()
        out[f'{kind}_last_lap1_fast'] = gk['lap1_fast'].last()
        if kind == 'hill':
            out['hill_best_fast'] = gk['fast'].max()
    for c in ('tr_n14', 'hill_n14', 'wood_n14'):
        out[c] = out[c].fillna(0)
    out = out.reindex(columns=TRAINING_FEATURES)
    out.index = targets.index
    return out
