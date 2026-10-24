"""条件補正した走破能力指数（再開計画 施策D）。

1. 基準タイム：(場, 芝ダ, 距離) ごとの走破時計の中央値。cutoff より前の走だけで推定する（学習期間のみ）
2. 馬場差：同じ日・同じ場・同じ芝ダの走の「基準からのずれ（比）」の中央値（その日の馬場の速さ）
3. 指数：-(基準からのずれ − 馬場差) × 1000（基準より何‰速いか）
4. 予測対象のレースには、そのレースの日付より前の走だけから集計した値を付ける（同日の走は使わない）

障害・時計の無い走（中止など）・基準タイムの無い条件は指数なし（欠損）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SI_FEATURES = ['si_last', 'si_mean3', 'si_mean5', 'si_max5', 'si_n5']
MIN_BASE_RUNS = 30
MIN_DAY_RUNS = 20


def parse_history(h: pd.DataFrame) -> pd.DataFrame:
    """馬戦績（旧 raw と同じ列名、index=horse_id）を指数の計算に使う形にする。"""
    venue = h['開催'].astype(str).str.extract(r'^\d*([^\d]+)\d*$')[0]
    dist = h['距離'].astype(str).str.extract(r'^(芝|ダ)\D*(\d+)$')
    t = h['タイム'].astype(str).str.extract(r'^(\d+):(\d{2}(?:\.\d+)?)$')
    runs = pd.DataFrame({
        'horse_id': h.index.astype(str),
        'date': pd.to_datetime(h['日付'], errors='coerce'),
        'venue': venue,
        'surface': dist[0],
        'distance': pd.to_numeric(dist[1], errors='coerce'),
        'time_sec': pd.to_numeric(t[0], errors='coerce') * 60 + pd.to_numeric(t[1], errors='coerce'),
    })
    runs = runs.dropna(subset=['date', 'venue', 'surface', 'distance', 'time_sec']).reset_index(drop=True)
    return runs


def fit_base_times(runs: pd.DataFrame, cutoff) -> pd.DataFrame:
    """cutoff より前の走から、(場, 芝ダ, 距離) ごとの基準タイムを推定する。"""
    past = runs[runs['date'] < pd.Timestamp(cutoff)]
    g = past.groupby(['venue', 'surface', 'distance'])['time_sec']
    base = pd.DataFrame({'base_time': g.median(), 'base_n': g.size()}).reset_index()
    return base[base['base_n'] >= MIN_BASE_RUNS]


def speed_index(runs: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """各走に指数を付ける（基準タイムの無い条件は欠損）。"""
    r = runs.merge(base[['venue', 'surface', 'distance', 'base_time']], on=['venue', 'surface', 'distance'], how='left')
    r['rel'] = (r['time_sec'] - r['base_time']) / r['base_time']
    day = r.groupby(['date', 'venue', 'surface'])['rel']
    r['day_variant'] = day.transform('median').where(day.transform('count') >= MIN_DAY_RUNS, 0.0)
    r['si'] = -(r['rel'] - r['day_variant']) * 1000
    return r


def horse_speed_features(runs_si: pd.DataFrame, targets: pd.DataFrame) -> pd.DataFrame:
    """targets（horse_id, date を持つ）に、その日付より前の走から集計した指数を付ける。

    戻り値は targets と同じ index・行順で SI_FEATURES の列を持つ。
    """
    r = runs_si.sort_values(['horse_id', 'date']).copy()
    g = r.groupby('horse_id')['si']
    r['si_last'] = r['si']
    r['si_mean3'] = g.transform(lambda s: s.rolling(3, min_periods=1).mean())
    r['si_mean5'] = g.transform(lambda s: s.rolling(5, min_periods=1).mean())
    r['si_max5'] = g.transform(lambda s: s.rolling(5, min_periods=1).max())
    r['si_n5'] = g.transform(lambda s: s.notna().astype(float).rolling(5, min_periods=1).sum())
    # 同じ日に複数走は無い前提だが、念のため日付ごとに最後の行を使う
    r = r.drop_duplicates(['horse_id', 'date'], keep='last')
    t = targets[['horse_id', 'date']].copy()
    t['_row'] = np.arange(len(t))
    t['date'] = pd.to_datetime(t['date'])
    t['horse_id'] = t['horse_id'].astype(str)
    merged = pd.merge_asof(t.sort_values('date'), r[['horse_id', 'date', *SI_FEATURES]].sort_values('date'),
                           on='date', by='horse_id', allow_exact_matches=False, direction='backward')
    merged = merged.sort_values('_row')
    out = merged[SI_FEATURES].set_index(targets.index)
    return out
