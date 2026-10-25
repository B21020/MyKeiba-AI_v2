"""P10: 特徴量エンジニアリング + 複勝 HPO 評価

既存 featured_data に新特徴量を追加し、P9-2 と同条件で HPO + Bootstrap 評価を行う。

### 新特徴量カテゴリ (約50-60新カラム):
1. 安定性指標 (std/cv) — 過去成績の分散・変動係数
2. 比率特徴量 — 斤量/体重, course_len/体重 等
3. トレンド特徴量 — 直近5R vs 全R の差分 (上昇/下降傾向)
4. 騎手×条件クロス — 騎手の芝/ダート別複勝率 (JockeyStats拡張)
5. 人気順位 — 単勝オッズのレース内ランク
6. 枠番バイアス — 枠番のレース内相対位置
7. クロス集約 race-relative — course_len別, race_type別 の相対特徴量

Usage:
  # Quick test (3 trials)
  .venv\\Scripts\\python.exe tools/run_p10_feature_engineering.py --n-trials 3 --n-boot 50

  # Full run
  .venv\\Scripts\\python.exe tools/run_p10_feature_engineering.py --n-trials 150 --n-boot 2000
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# Force UTF-8 on Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BASE_PICKLE = "data/tmp/featured_data_20260205_rel_ext.pickle"
P10_PICKLE = "data/tmp/featured_data_20260205_p10.pickle"


# =====================================================================
# Feature Engineering Functions
# =====================================================================

def add_stability_features(df: pd.DataFrame) -> pd.DataFrame:
    """過去成績の分散・変動係数を追加。

    既存の mean 集約だけでは「安定した馬」と「ムラのある馬」を区別できない。
    std (標準偏差) と cv (変動係数 = std/mean) を追加する。

    ただし raw horse_results から再集約するのではなく、
    5R vs 9R vs allR の差分から安定性を近似する (既存カラムのみで計算可能)。
    """
    new_cols = {}

    # 着順の window 間差分 → 成績トレンドと安定性
    for target in ['着順', '賞金', '着差', 'time_seconds']:
        c5 = f'{target}_5R'
        c9 = f'{target}_9R'
        cAll = f'{target}_allR'

        if c5 in df.columns and c9 in df.columns and cAll in df.columns:
            v5 = df[c5].astype('float32')
            v9 = df[c9].astype('float32')
            vAll = df[cAll].astype('float32')

            # 3 window の標準偏差 = 成績のバラつき
            stacked = np.column_stack([v5.values, v9.values, vAll.values])
            new_cols[f'{target}_window_std'] = np.nanstd(stacked, axis=1).astype('float32')

            # mean across windows
            win_mean = np.nanmean(stacked, axis=1)
            win_mean_safe = np.where(np.abs(win_mean) < 1e-6, 1e-6, win_mean)
            new_cols[f'{target}_window_cv'] = (
                np.nanstd(stacked, axis=1) / np.abs(win_mean_safe)
            ).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    """直近 5R vs 全体 (allR) の差分 = トレンド特徴量。

    正の値 → 最近悪化 (着順が大きい = 悪い)
    負の値 → 最近改善
    """
    new_cols = {}

    for target in ['着順', '賞金', '着差', 'time_seconds',
                    'first_corner', 'final_corner', 'first_to_rank', 'final_to_rank']:
        c5 = f'{target}_5R'
        cAll = f'{target}_allR'
        if c5 in df.columns and cAll in df.columns:
            new_cols[f'{target}_trend'] = (df[c5] - df[cAll]).astype('float32')

    # 条件別トレンド (course_len, race_type)
    for group in ['course_len', 'race_type']:
        for target in ['着順', 'time_seconds']:
            c5 = f'{target}_{group}_5R'
            cAll = f'{target}_{group}_allR'
            if c5 in df.columns and cAll in df.columns:
                new_cols[f'{target}_{group}_trend'] = (df[c5] - df[cAll]).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
    """比率・交差特徴量。

    LightGBM は乗除を直接学習しにくいため、明示的な比率が有効。
    """
    new_cols = {}

    # 斤量 / 体重 = 負担重量比率 (馬体重に対する斤量の比率)
    if '斤量' in df.columns and '体重' in df.columns:
        weight_safe = df['体重'].replace(0, np.nan).astype('float32')
        new_cols['斤量_体重_ratio'] = (df['斤量'].astype('float32') / weight_safe).astype('float32')

    # 距離 / 体重 = 距離適性 (大きい馬は長距離向き?)
    if 'course_len' in df.columns and '体重' in df.columns:
        new_cols['course_len_体重_ratio'] = (
            df['course_len'].astype('float32') / weight_safe
        ).astype('float32')

    # 過去成績 / 出走頭数 = 正規化着順
    if '着順_5R' in df.columns and 'n_horses' in df.columns:
        nh = df['n_horses'].replace(0, np.nan).astype('float32')
        new_cols['着順_5R_per_nhorses'] = (df['着順_5R'].astype('float32') / nh).astype('float32')
        if '着順_allR' in df.columns:
            new_cols['着順_allR_per_nhorses'] = (df['着順_allR'].astype('float32') / nh).astype('float32')

    # 賞金 / 出走レース数的近似 → 1Rあたり平均賞金 (allR平均はこれだが5R/allRの比が有効)
    if '賞金_5R' in df.columns and '賞金_allR' in df.columns:
        prize_all_safe = df['賞金_allR'].replace(0, np.nan).astype('float32')
        new_cols['賞金_5R_vs_allR_ratio'] = (
            df['賞金_5R'].astype('float32') / prize_all_safe
        ).astype('float32')

    # タイム / 距離 = 速度的指標 (秒/100m)
    if 'time_seconds_5R' in df.columns and 'course_len' in df.columns:
        cl = df['course_len'].replace(0, np.nan).astype('float32')
        new_cols['speed_5R'] = (df['time_seconds_5R'].astype('float32') / cl * 100).astype('float32')
    if 'time_seconds_allR' in df.columns and 'course_len' in df.columns:
        new_cols['speed_allR'] = (df['time_seconds_allR'].astype('float32') / cl * 100).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_odds_rank_features(df: pd.DataFrame) -> pd.DataFrame:
    """オッズ系レース内ランク特徴量。

    単勝オッズのレース内順位 = 人気順位。
    """
    new_cols = {}

    if '単勝' in df.columns and 'race_id' in df.columns:
        odds = pd.to_numeric(df['単勝'], errors='coerce').astype('float32')
        grouped = odds.groupby(df['race_id'])

        # 人気順位 (オッズ昇順 = 1番人気)
        new_cols['odds_rank'] = grouped.rank(method='min', ascending=True).astype('float32')

        # 人気と出走頭数の比率
        if 'n_horses' in df.columns:
            nh = df['n_horses'].replace(0, np.nan).astype('float32')
            new_cols['odds_rank_pct'] = (new_cols['odds_rank'] / nh).astype('float32')

        # log(オッズ) — 分布の歪みを緩和
        new_cols['log_odds'] = np.log1p(odds.clip(lower=0)).astype('float32')

        # オッズ z-score within race
        race_mean = grouped.transform('mean')
        race_std = grouped.transform('std').fillna(0).clip(lower=1e-6)
        new_cols['odds_zR'] = ((odds - race_mean) / race_std).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_frame_features(df: pd.DataFrame) -> pd.DataFrame:
    """枠番系特徴量。

    内枠/外枠のバイアスをモデルが拾いやすくする。
    """
    new_cols = {}

    if '枠番' in df.columns and 'n_horses' in df.columns:
        frame = df['枠番'].astype('float32')
        nh = df['n_horses'].replace(0, np.nan).astype('float32')

        # 枠番 / 頭数 = 相対枠位置 (0に近い=内枠)
        new_cols['frame_pct'] = (frame / nh).astype('float32')

    if '馬番' in df.columns and 'n_horses' in df.columns:
        umaban = df['馬番'].astype('float32')
        nh = df['n_horses'].replace(0, np.nan).astype('float32')

        # 馬番 / 頭数 = 相対馬番位置
        new_cols['umaban_pct'] = (umaban / nh).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_cross_relative_features(df: pd.DataFrame) -> pd.DataFrame:
    """条件別集約のレース内相対化。

    既存: 着順_course_len_5R (馬の同距離成績)
    新規: 着順_course_len_5R_zR (レース内でその値がどの位置か)

    条件別成績のレース内zスコア = 「同距離の過去成績がレースメンバー中でどの位置か」
    """
    new_cols = {}
    eps = 1e-6

    cross_targets = []
    for target in ['着順', 'time_seconds', '賞金']:
        for group in ['course_len', 'race_type']:
            for window in ['5R', 'allR']:
                col = f'{target}_{group}_{window}'
                if col in df.columns:
                    cross_targets.append(col)

    if 'race_id' not in df.columns or not cross_targets:
        return df

    for col in cross_targets:
        grouped = df.groupby('race_id')[col]
        race_mean = grouped.transform('mean')
        race_std = grouped.transform('std').fillna(0).clip(lower=eps)
        new_cols[f'{col}_zR'] = ((df[col] - race_mean) / race_std).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def add_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """高次交互作用特徴量。

    着順 × 騎手複勝率 = 馬の実力 + 騎手の腕の合成指標
    """
    new_cols = {}

    # 着順(5R平均) × 騎手複勝率 → 馬+騎手の合成指標
    if '着順_5R' in df.columns and 'jockey_plc_rate_50_all' in df.columns:
        rank5 = df['着順_5R'].astype('float32')
        jplc = df['jockey_plc_rate_50_all'].astype('float32')
        # 着順が小さい(良い) × 騎手複勝率が高い → 小さい値 = 好成績期待
        new_cols['rank5_x_jockey_plc'] = (rank5 * (1 - jplc)).astype('float32')

    # 体重変化の絶対値 = 調整の大きさ
    if '体重変化' in df.columns:
        new_cols['abs_weight_change'] = df['体重変化'].abs().astype('float32')

    # 年齢 × interval = 高齢 + 長期休養 → リスク指標
    if '年齢' in df.columns and 'interval' in df.columns:
        new_cols['age_x_interval'] = (
            df['年齢'].astype('float32') * df['interval'].astype('float32')
        ).astype('float32')

    return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)


def build_p10_features(base_path: str, out_path: str) -> str:
    """P10 特徴量を構築して保存する。"""
    if os.path.isfile(out_path):
        fd = pd.read_pickle(out_path)
        if any(c.endswith('_trend') for c in fd.columns):
            print(f"  P10 data already exists: {out_path} ({fd.shape[1]} cols)")
            return out_path

    print(f"  Building P10 features from {base_path}...")
    t0 = time.time()
    fd = pd.read_pickle(base_path)
    n_before = fd.shape[1]

    # 1. 安定性指標
    fd = add_stability_features(fd)
    n1 = fd.shape[1] - n_before
    print(f"    [1] Stability features: +{n1} cols")

    # 2. トレンド特徴量
    n_b = fd.shape[1]
    fd = add_trend_features(fd)
    n2 = fd.shape[1] - n_b
    print(f"    [2] Trend features: +{n2} cols")

    # 3. 比率特徴量
    n_b = fd.shape[1]
    fd = add_ratio_features(fd)
    n3 = fd.shape[1] - n_b
    print(f"    [3] Ratio features: +{n3} cols")

    # 4. オッズ系ランク特徴量
    n_b = fd.shape[1]
    fd = add_odds_rank_features(fd)
    n4 = fd.shape[1] - n_b
    print(f"    [4] Odds/rank features: +{n4} cols")

    # 5. 枠番系特徴量
    n_b = fd.shape[1]
    fd = add_frame_features(fd)
    n5 = fd.shape[1] - n_b
    print(f"    [5] Frame features: +{n5} cols")

    # 6. 条件別クロス相対化
    n_b = fd.shape[1]
    fd = add_cross_relative_features(fd)
    n6 = fd.shape[1] - n_b
    print(f"    [6] Cross-relative features: +{n6} cols")

    # 7. 交互作用特徴量
    n_b = fd.shape[1]
    fd = add_interaction_features(fd)
    n7 = fd.shape[1] - n_b
    print(f"    [7] Interaction features: +{n7} cols")

    total_new = fd.shape[1] - n_before
    print(f"  Total: {n_before} -> {fd.shape[1]} cols (+{total_new}) in {time.time()-t0:.1f}s")

    fd.to_pickle(out_path)
    print(f"  Saved: {out_path}")
    return out_path


# =====================================================================
# Main: Build features + run P9-style HPO
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="P10: Feature Engineering + Fukusho HPO")
    parser.add_argument("--n-trials", type=int, default=150)
    parser.add_argument("--n-sweep-obj", type=int, default=30)
    parser.add_argument("--n-sweep-test", type=int, default=200)
    parser.add_argument("--min-bets-obj", type=int, default=30)
    parser.add_argument("--min-bets-test", type=int, default=50)
    parser.add_argument("--max-bets-per-race", type=int, default=3)
    parser.add_argument("--objective-mode", type=str, default="rr", choices=["rr", "profit"])
    parser.add_argument("--n-boot", type=int, default=2000)
    parser.add_argument("--out-dir", type=str, default="models/20260209/p10")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-hpo", action="store_true", help="Skip HPO and only build features")
    args = parser.parse_args()

    # Step 1: Build P10 features
    print("=" * 60)
    print("Step 1: Build P10 features")
    print("=" * 60)
    p10_path = build_p10_features(BASE_PICKLE if not os.path.isfile(BASE_PICKLE) else BASE_PICKLE,
                                   P10_PICKLE)
    # Verify: use rel_ext as base if that's what exists
    if not os.path.isfile(P10_PICKLE):
        p10_path = build_p10_features(
            "data/tmp/featured_data_20260205_rel_ext.pickle",
            P10_PICKLE,
        )

    if args.skip_hpo:
        print("\n--skip-hpo: Done.")
        return 0

    # Step 2: Run P9-style HPO with P10 features
    # We reuse run_p9_fukusho_hpo.py via subprocess to avoid duplicating 1000 lines
    print("\n" + "=" * 60)
    print("Step 2: Run Fukusho HPO with P10 features")
    print("=" * 60)

    import subprocess
    cmd = [
        sys.executable,
        str(REPO_ROOT / "tools" / "run_p9_fukusho_hpo.py"),
        "--featured", p10_path,
        "--n-trials", str(args.n_trials),
        "--n-sweep-obj", str(args.n_sweep_obj),
        "--n-sweep-test", str(args.n_sweep_test),
        "--min-bets-obj", str(args.min_bets_obj),
        "--min-bets-test", str(args.min_bets_test),
        "--max-bets-per-race", str(args.max_bets_per_race),
        "--objective-mode", args.objective_mode,
        "--n-boot", str(args.n_boot),
        "--out-dir", args.out_dir,
        "--seed", str(args.seed),
    ]
    print(f"  CMD: {' '.join(cmd)}")
    ret = subprocess.run(cmd)
    return ret.returncode


if __name__ == "__main__":
    raise SystemExit(main())
