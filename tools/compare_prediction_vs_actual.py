"""
2/14・2/15 レースの予測スコア vs 実際の着順を照合してCSVを出力するスクリプト。

使い方:
    python tools/compare_prediction_vs_actual.py

出力:
    data/prediction_vs_actual_20260214.csv
    data/prediction_vs_actual_20260215.csv
    data/prediction_vs_actual_20260214_20260215.csv  (統合版)
"""
import os
import sys
import glob
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from modules import preprocessing, policies
from modules.constants import Master, ResultsCols

# ============================================================
# 設定
# ============================================================
SCORE_DIR = 'data/score_tables'
RESULTS_PICKLE = 'data/raw/results.pickle'
RETURN_PICKLE = 'data/raw/return_tables.pickle'
OUTPUT_DIR = 'data'

FUKUSHO_THRESHOLD = 3.136  # P11-ext 賭け閾値

TARGET_DATES = {
    '20260214': [],   # race_id リストは後で収集
    '20260215': [],
}

# ============================================================
# 1) スコアCSV の収集
# ============================================================
print("=== 1) スコアCSV の収集 ===")
date_race_scores: dict[str, dict[str, pd.DataFrame]] = {}

for date_str in TARGET_DATES:
    pattern = os.path.join(SCORE_DIR, f'{date_str}_*.csv')
    csv_files = sorted(glob.glob(pattern))
    print(f"  {date_str}: {len(csv_files)} score CSVs found")

    race_scores = {}
    for csv_path in csv_files:
        basename = os.path.basename(csv_path).replace('.csv', '')
        # 形式: 20260214_202605010501
        parts = basename.split('_', 1)
        if len(parts) != 2:
            continue
        race_id = parts[1]
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        # index列の整理
        if 'race_id' in df.columns:
            pass
        elif 'index' in df.columns:
            pass
        df['race_id'] = race_id
        race_scores[race_id] = df

    date_race_scores[date_str] = race_scores
    TARGET_DATES[date_str] = sorted(race_scores.keys())

# ============================================================
# 2) 実際の着順データを取得
# ============================================================
print("\n=== 2) 実際の着順データを取得 ===")
results_all = pd.read_pickle(RESULTS_PICKLE)
print(f"  results total: {len(results_all)} rows")

# race_id はインデックスに入っている
all_target_race_ids = []
for ids in TARGET_DATES.values():
    all_target_race_ids.extend(ids)

results_target = results_all.loc[results_all.index.isin(all_target_race_ids)].copy()
results_target['race_id'] = results_target.index
print(f"  filtered to target races: {len(results_target)} rows")

# 着順を数値化（中止・除外等はNaN）
results_target['着順_num'] = pd.to_numeric(results_target['着順'], errors='coerce')
# 馬番を数値化
results_target['馬番_num'] = pd.to_numeric(results_target['馬番'], errors='coerce')

# ============================================================
# 3) 複勝払戻データを取得
# ============================================================
print("\n=== 3) 複勝払戻データを取得 ===")
return_processor = preprocessing.ReturnProcessor(RETURN_PICKLE)
fukusho_table = return_processor.preprocessed_data['fukusho']
tansho_table = return_processor.preprocessed_data['tansho']
print(f"  fukusho: {len(fukusho_table)} races")

# ============================================================
# 4) 統合テーブルの作成
# ============================================================
print("\n=== 4) 統合テーブルの作成 ===")

def build_comparison_df(date_str: str, race_scores: dict) -> pd.DataFrame:
    """日付ごとに予測 vs 実績の比較テーブルを作成"""
    rows = []

    for race_id, score_df in sorted(race_scores.items()):
        # --- スコアテーブル整理 ---
        if '馬番' in score_df.columns:
            umaban_col = '馬番'
        elif ResultsCols.UMABAN in score_df.columns:
            umaban_col = ResultsCols.UMABAN
        else:
            # 推定
            for c in score_df.columns:
                if 'umaban' in str(c).lower() or '馬番' in str(c):
                    umaban_col = c
                    break
            else:
                print(f"  [WARN] {race_id}: 馬番列不明, skip")
                continue

        score_df['馬番_int'] = pd.to_numeric(score_df[umaban_col], errors='coerce').astype('Int64')
        score_df = score_df.sort_values('score', ascending=False).reset_index(drop=True)
        score_df['score_rank'] = range(1, len(score_df) + 1)

        # --- 実着順と結合 ---
        race_results = results_target[results_target['race_id'] == race_id].copy()
        if race_results.empty:
            print(f"  [WARN] {race_id}: 着順データなし")
            continue

        # --- 複勝払戻の取得 ---
        try:
            fuku = fukusho_table.loc[race_id]
            fuku_wins = [int(fuku[f'win_{i}']) for i in range(3) if pd.notna(fuku.get(f'win_{i}', np.nan))]
            fuku_returns = [int(fuku[f'return_{i}']) for i in range(3) if pd.notna(fuku.get(f'return_{i}', np.nan))]
        except KeyError:
            fuku_wins = []
            fuku_returns = []

        # --- 単勝払戻の取得 ---
        try:
            tan = tansho_table.loc[race_id]
            if isinstance(tan, pd.DataFrame):
                tan = tan.iloc[0]
            tansho_win = int(tan['win']) if pd.notna(tan.get('win', np.nan)) else None
            tansho_return = int(tan['return']) if pd.notna(tan.get('return', np.nan)) else None
        except KeyError:
            tansho_win = None
            tansho_return = None

        # --- 賭け判定 ---
        st_for_judge = score_df.copy()
        if 'race_id' not in st_for_judge.columns:
            st_for_judge['race_id'] = race_id
        st_for_judge.index = [race_id] * len(st_for_judge)
        # 馬番列名をResultsCols.UMABANに揃える
        if umaban_col != ResultsCols.UMABAN:
            st_for_judge[ResultsCols.UMABAN] = st_for_judge[umaban_col]

        actions = policies.BetPolicyFukusho.judge(st_for_judge, threshold=FUKUSHO_THRESHOLD)
        bet_umabans = actions.get(race_id, {}).get('fukusho', [])
        bet_umabans_int = [int(x) for x in bet_umabans]

        # --- 場所名の取得 ---
        place_code = race_id[4:6]
        place_name = next((n for n, c in Master.PLACE_DICT.items() if c == place_code), "??")
        race_num = race_id[10:12]

        # --- 各馬のレコード作成 ---
        for _, srow in score_df.iterrows():
            uma = srow['馬番_int']
            if pd.isna(uma):
                continue
            uma = int(uma)

            # 着順
            r_row = race_results[race_results['馬番_num'] == uma]
            if not r_row.empty:
                actual_rank = r_row.iloc[0]['着順_num']
                horse_name = r_row.iloc[0]['馬名'] if '馬名' in r_row.columns else ''
                popularity = r_row.iloc[0]['人気'] if '人気' in r_row.columns else np.nan
            else:
                actual_rank = np.nan
                horse_name = ''
                popularity = np.nan

            # 3着以内か
            is_top3 = (actual_rank <= 3) if pd.notna(actual_rank) else False

            # 複勝的中か
            is_fukusho_hit = uma in fuku_wins
            fukusho_payout = 0
            if is_fukusho_hit:
                idx = fuku_wins.index(uma)
                if idx < len(fuku_returns):
                    fukusho_payout = fuku_returns[idx]

            # 賭け対象か
            is_bet = uma in bet_umabans_int

            # 賭けた場合の損益
            bet_result = ''
            bet_profit = 0
            if is_bet:
                if is_fukusho_hit:
                    bet_result = '的中'
                    bet_profit = fukusho_payout - 100  # 100円賭け
                else:
                    bet_result = 'ハズレ'
                    bet_profit = -100

            rows.append({
                'date': date_str,
                'place': place_name,
                'R': int(race_num),
                'race_id': race_id,
                '馬番': uma,
                '馬名': horse_name,
                'score': round(srow['score'], 4),
                'score_rank': srow['score_rank'],
                '人気': int(popularity) if pd.notna(popularity) else '',
                '着順': int(actual_rank) if pd.notna(actual_rank) else '',
                '3着以内': '○' if is_top3 else '',
                '複勝対象': '○' if is_bet else '',
                '複勝結果': bet_result,
                '複勝払戻(円)': fukusho_payout if is_fukusho_hit else '',
                '複勝損益(円)': bet_profit if is_bet else '',
            })

    df = pd.DataFrame(rows)
    return df


# 各日付ごとに作成
all_dfs = []
for date_str, race_scores in date_race_scores.items():
    df = build_comparison_df(date_str, race_scores)
    all_dfs.append(df)

    csv_path = os.path.join(OUTPUT_DIR, f'prediction_vs_actual_{date_str}.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"  saved: {csv_path} ({len(df)} rows)")

    # --- サマリー ---
    bets = df[df['複勝対象'] == '○']
    n_bets = len(bets)
    n_hits = len(bets[bets['複勝結果'] == '的中'])
    total_invest = n_bets * 100
    total_return_val = bets['複勝払戻(円)'].replace('', 0).astype(int).sum()

    print(f"\n  【{date_str} サマリー】")
    print(f"    レース数: {df['race_id'].nunique()}")
    print(f"    賭け点数: {n_bets}")
    print(f"    的中数: {n_hits}")
    print(f"    的中率: {n_hits / n_bets * 100:.1f}%" if n_bets > 0 else "    的中率: N/A")
    print(f"    投資額: {total_invest}円")
    print(f"    回収額: {total_return_val}円")
    print(f"    回収率: {total_return_val / total_invest * 100:.1f}%" if total_invest > 0 else "    回収率: N/A")
    print(f"    損益: {total_return_val - total_invest}円")
    print()

# 統合版
if all_dfs:
    combined = pd.concat(all_dfs, ignore_index=True)
    combined_path = os.path.join(OUTPUT_DIR, 'prediction_vs_actual_20260214_20260215.csv')
    combined.to_csv(combined_path, index=False, encoding='utf-8-sig')
    print(f"  saved (combined): {combined_path} ({len(combined)} rows)")

    # 全体サマリー
    bets = combined[combined['複勝対象'] == '○']
    n_bets = len(bets)
    n_hits = len(bets[bets['複勝結果'] == '的中'])
    total_invest = n_bets * 100
    total_return_val = bets['複勝払戻(円)'].replace('', 0).astype(int).sum()

    print(f"\n  【全体サマリー (2/14 + 2/15)】")
    print(f"    レース数: {combined['race_id'].nunique()}")
    print(f"    賭け点数: {n_bets}")
    print(f"    的中数: {n_hits}")
    print(f"    的中率: {n_hits / n_bets * 100:.1f}%" if n_bets > 0 else "    的中率: N/A")
    print(f"    投資額: {total_invest}円")
    print(f"    回収額: {total_return_val}円")
    print(f"    回収率: {total_return_val / total_invest * 100:.1f}%" if total_invest > 0 else "    回収率: N/A")
    print(f"    損益: {total_return_val - total_invest}円")

print("\n完了！")
