"""P6: 4施策一括比較実験

1. rel features恒久組込み + 拡張 (rel_ext)
2. 単勝特化モデル (win model)
3. 複勝EV戦略 (fukusho_ev)
4. rel_ext + win model 組合せ

全てのバリアントに対して run_calibration_sweep を呼ぶ。

Usage:
  .venv/Scripts/python.exe tools/run_p6_experiments.py
  .venv/Scripts/python.exe tools/run_p6_experiments.py --min-bets 50
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


def build_rel_ext_data(base_path: str, out_path: str) -> str:
    """featured_data に拡張 race-relative features を追加して保存する。

    win カラムが欠損している場合は raw results から復元する。
    """
    if os.path.isfile(out_path):
        fd = pd.read_pickle(out_path)
        # 既に拡張済みかチェック
        if any(c.endswith('_zR') for c in fd.columns):
            print(f"  rel_ext data already exists: {out_path} ({fd.shape[1]} cols)")
            return out_path

    print(f"  Building rel_ext data from {base_path}...")
    t0 = time.time()
    fd = pd.read_pickle(base_path)

    target_cols = [
        # 過去成績 (5R/9R/allR)
        '着順_5R', '賞金_5R', '着差_5R', 'time_seconds_5R',
        '着順_9R', '賞金_9R', '着差_9R', 'time_seconds_9R',
        '着順_allR', '賞金_allR', '着差_allR', 'time_seconds_allR',
        # コーナー通過
        'first_corner_5R', 'final_corner_5R',
        'first_corner_allR', 'final_corner_allR',
        # 位置取り変動
        'first_to_rank_5R', 'first_to_final_5R', 'final_to_rank_5R',
        'first_to_rank_allR', 'first_to_final_allR', 'final_to_rank_allR',
        # 馬属性
        '斤量', '年齢', '体重', '体重変化',
        # 騎手統計
        'jockey_plc_rate_10_all', 'jockey_plc_rate_50_all',
        # 時系列
        'interval', 'age_days',
    ]
    target_cols = [c for c in target_cols if c in fd.columns]

    eps = 1e-6
    for col in target_cols:
        grouped = fd.groupby('race_id')[col]
        race_mean = grouped.transform('mean')
        race_std = grouped.transform('std').fillna(0).clip(lower=eps)
        fd[f'{col}_zR'] = ((fd[col] - race_mean) / race_std).astype('float32')
        fd[f'{col}_rnkR'] = grouped.rank(method='min', ascending=True).astype('float32')

    new_cols = [c for c in fd.columns if c.endswith('_zR') or c.endswith('_rnkR')]
    print(f"  Added {len(new_cols)} rel features -> {fd.shape[1]} cols ({time.time()-t0:.1f}s)")

    # win カラム復元
    if 'win' not in fd.columns:
        raw_path = os.path.join(str(REPO_ROOT), 'data', 'raw', 'results.pickle.bak')
        if os.path.isfile(raw_path):
            print("  Reconstructing 'win' column from raw results...")
            results = pd.read_pickle(raw_path)
            rank_num = pd.to_numeric(results['着順'], errors='coerce')
            umaban_num = pd.to_numeric(results['馬番'], errors='coerce')
            win_df = pd.DataFrame({
                'race_id': results.index,
                '馬番': umaban_num,
                'win': (rank_num == 1).astype('int8'),
            }).dropna(subset=['馬番', 'win'])
            win_df['馬番'] = win_df['馬番'].astype(int)
            if 'race_id' in fd.columns:
                win_df['race_id'] = win_df['race_id'].astype(fd['race_id'].dtype)
            if fd['馬番'].dtype != win_df['馬番'].dtype:
                win_df['馬番'] = win_df['馬番'].astype(fd['馬番'].dtype)
            win_df = win_df.drop_duplicates(subset=['race_id', '馬番'], keep='first')
            n_before = len(fd)
            fd = fd.merge(win_df[['race_id', '馬番', 'win']], on=['race_id', '馬番'], how='left')
            assert len(fd) == n_before, f"Merge row explosion: {n_before} -> {len(fd)}"
            fd['win'] = fd['win'].fillna(0).astype('int8')
            print(f"  win=1: {int((fd['win']==1).sum()):,}  win=0: {int((fd['win']==0).sum()):,}")

    fd.to_pickle(out_path)
    print(f"  Saved: {out_path}")
    return out_path


def run_sweep(
    label: str,
    featured_path: str,
    out_dir: str,
    target: str = "rank",
    windows: str = "all",
    calibrations: str = "none,platt,isotonic",
    min_bets: int = 50,
    n_sweep: int = 100,
) -> dict | None:
    """run_calibration_sweep.py を subprocess として実行する。"""
    import subprocess

    python = str(Path(REPO_ROOT) / ".venv" / "Scripts" / "python.exe")
    script = str(Path(REPO_ROOT) / "tools" / "run_calibration_sweep.py")

    cmd = [
        python, script,
        "--featured", featured_path,
        "--windows", windows,
        "--calibrations", calibrations,
        "--min-bets", str(min_bets),
        "--n-sweep", str(n_sweep),
        "--out-dir", out_dir,
        "--target", target,
    ]

    print(f"\n{'#'*70}")
    print(f"# [{label}]  target={target}  featured={os.path.basename(featured_path)}")
    print(f"# out_dir={out_dir}")
    print(f"{'#'*70}")

    t0 = time.time()
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"  ERROR: exit code {result.returncode}")
        return None

    # Load summary
    summary_path = os.path.join(out_dir, "calibration_sweep_summary.pickle")
    if os.path.isfile(summary_path):
        summary = pd.read_pickle(summary_path)
        print(f"  Completed in {elapsed:.0f}s, {len(summary)} combos")
        return {"label": label, "summary_path": summary_path, "elapsed": elapsed}
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="P6: 4 strategies comparison")
    ap.add_argument("--featured", default="data/tmp/featured_data_20260205.pickle")
    ap.add_argument("--windows", default="all")
    ap.add_argument("--calibrations", default="none,platt,isotonic")
    ap.add_argument("--min-bets", type=int, default=50)
    ap.add_argument("--n-sweep", type=int, default=100)
    ap.add_argument("--skip-baseline", action="store_true")
    args = ap.parse_args()

    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    base_out = os.path.join("models", yyyymmdd)

    # ---- 0) Build rel_ext featured_data ----
    rel_ext_path = args.featured.replace(".pickle", "_rel_ext.pickle")
    rel_ext_path = build_rel_ext_data(args.featured, rel_ext_path)

    # ---- Define experiments ----
    experiments = []

    if not args.skip_baseline:
        # A) Baseline (rank model, original features)
        experiments.append({
            "label": "baseline_rank",
            "featured": args.featured,
            "target": "rank",
            "out_dir": os.path.join(base_out, "p6_baseline_rank"),
        })

    # B) rel_ext + rank model
    experiments.append({
        "label": "rel_ext_rank",
        "featured": rel_ext_path,
        "target": "rank",
        "out_dir": os.path.join(base_out, "p6_rel_ext_rank"),
    })

    # C) Baseline + win model
    experiments.append({
        "label": "baseline_win",
        "featured": args.featured,
        "target": "win",
        "out_dir": os.path.join(base_out, "p6_baseline_win"),
    })

    # D) rel_ext + win model
    experiments.append({
        "label": "rel_ext_win",
        "featured": rel_ext_path,
        "target": "win",
        "out_dir": os.path.join(base_out, "p6_rel_ext_win"),
    })

    # ---- Run all experiments ----
    results = []
    total = len(experiments)
    for i, exp in enumerate(experiments, 1):
        print(f"\n{'='*70}")
        print(f"  Experiment {i}/{total}: {exp['label']}")
        print(f"{'='*70}")

        r = run_sweep(
            label=exp["label"],
            featured_path=exp["featured"],
            out_dir=exp["out_dir"],
            target=exp["target"],
            windows=args.windows,
            calibrations=args.calibrations,
            min_bets=args.min_bets,
            n_sweep=args.n_sweep,
        )
        if r is not None:
            results.append(r)

    # ---- Comparison summary ----
    if not results:
        print("\nNo successful experiments.")
        return 1

    print(f"\n\n{'='*80}")
    print("  P6 COMPARISON SUMMARY")
    print(f"{'='*80}")

    all_summaries = []
    for r in results:
        df = pd.read_pickle(r["summary_path"])
        df["experiment"] = r["label"]
        all_summaries.append(df)

    combined = pd.concat(all_summaries, ignore_index=True)

    # Show key metrics per experiment
    for label in combined["experiment"].unique():
        sub = combined[combined["experiment"] == label]
        print(f"\n--- {label} ---")
        for _, row in sub.iterrows():
            parts = [f"  {row.get('combo', '?')}"]
            parts.append(f"AUC={row.get('auc_test', 0):.4f}")
            for policy in ["tansho_ev", "fukusho", "fukusho_ev", "edge"]:
                rr = row.get(f"{policy}_rr", float("nan"))
                n = row.get(f"{policy}_n_bets", 0)
                if pd.notna(rr) and n > 0:
                    parts.append(f"{policy}:RR={rr:.4f}(n={n:.0f})")
            print("  ".join(parts))

    # Overall best per policy
    print(f"\n{'='*80}")
    print("  OVERALL BEST (across all experiments)")
    print(f"{'='*80}")

    for policy in ["tansho_ev", "fukusho", "fukusho_ev", "edge"]:
        rr_col = f"{policy}_rr"
        n_col = f"{policy}_n_bets"
        if rr_col not in combined.columns:
            continue
        valid = combined[combined[n_col] >= args.min_bets]
        if len(valid) == 0:
            print(f"  {policy}: no result with n >= {args.min_bets}")
            continue
        best_idx = valid[rr_col].idxmax()
        best = valid.loc[best_idx]
        print(f"  {policy}: [{best['experiment']}] {best.get('combo', '?')}  "
              f"RR={best[rr_col]:.4f}  n={best[n_col]:.0f}  "
              f"std={best.get(f'{policy}_std', float('nan')):.4f}")

    # Save combined
    combined_path = os.path.join(base_out, "p6_combined_summary.pickle")
    combined.to_pickle(combined_path)
    print(f"\nCombined summary saved: {combined_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
