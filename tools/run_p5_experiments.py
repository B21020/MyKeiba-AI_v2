"""
P5 統合実験: 特徴量エンジニアリング改善 + 少数精鋭戦略
===========================================================
Phase A: 少数精鋭戦略 (P5-2)
  - 既存defaultモデルで高閾値sweep
  - top-N per race アプローチ
  - n_bets 100-500 帯でのRR検証

Phase B: 特徴量エンジニアリング (P5-1)
  Variant  説明
  -------  ----------------------------------------
  baseline 現状そのまま（比較用）
  no_hid   horse_id を除外
  rel      race-relative特徴量を追加
  no_hid+rel  horse_id除外 + race-relative追加
  no_highcard  horse_id/owner_id/breeder_id 全除外 + race-relative追加

Usage:
  python tools/run_p5_experiments.py [--n-sweep 100] [--min-bets 50] [--skip-phase-a] [--skip-phase-b]
"""

import sys, pathlib, argparse, time, pickle, json, warnings
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

warnings.filterwarnings("ignore")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from modules.training._data_splitter import DataSplitter
from modules.training._model_wrapper import ModelWrapper
from modules.training._calibration import fit_calibrator

OUT_DIR = pathlib.Path("models/20260208")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ================================================================
# Utility functions
# ================================================================

def load_featured_data():
    fd = pd.read_pickle("data/tmp/featured_data_20260205.pickle")
    print(f"featured_data: {fd.shape[0]:,} rows × {fd.shape[1]} cols, "
          f"date range {fd['date'].min()} ~ {fd['date'].max()}")
    return fd


def train_and_predict(ds: DataSplitter, params: dict | None = None):
    """Train LGBMClassifier and return (model, y_test, y_score_test, auc_test)."""
    mw = ModelWrapper()
    if params:
        mw.set_params(params)
    mw.train(ds)
    model = mw.lgb_model
    train_cols = list(ds.X_train.columns)
    # AUC on test
    y_test = ds.y_test.values
    X_test_aligned = ds.X_test.reindex(columns=train_cols, fill_value=0)
    y_score = model.predict_proba(X_test_aligned)[:, 1]
    auc_test = roc_auc_score(y_test, y_score)
    return mw, y_test, y_score, auc_test


def sweep_tansho_ev(y_test, y_score, tansho_odds, n_sweep=100, min_bets=50):
    """EV = score * odds >= 1+margin をスイープし、各margin の (rr, n, std) を返す。"""    # Filter out NaN odds
    valid = ~np.isnan(tansho_odds)
    y_test = y_test[valid]
    y_score = y_score[valid]
    tansho_odds = tansho_odds[valid]
    margins = np.linspace(0, 5.0, n_sweep)
    results = []
    for m in margins:
        ev = y_score * tansho_odds
        bet_mask = ev >= (1 + m)
        n_bets = bet_mask.sum()
        if n_bets < min_bets:
            continue
        wins = y_test[bet_mask]
        odds_won = tansho_odds[bet_mask]
        payout = (wins * odds_won).sum()
        rr = payout / n_bets
        std = np.sqrt(((wins * odds_won - 1) ** 2).sum() / n_bets) / np.sqrt(n_bets)
        results.append({"margin": m, "rr": rr, "n_bets": int(n_bets), "std": std})
    return pd.DataFrame(results)


def sweep_fukusho(y_test, y_score, n_sweep=100, min_bets=50):
    """score >= threshold をスイープし複勝の近似RRを返す。rank=1(3着以内)として払戻120%想定。"""
    thresholds = np.linspace(0, 5.0, n_sweep)
    results = []
    for th in thresholds:
        bet_mask = y_score >= th
        n_bets = bet_mask.sum()
        if n_bets < min_bets:
            continue
        hit_rate = y_test[bet_mask].mean()
        rr = hit_rate * 3.0  # 3着以内=1, 平均複勝払戻≈3倍で概算
        # actually let's compute more accurately: if rank=1 (3着以内), payout ≈ 1.2 per 100
        # 複勝は 3着以内的中なので hit_rate * avg_payout
        # simplified: hit_rate ≈ 30% base, payout≈3x → rr≈0.9 base
        # We keep this simple for comparison across variants
        results.append({"threshold": th, "rr": rr, "n_bets": int(n_bets),
                        "hit_rate": hit_rate})
    return pd.DataFrame(results)


def best_in_range(df, n_min, n_max, metric="rr"):
    """n_bets が [n_min, n_max] 範囲の中でbest rr を返す。"""
    mask = (df["n_bets"] >= n_min) & (df["n_bets"] <= n_max)
    sub = df[mask]
    if sub.empty:
        return None
    return sub.loc[sub[metric].idxmax()].to_dict()


# ================================================================
# Phase A: 少数精鋭戦略 (P5-2)
# ================================================================

def run_phase_a(fd, n_sweep=200, min_bets=10):
    """
    既存defaultモデルで少数精鋭戦略を検証。
    - 高margin sweep (margin 0~5 を200点で)
    - top-N per race アプローチ
    """
    print("\n" + "=" * 60)
    print("Phase A: 少数精鋭戦略 (P5-2)")
    print("=" * 60)

    ds = DataSplitter(fd, test_size=0.2, valid_size=0.2)
    mw, y_test, y_score, auc_test = train_and_predict(ds)

    test_data = ds.test_data
    tansho_odds = test_data["単勝"].values.astype(float)
    race_ids = test_data["race_id"].values

    # ── A1: Fine-grained EV sweep ──
    print("\n--- A1: Fine-grained tansho EV sweep ---")
    tansho_df = sweep_tansho_ev(y_test, y_score, tansho_odds, n_sweep=n_sweep, min_bets=min_bets)

    # Report for different n_bets ranges
    ranges = [(10, 100), (100, 300), (300, 500), (500, 1000), (1000, 3000), (3000, 10000)]
    print(f"\n{'Range':<14} {'RR':>8} {'n_bets':>8} {'margin':>8}")
    print("-" * 42)
    for lo, hi in ranges:
        b = best_in_range(tansho_df, lo, hi)
        if b:
            print(f"{lo}-{hi:<8} {b['rr']:8.4f} {b['n_bets']:8d} {b['margin']:8.4f}")
        else:
            print(f"{lo}-{hi:<8}     N/A")

    # ── A2: Top-N per race ──
    print("\n--- A2: Top-N per race (proxy RR: rank=top3) ---")
    test_df = pd.DataFrame({
        "race_id": race_ids,
        "y": y_test,
        "score": y_score,
        "odds": tansho_odds,
    })
    # Drop rows with NaN odds for tansho evaluation
    test_df_valid = test_df.dropna(subset=["odds"]).copy()
    print(f"  (odds available: {len(test_df_valid)} / {len(test_df)} rows)")

    for top_n in [1, 2, 3]:
        top_df = test_df_valid.groupby("race_id").apply(
            lambda g: g.nlargest(top_n, "score"), include_groups=False
        ).reset_index(drop=True)
        n_bets = len(top_df)
        payout = (top_df["y"] * top_df["odds"]).sum()
        rr = payout / n_bets if n_bets > 0 else 0
        hit = top_df["y"].mean()
        print(f"  top-{top_n}: RR={rr:.4f}, n_bets={n_bets}, hit_rate={hit:.4f}")

    # ── A3: Top-N per race + EV filter ──
    print("\n--- A3: Top-1 per race + EV margin filter ---")
    top1_df = test_df_valid.groupby("race_id").apply(
        lambda g: g.nlargest(1, "score"), include_groups=False
    ).reset_index(drop=True)
    for margin in [0, 0.1, 0.2, 0.5, 1.0, 1.5, 2.0, 3.0]:
        ev = top1_df["score"] * top1_df["odds"]
        mask = ev >= (1 + margin)
        n = mask.sum()
        if n < min_bets:
            continue
        sub = top1_df[mask]
        payout = (sub["y"] * sub["odds"]).sum()
        rr = payout / n
        hit = sub["y"].mean()
        print(f"  margin>={margin:.1f}: RR={rr:.4f}, n={n}, hit={hit:.4f}")

    # ── A4: Odds filter (高オッズ穴狙い vs 低オッズ本命) ──
    print("\n--- A4: Odds range filter (top-1 per race + EV>=1) ---")
    for odds_lo, odds_hi, label in [
        (1.0, 5.0, "本命(1-5倍)"),
        (5.0, 15.0, "中穴(5-15倍)"),
        (15.0, 50.0, "穴(15-50倍)"),
        (50.0, 999.0, "大穴(50+倍)"),
        (1.0, 15.0, "本命中穴(1-15倍)"),
        (5.0, 50.0, "中穴穴(5-50倍)"),
    ]:
        mask_odds = (top1_df["odds"] >= odds_lo) & (top1_df["odds"] < odds_hi)
        mask_ev = (top1_df["score"] * top1_df["odds"]) >= 1.0
        mask = mask_odds & mask_ev
        n = mask.sum()
        if n < min_bets:
            print(f"  {label}: n={n} < {min_bets}, skip")
            continue
        sub = top1_df[mask]
        payout = (sub["y"] * sub["odds"]).sum()
        rr = payout / n
        hit = sub["y"].mean()
        print(f"  {label}: RR={rr:.4f}, n={n}, hit={hit:.4f}")

    # ── A5: Score percentile approach ──
    print("\n--- A5: Score percentile filter (top-X% of all predictions) ---")
    # Use valid odds only
    valid_mask = ~np.isnan(tansho_odds)
    y_v = y_test[valid_mask]
    s_v = y_score[valid_mask]
    o_v = tansho_odds[valid_mask]
    for pct in [1, 2, 3, 5, 10, 20]:
        threshold = np.percentile(s_v, 100 - pct)
        mask = s_v >= threshold
        n = mask.sum()
        if n < min_bets:
            continue
        payout = (y_v[mask] * o_v[mask]).sum()
        rr = payout / n
        hit = y_v[mask].mean()
        print(f"  top-{pct}%: th={threshold:.4f}, RR={rr:.4f}, n={n}, hit={hit:.4f}")

    # Save results
    phase_a_results = {
        "auc_test": auc_test,
        "tansho_sweep": tansho_df.to_dict("records"),
        "model_params": "default",
    }
    with open(OUT_DIR / "p5_phase_a_elite_sweep.pickle", "wb") as f:
        pickle.dump(phase_a_results, f)
    print(f"\nPhase A saved: {OUT_DIR / 'p5_phase_a_elite_sweep.pickle'}")
    return phase_a_results


# ================================================================
# Phase B: 特徴量エンジニアリング (P5-1)
# ================================================================

def add_race_relative_features(df):
    """
    レース内相対特徴量を追加。
    各馬の過去成績をレース内の他馬と比較したz-scoreとrankを追加。
    """
    # 対象: 主要なパフォーマンス特徴量のみ（数を絞る）
    target_cols = [
        "着順_5R", "賞金_5R", "着差_5R", "time_seconds_5R",
        "着順_allR", "賞金_allR", "着差_allR", "time_seconds_allR",
        "斤量", "年齢", "体重",
        "jockey_plc_rate_10_all",
    ]
    # 実在する列のみ
    target_cols = [c for c in target_cols if c in df.columns]

    print(f"  Adding race-relative features for {len(target_cols)} columns...")
    eps = 1e-6

    for col in target_cols:
        grouped = df.groupby("race_id")[col]
        race_mean = grouped.transform("mean")
        race_std = grouped.transform("std").fillna(0).clip(lower=eps)
        # z-score
        df[f"{col}_zR"] = ((df[col] - race_mean) / race_std).astype("float32")
        # rank within race (lower is better for 着順, higher for 賞金 → just rank)
        df[f"{col}_rnkR"] = grouped.rank(method="min", ascending=True).astype("float32")

    new_cols = [c for c in df.columns if c.endswith("_zR") or c.endswith("_rnkR")]
    print(f"  Added {len(new_cols)} new columns")
    return df


def prepare_variant(fd_orig, variant_name):
    """
    Variant に応じた featured_data を返す。
    """
    fd = fd_orig.copy()

    if variant_name == "baseline":
        pass  # no changes

    elif variant_name == "no_hid":
        # horse_id を除外
        if "horse_id" in fd.columns:
            # DataSplitter は feature_cols から drop_cols 以外を全て使うので、
            # horse_id を drop_cols に追加する代わりに列自体を削除
            fd = fd.drop(columns=["horse_id"])

    elif variant_name == "rel":
        # race-relative 特徴量を追加（horse_idはそのまま）
        fd = add_race_relative_features(fd)

    elif variant_name == "no_hid+rel":
        # horse_id 除外 + race-relative
        if "horse_id" in fd.columns:
            fd = fd.drop(columns=["horse_id"])
        fd = add_race_relative_features(fd)

    elif variant_name == "no_highcard+rel":
        # 高cardinality ID全除外 + race-relative
        for c in ["horse_id", "owner_id", "breeder_id"]:
            if c in fd.columns:
                fd = fd.drop(columns=[c])
        fd = add_race_relative_features(fd)

    else:
        raise ValueError(f"Unknown variant: {variant_name}")

    return fd


def evaluate_variant(fd_variant, variant_name, n_sweep=100, min_bets=50):
    """1つのvariantを学習・評価し結果辞書を返す。"""
    print(f"\n--- Variant: {variant_name} (cols={fd_variant.shape[1]}) ---")
    t0 = time.time()

    ds = DataSplitter(fd_variant, test_size=0.2, valid_size=0.2)
    mw, y_test, y_score, auc_test = train_and_predict(ds)

    test_data = ds.test_data
    tansho_odds = test_data["単勝"].values.astype(float)

    # Tansho sweep
    tansho_df = sweep_tansho_ev(y_test, y_score, tansho_odds, n_sweep=n_sweep, min_bets=min_bets)

    # Best at various n_bets ranges
    bests = {}
    for lo, hi in [(50, 300), (300, 500), (500, 1000), (1000, 10000)]:
        b = best_in_range(tansho_df, lo, hi)
        bests[f"tansho_{lo}_{hi}"] = b

    # top-1 per race
    race_ids = test_data["race_id"].values
    test_df = pd.DataFrame({
        "race_id": race_ids, "y": y_test, "score": y_score, "odds": tansho_odds,
    })
    test_df_valid = test_df.dropna(subset=["odds"]).copy()
    top1 = test_df_valid.groupby("race_id").apply(
        lambda g: g.nlargest(1, "score"), include_groups=False
    ).reset_index(drop=True)
    top1_n = len(top1)
    top1_rr = (top1["y"] * top1["odds"]).sum() / top1_n if top1_n > 0 else 0
    top1_hit = top1["y"].mean()

    elapsed = time.time() - t0
    result = {
        "variant": variant_name,
        "n_features": fd_variant.shape[1] - 7,  # subtract dropped cols
        "auc_test": auc_test,
        "tansho_sweep": tansho_df.to_dict("records"),
        "bests": bests,
        "top1_rr": top1_rr,
        "top1_hit": top1_hit,
        "top1_n": top1_n,
        "elapsed_s": elapsed,
    }

    # Summary print
    b500 = bests.get("tansho_500_1000")
    b1k = bests.get("tansho_1000_10000")
    b50 = bests.get("tansho_50_300")
    print(f"  AUC(test)={auc_test:.4f}")
    print(f"  top-1/race: RR={top1_rr:.4f}, n={top1_n}, hit={top1_hit:.4f}")
    if b50:
        print(f"  tansho best (n=50-300):  RR={b50['rr']:.4f}, n={b50['n_bets']}")
    if b500:
        print(f"  tansho best (n=500-1k):  RR={b500['rr']:.4f}, n={b500['n_bets']}")
    if b1k:
        print(f"  tansho best (n=1k-10k):  RR={b1k['rr']:.4f}, n={b1k['n_bets']}")
    print(f"  elapsed: {elapsed:.1f}s")
    return result


def run_phase_b(fd, n_sweep=100, min_bets=50):
    """
    特徴量エンジニアリング実験。複数variant を順次評価。
    """
    print("\n" + "=" * 60)
    print("Phase B: 特徴量エンジニアリング (P5-1)")
    print("=" * 60)

    variants = ["baseline", "no_hid", "rel", "no_hid+rel", "no_highcard+rel"]
    all_results = {}

    for v in variants:
        fd_v = prepare_variant(fd, v)
        result = evaluate_variant(fd_v, v, n_sweep=n_sweep, min_bets=min_bets)
        all_results[v] = result
        # Free memory
        del fd_v

    # ── Comparison table ──
    print("\n" + "=" * 60)
    print("FEATURE VARIANT COMPARISON")
    print("=" * 60)
    header = f"{'Variant':<18} {'#feat':>6} {'AUC':>8} {'top1_RR':>8} {'RR_50-300':>10} {'RR_500-1k':>10} {'RR_1k+':>10}"
    print(header)
    print("-" * len(header))
    for v in variants:
        r = all_results[v]
        b50 = r["bests"].get("tansho_50_300")
        b500 = r["bests"].get("tansho_500_1000")
        b1k = r["bests"].get("tansho_1000_10000")
        rr50 = f"{b50['rr']:.4f}" if b50 else "N/A"
        rr500 = f"{b500['rr']:.4f}" if b500 else "N/A"
        rr1k = f"{b1k['rr']:.4f}" if b1k else "N/A"
        print(f"{v:<18} {r['n_features']:6d} {r['auc_test']:8.4f} {r['top1_rr']:8.4f}"
              f" {rr50:>10} {rr500:>10} {rr1k:>10}")

    # Save
    with open(OUT_DIR / "p5_phase_b_feature_variants.pickle", "wb") as f:
        pickle.dump(all_results, f)
    print(f"\nPhase B saved: {OUT_DIR / 'p5_phase_b_feature_variants.pickle'}")
    return all_results


# ================================================================
# Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="P5 experiments")
    parser.add_argument("--n-sweep", type=int, default=200)
    parser.add_argument("--min-bets", type=int, default=10)
    parser.add_argument("--skip-phase-a", action="store_true")
    parser.add_argument("--skip-phase-b", action="store_true")
    args = parser.parse_args()

    fd = load_featured_data()

    results = {}
    if not args.skip_phase_a:
        results["phase_a"] = run_phase_a(fd, n_sweep=args.n_sweep, min_bets=args.min_bets)

    if not args.skip_phase_b:
        results["phase_b"] = run_phase_b(fd, n_sweep=args.n_sweep, min_bets=args.min_bets)

    print("\n" + "=" * 60)
    print("ALL DONE")
    print("=" * 60)


if __name__ == "__main__":
    main()
