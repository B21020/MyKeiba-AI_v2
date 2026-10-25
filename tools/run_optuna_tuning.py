"""Optuna によるLightGBMハイパーパラメータ最適化。

objective: AUC(valid) を最大化。
テストデータ(2024-07-01〜) は最終評価のみに使用し、最適化には使わない。

最適化完了後、ベストparams で再学習→単勝EV/複勝/Edge sweep を自動実行。

Usage:
  .venv/Scripts/python.exe tools/run_optuna_tuning.py
  .venv/Scripts/python.exe tools/run_optuna_tuning.py --n-trials 200 --window all
  .venv/Scripts/python.exe tools/run_optuna_tuning.py --n-trials 100 --window 7
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
from pathlib import Path

import numpy as np
import optuna
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import lightgbm as lgb
from sklearn.metrics import roc_auc_score

from modules import policies, preprocessing, simulation, training
from modules.constants import LocalPaths, ResultsCols
from modules.training._data_splitter import DataSplitter


# ---------------------------------------------------------------------------
# Helpers (shared with run_calibration_sweep.py)
# ---------------------------------------------------------------------------

def _linspace_inclusive(start: float, end: float, n: int) -> list[float]:
    if n <= 1:
        return [float(start)]
    return [float(start + (end - start) * i / (n - 1)) for i in range(n)]


def _standardize_within_race(score_table: pd.DataFrame) -> pd.DataFrame:
    st = score_table.copy()
    if "race_id" in st.columns:
        keys = st["race_id"]
    else:
        try:
            keys = st.index.get_level_values(0)
        except Exception:
            keys = None

    s = pd.to_numeric(st["score"], errors="coerce").astype(float)
    if keys is None:
        m, std = float(s.mean()), float(s.std(ddof=0))
        st["score"] = ((s - m) / std).replace([np.inf, -np.inf], 0).fillna(0) if std > 0 else s.fillna(0)
        return st

    g = s.groupby(keys)
    mean_ = g.transform("mean")
    std_ = g.transform("std").replace(0, np.nan)
    z = (s - mean_) / std_
    st["score"] = z.where(np.isfinite(z), s).fillna(0).astype(float)
    return st


def _filter_by_date(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    dates = pd.to_datetime(df["date"])
    mask = pd.Series(True, index=df.index)
    if start is not None:
        mask &= dates >= pd.Timestamp(start)
    if end is not None:
        mask &= dates <= pd.Timestamp(end)
    return df.loc[mask]


def _compute_train_start(test_start: str, window_years: int) -> str:
    ts = pd.Timestamp(test_start)
    return (ts - pd.DateOffset(years=window_years)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Optuna Objective
# ---------------------------------------------------------------------------

class LGBMObjective:
    """Optuna objective: AUC(valid) を最大化。"""

    def __init__(self, ds: DataSplitter):
        self.ds = ds
        # 事前にデータを取得してキャッシュ
        self.X_train_opt = ds._DataSplitter__X_train_optuna
        self.y_train_opt = ds._DataSplitter__y_train_optuna
        self.X_valid = ds.X_valid
        self.y_valid = ds.y_valid

        # lazy 構築
        if self.X_train_opt is None:
            fd = ds._DataSplitter__featured_data
            fc = ds._DataSplitter__feature_cols
            train_opt_ids = ds._DataSplitter__train_optuna_id_list
            if "race_id" in fd.columns:
                df = fd[fd["race_id"].isin(train_opt_ids)]
            else:
                df = fd.loc[train_opt_ids]
            self.X_train_opt = df[fc]
            self.y_train_opt = df["rank"]

        # カテゴリ列
        self.cat_cols = self.X_train_opt.select_dtypes(include="category").columns.tolist()

    def __call__(self, trial: optuna.Trial) -> float:
        params = {
            "objective": "binary",
            "metric": "auc",
            "verbosity": -1,
            "boosting_type": "gbdt",
            # 探索空間
            "num_leaves": trial.suggest_int("num_leaves", 15, 255),
            "max_depth": trial.suggest_int("max_depth", 3, 12),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            "n_estimators": trial.suggest_int("n_estimators", 100, 1000, step=50),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "subsample_freq": trial.suggest_int("subsample_freq", 0, 7),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            "min_split_gain": trial.suggest_float("min_split_gain", 0.0, 1.0),
        }

        model = lgb.LGBMClassifier(**params)

        try:
            model.fit(
                self.X_train_opt, self.y_train_opt,
                eval_set=[(self.X_valid, self.y_valid)],
                eval_metric="auc",
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
            )
        except Exception as e:
            print(f"  Trial {trial.number} failed: {e}")
            return 0.0

        preds = model.predict_proba(self.X_valid)[:, 1]
        auc = roc_auc_score(self.y_valid, preds)
        return auc


# ---------------------------------------------------------------------------
# Post-tuning evaluation
# ---------------------------------------------------------------------------

def evaluate_best_params(
    featured_data: pd.DataFrame,
    train_start: str | None,
    test_start: str,
    test_end: str | None,
    valid_ratio: float,
    best_params: dict,
    out_dir: str,
    min_bets: int = 500,
    n_sweep: int = 100,
) -> dict:
    """ベストパラメータで学習→テストAUC + 回収率sweep。"""

    print("\n" + "=" * 60)
    print("Evaluating best params on TEST set")
    print("=" * 60)

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, train_start, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    ds = DataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)
    keiba_ai = training.KeibaAI(ds)

    # ベストパラメータからsklearn互換キーに変換
    sklearn_params = _convert_to_sklearn_params(best_params)
    keiba_ai.set_params(sklearn_params)

    print("  training with best params...")
    keiba_ai.train_without_tuning()

    # AUC
    X_test = ds.X_test
    y_test = ds.y_test
    score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(score_table["score"], errors="coerce").astype(float)
    try:
        auc_test = roc_auc_score(y_test, raw_scores)
    except Exception:
        auc_test = float("nan")
    print(f"  AUC test = {auc_test:.4f}")

    # tansho odds
    tansho_odds = None
    try:
        td = ds.test_data
        if ResultsCols.TANSHO_ODDS in td.columns:
            tansho_odds = td[ResultsCols.TANSHO_ODDS]
    except Exception:
        pass
    if tansho_odds is None:
        tansho_odds = ds.tansho_odds_test

    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # --- 単勝EV sweep ---
    score_std = _standardize_within_race(score_table)
    margins = _linspace_inclusive(0.0, 0.5, n_sweep)
    tansho_results = {}
    for margin in margins:
        actions = keiba_ai.decide_action(
            score_std, policies.BetPolicyEVTansho,
            margin=float(margin), tansho_odds=tansho_odds, max_bets_per_race=2)
        tansho_results[float(margin)] = sim.calc_returns(actions)

    tansho_df = pd.DataFrame.from_dict(tansho_results, orient="index").sort_index()
    tansho_df.index.name = "margin"
    tansho_df.to_pickle(os.path.join(out_dir, "optuna_best_tansho_ev.pickle"))

    # --- 複勝 sweep ---
    thresholds = _linspace_inclusive(0.0, 3.5, n_sweep)
    fukusho_results = {}
    for th in thresholds:
        actions = keiba_ai.decide_action(
            score_std, policies.BetPolicyFukusho,
            threshold=float(th), max_bets_per_race=3)
        fukusho_results[float(th)] = sim.calc_returns(actions)

    fukusho_df = pd.DataFrame.from_dict(fukusho_results, orient="index").sort_index()
    fukusho_df.index.name = "threshold"
    fukusho_df.to_pickle(os.path.join(out_dir, "optuna_best_fukusho.pickle"))

    # best with n_bets filter
    def _pick_best(df, min_n):
        if "n_bets" in df.columns:
            r = df[df["n_bets"] >= min_n]
        else:
            r = df
        if len(r) == 0:
            return float("nan"), float("nan"), 0, float("nan")
        idx = r["return_rate"].idxmax()
        row = r.loc[idx]
        return idx, row["return_rate"], row.get("n_bets", 0), row.get("std", float("nan"))

    te_param, te_rr, te_n, te_std = _pick_best(tansho_df, min_bets)
    fk_param, fk_rr, fk_n, fk_std = _pick_best(fukusho_df, min_bets)

    print(f"  tansho best (n>={min_bets}): margin={te_param}  RR={te_rr:.4f}  n={te_n}")
    print(f"  fukusho best (n>={min_bets}): th={fk_param}  RR={fk_rr:.4f}  n={fk_n}")

    # feature importance top 20
    fi = keiba_ai.feature_importance(30)
    fi_path = os.path.join(out_dir, "optuna_best_feature_importance.csv")
    fi.to_csv(fi_path, index=False)
    print(f"  feature importance saved: {fi_path}")

    return {
        "auc_test": auc_test,
        "tansho_margin": te_param,
        "tansho_rr": te_rr,
        "tansho_n_bets": te_n,
        "tansho_std": te_std,
        "fukusho_th": fk_param,
        "fukusho_rr": fk_rr,
        "fukusho_n_bets": fk_n,
        "fukusho_std": fk_std,
    }


def _convert_to_sklearn_params(params: dict) -> dict:
    """Optuna best_params → LGBMClassifier.set_params 互換キーに変換。"""
    out = {"objective": "binary"}
    key_map = {
        "num_leaves": "num_leaves",
        "max_depth": "max_depth",
        "learning_rate": "learning_rate",
        "n_estimators": "n_estimators",
        "min_child_samples": "min_child_samples",
        "subsample": "subsample",
        "subsample_freq": "subsample_freq",
        "colsample_bytree": "colsample_bytree",
        "reg_alpha": "reg_alpha",
        "reg_lambda": "reg_lambda",
        "min_split_gain": "min_split_gain",
    }
    for k, sk_k in key_map.items():
        if k in params:
            out[sk_k] = params[k]
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Optunaハイパーパラメータ最適化")
    ap.add_argument("--featured", default="data/tmp/featured_data_20260205.pickle")
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--window", default="all",
                    help="窓長 (年数 or 'all')")
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-trials", type=int, default=100)
    ap.add_argument("--timeout", type=int, default=None,
                    help="最適化タイムアウト(秒)")
    ap.add_argument("--min-bets", type=int, default=500)
    ap.add_argument("--n-sweep", type=int, default=100)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    # Load data
    print("loading featured_data:", args.featured)
    featured_data = pd.read_pickle(args.featured)
    dates = pd.to_datetime(featured_data["date"])
    data_min_date = dates.min().strftime("%Y-%m-%d")
    data_max_date = dates.max().strftime("%Y-%m-%d")
    print(f"  shape: {featured_data.shape}, date: {data_min_date} ~ {data_max_date}")

    # Window
    if args.window.lower() == "all":
        train_start = data_min_date
    else:
        years = int(args.window)
        train_start = _compute_train_start(args.test_start, years)
        if pd.Timestamp(train_start) < pd.Timestamp(data_min_date):
            train_start = data_min_date

    # Output dir
    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd)
    os.makedirs(out_dir, exist_ok=True)

    # --- Build DataSplitter for tuning (train+test 結合) ---
    test_df = _filter_by_date(featured_data, args.test_start, args.test_end)
    train_df = _filter_by_date(featured_data, train_start, args.test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(args.test_start)]

    print(f"\n  window: {args.window} (train_start={train_start})")
    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)
    ds = DataSplitter(combined, test_size=test_ratio, valid_size=args.valid_ratio)

    # --- Optuna study ---
    print(f"\n{'='*60}")
    print(f"Starting Optuna optimization: {args.n_trials} trials")
    print(f"{'='*60}")

    sampler = optuna.samplers.TPESampler(seed=args.seed)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        study_name=f"lgbm_tuning_{args.window}",
    )

    objective = LGBMObjective(ds)

    # ログを抑制（INFO → WARNING のみ）
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    t0 = time.time()
    study.optimize(
        objective,
        n_trials=args.n_trials,
        timeout=args.timeout,
        show_progress_bar=True,
    )
    tune_time = time.time() - t0

    print(f"\nOptuna completed: {len(study.trials)} trials in {tune_time:.0f}s")
    print(f"Best AUC(valid): {study.best_value:.6f}")
    print(f"Best params:")
    for k, v in study.best_params.items():
        print(f"  {k}: {v}")

    # Save study
    study_path = os.path.join(out_dir, "optuna_study.pickle")
    import pickle
    with open(study_path, "wb") as f:
        pickle.dump(study, f)
    print(f"\nStudy saved: {study_path}")

    # Best params を JSON で保存
    import json
    params_path = os.path.join(out_dir, "optuna_best_params.json")
    with open(params_path, "w") as f:
        json.dump(study.best_params, f, indent=2)
    print(f"Best params saved: {params_path}")

    # --- デフォルトパラメータとの比較 ---
    print(f"\n{'='*60}")
    print("Comparison: default vs best params")
    print(f"{'='*60}")

    # default AUC(valid) を算出
    default_model = lgb.LGBMClassifier(objective="binary")
    default_model.fit(
        objective.X_train_opt, objective.y_train_opt,
        eval_set=[(objective.X_valid, objective.y_valid)],
        eval_metric="auc",
        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
    )
    default_preds = default_model.predict_proba(objective.X_valid)[:, 1]
    default_auc = roc_auc_score(objective.y_valid, default_preds)
    print(f"  Default AUC(valid): {default_auc:.6f}")
    print(f"  Best    AUC(valid): {study.best_value:.6f}")
    print(f"  Improvement:        {study.best_value - default_auc:+.6f}")

    # --- ベストパラメータでテスト評価 + sweep ---
    results = evaluate_best_params(
        featured_data=featured_data,
        train_start=train_start,
        test_start=args.test_start,
        test_end=args.test_end,
        valid_ratio=args.valid_ratio,
        best_params=study.best_params,
        out_dir=out_dir,
        min_bets=args.min_bets,
        n_sweep=args.n_sweep,
    )

    # --- Also evaluate default for comparison ---
    print(f"\n{'='*60}")
    print("Evaluating DEFAULT params on TEST set")
    print(f"{'='*60}")
    default_results = evaluate_best_params(
        featured_data=featured_data,
        train_start=train_start,
        test_start=args.test_start,
        test_end=args.test_end,
        valid_ratio=args.valid_ratio,
        best_params={},  # empty = defaults
        out_dir=out_dir,
        min_bets=args.min_bets,
        n_sweep=args.n_sweep,
    )

    # --- Final summary ---
    print(f"\n{'='*60}")
    print("FINAL COMPARISON")
    print(f"{'='*60}")
    print(f"{'Metric':<25} {'Default':>12} {'Optuna Best':>12} {'Delta':>10}")
    print("-" * 60)
    for key in ["auc_test", "tansho_rr", "tansho_n_bets", "tansho_std", "fukusho_rr", "fukusho_n_bets", "fukusho_std"]:
        d = default_results.get(key, float("nan"))
        o = results.get(key, float("nan"))
        try:
            delta = o - d
            print(f"  {key:<23} {d:>12.4f} {o:>12.4f} {delta:>+10.4f}")
        except Exception:
            print(f"  {key:<23} {d!s:>12} {o!s:>12}")

    # Save comparison summary
    summary = {
        "default": default_results,
        "optuna_best": results,
        "best_params": study.best_params,
        "best_auc_valid": study.best_value,
        "default_auc_valid": default_auc,
        "n_trials": len(study.trials),
        "tune_time_sec": tune_time,
        "window": args.window,
        "train_start": train_start,
        "test_start": args.test_start,
    }
    summary_path = os.path.join(out_dir, "optuna_comparison_summary.pickle")
    with open(summary_path, "wb") as f:
        pickle.dump(summary, f)
    print(f"\nSummary saved: {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
