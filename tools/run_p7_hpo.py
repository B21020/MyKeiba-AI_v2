"""P7: Optuna HPO → 校正sweep 一括実行。

P7-1: win model HPO  → edge sweep で RR>1.0 を目指す
P7-2: rel_ext_rank + isotonic HPO → fukusho sweep で RR>1.0 を目指す

Usage:
  # P7-1: win model HPO (baseline features)
  .venv/Scripts/python.exe tools/run_p7_hpo.py --target win --featured data/tmp/featured_data_20260205.pickle --n-trials 150

  # P7-2: rel_ext rank model HPO
  .venv/Scripts/python.exe tools/run_p7_hpo.py --target rank --featured data/tmp/featured_data_20260205_rel_ext.pickle --n-trials 150

  # Both sequentially
  .venv/Scripts/python.exe tools/run_p7_hpo.py --run-all --n-trials 150
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pickle
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
from modules.training._calibration import fit_calibrator
from modules.training._data_splitter import DataSplitter
from modules.training._win_data_splitter import WinDataSplitter


# ---------------------------------------------------------------------------
# Helpers (shared)
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


def _ensure_win_column(fd: pd.DataFrame) -> pd.DataFrame:
    """featured_data に ``win`` カラムがなければ raw results から復元する。"""
    if "win" in fd.columns:
        return fd

    raw_path = os.path.join(str(REPO_ROOT), "data", "raw", "results.pickle.bak")
    if not os.path.isfile(raw_path):
        raise FileNotFoundError(f"raw results not found: {raw_path}")

    print("  Reconstructing 'win' column from raw results...")
    results = pd.read_pickle(raw_path)
    rank_num = pd.to_numeric(results["着順"], errors="coerce")
    umaban_num = pd.to_numeric(results["馬番"], errors="coerce")
    win_df = pd.DataFrame({
        "race_id": results.index,
        "馬番": umaban_num,
        "win": (rank_num == 1).astype("int8"),
    }).dropna(subset=["馬番", "win"])
    win_df["馬番"] = win_df["馬番"].astype(int)
    win_df = win_df.drop_duplicates(subset=["race_id", "馬番"], keep="first")

    if "race_id" in fd.columns:
        win_df["race_id"] = win_df["race_id"].astype(fd["race_id"].dtype)
    if fd["馬番"].dtype != win_df["馬番"].dtype:
        win_df["馬番"] = win_df["馬番"].astype(fd["馬番"].dtype)

    n_before = len(fd)
    fd = fd.merge(win_df[["race_id", "馬番", "win"]], on=["race_id", "馬番"], how="left")
    assert len(fd) == n_before, f"Merge row explosion: {n_before} -> {len(fd)}"
    fd["win"] = fd["win"].fillna(0).astype("int8")
    print(f"  win column added: 1={int((fd['win']==1).sum()):,}  0={int((fd['win']==0).sum()):,}")
    return fd


def _judge_edge_tansho(
    score_table: pd.DataFrame,
    tansho_odds: pd.Series,
    edge_threshold: float,
    max_bets_per_race: int = 2,
) -> dict:
    """Edge = calibrated_p - (1/odds) >= edge_threshold の馬に単勝購入。"""
    table = score_table.copy()
    odds_col = ResultsCols.TANSHO_ODDS
    if odds_col not in table.columns:
        if tansho_odds is not None:
            table[odds_col] = tansho_odds.reindex(table.index)
        else:
            return {}
    table["score"] = pd.to_numeric(table["score"], errors="coerce")
    table[odds_col] = pd.to_numeric(table[odds_col], errors="coerce")
    table[ResultsCols.UMABAN] = pd.to_numeric(table[ResultsCols.UMABAN], errors="coerce")
    table = table.dropna(subset=["score", odds_col, ResultsCols.UMABAN])
    table = table[(table["score"] >= 0) & (table["score"] <= 1) & (table[odds_col] > 0)]
    table["edge"] = table["score"] - (1.0 / table[odds_col])
    table = table[table["edge"] >= edge_threshold]
    if table.empty:
        return {}
    k = int(max_bets_per_race)
    if "race_id" in table.columns:
        table = (
            table.sort_values(["race_id", "edge"], ascending=[True, False])
            .groupby("race_id", sort=False).head(k)
        )
    else:
        race_keys = table.index.get_level_values(0)
        table = (
            table.assign(_rk=race_keys)
            .sort_values(["_rk", "edge"], ascending=[True, False])
            .groupby("_rk", sort=False).head(k)
            .drop(columns=["_rk"])
        )
    if "race_id" in table.columns:
        bet_df = table.groupby("race_id")[ResultsCols.UMABAN].apply(list).to_frame()
    else:
        bet_df = table.groupby(level=0)[ResultsCols.UMABAN].apply(list).to_frame()

    def _to_int_list(xs):
        s = pd.to_numeric(pd.Series(xs), errors="coerce")
        return s.dropna().astype(int).drop_duplicates().tolist()

    bet_df[ResultsCols.UMABAN] = bet_df[ResultsCols.UMABAN].apply(_to_int_list)
    bet_df = bet_df[bet_df[ResultsCols.UMABAN].apply(len) > 0]
    return bet_df.rename(columns={ResultsCols.UMABAN: "tansho"}).T.to_dict()


# ---------------------------------------------------------------------------
# Optuna Objective (target agnostic)
# ---------------------------------------------------------------------------

class LGBMObjective:
    """Optuna objective: AUC(valid) を最大化。"""

    def __init__(self, ds):
        """ds は DataSplitter or WinDataSplitter。"""
        self.ds = ds

        # 公開 property を使って validation data を取得
        self.X_valid = ds.X_valid
        self.y_valid = ds.y_valid

        # train_optuna を取得 (両方の Splitter で動くように)
        # WinDataSplitter は __X_train_optuna を直接公開していないので
        # train_data_optuna property から特徴量を抽出
        if hasattr(ds, 'train_data_optuna'):
            train_opt_df = ds.train_data_optuna
            drop_cols = ['rank', 'win', 'longshot', 'date',
                         ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']
            self.X_train_opt = train_opt_df.drop(drop_cols, axis=1, errors='ignore')
            # target column
            if 'win' in train_opt_df.columns and isinstance(ds, WinDataSplitter):
                self.y_train_opt = train_opt_df['win']
            else:
                self.y_train_opt = train_opt_df['rank']
        else:
            # Fallback: DataSplitter private attributes
            self.X_train_opt = ds._DataSplitter__X_train_optuna
            self.y_train_opt = ds._DataSplitter__y_train_optuna

        self.cat_cols = self.X_train_opt.select_dtypes(include="category").columns.tolist()

    def __call__(self, trial: optuna.Trial) -> float:
        params = {
            "objective": "binary",
            "metric": "auc",
            "verbosity": -1,
            "boosting_type": "gbdt",
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
        return roc_auc_score(self.y_valid, preds)


def _convert_to_sklearn_params(params: dict) -> dict:
    out = {"objective": "binary"}
    for k in ["num_leaves", "max_depth", "learning_rate", "n_estimators",
              "min_child_samples", "subsample", "subsample_freq",
              "colsample_bytree", "reg_alpha", "reg_lambda", "min_split_gain"]:
        if k in params:
            out[k] = params[k]
    return out


# ---------------------------------------------------------------------------
# Post-HPO evaluation with full sweep + calibration
# ---------------------------------------------------------------------------

def evaluate_with_sweep(
    featured_data: pd.DataFrame,
    train_start: str | None,
    test_start: str,
    test_end: str | None,
    valid_ratio: float,
    best_params: dict,
    target: str,
    calibrations: list[str],
    out_dir: str,
    min_bets: int = 50,
    n_sweep: int = 100,
    label: str = "hpo",
) -> list[dict]:
    """HPO best params で学習 → 各 calibration で全sweep → 結果リスト返却。"""

    results = []

    for cal in calibrations:
        combo_label = f"{label}_{cal}"
        print(f"\n{'='*60}")
        print(f"Sweep: {combo_label}  (calibration={cal})")
        print(f"{'='*60}")

        t0 = time.time()

        test_df = _filter_by_date(featured_data, test_start, test_end)
        train_df = _filter_by_date(featured_data, train_start, test_start)
        train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

        print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")
        combined = pd.concat([train_df, test_df])
        test_ratio = len(test_df) / len(combined)

        if target == "win":
            ds = WinDataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)
        else:
            ds = DataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)

        keiba_ai = training.KeibaAI(ds)
        if best_params:
            sklearn_params = _convert_to_sklearn_params(best_params)
            keiba_ai.set_params(sklearn_params)

        print("  training...")
        keiba_ai.train_without_tuning()

        # Score
        X_test = ds.X_test
        y_test = ds.y_test
        raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
        raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)

        try:
            auc_test = roc_auc_score(y_test, raw_scores)
        except Exception:
            auc_test = float("nan")
        print(f"  AUC test={auc_test:.4f}")

        # Calibration
        calibrated_scores = raw_scores.copy()
        auc_calibrated = auc_test
        if cal != "none":
            print(f"  fitting calibrator: {cal}...")
            X_valid = ds.X_valid
            y_valid = ds.y_valid
            valid_score_table = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
            valid_raw = pd.to_numeric(valid_score_table["score"], errors="coerce").astype(float)
            calibrator = fit_calibrator(cal, y_valid.values, valid_raw.values)
            calibrated_scores = pd.Series(calibrator(raw_scores.values), index=raw_scores.index)
            try:
                auc_calibrated = roc_auc_score(y_test, calibrated_scores)
            except Exception:
                auc_calibrated = float("nan")
            print(f"  AUC calibrated={auc_calibrated:.4f}")

        cal_score_table = raw_score_table.copy()
        cal_score_table["score"] = calibrated_scores.values

        # Tansho odds
        tansho_odds = ds.tansho_odds_test

        return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
        sim = simulation.Simulator(return_processor)

        # --- Tansho EV sweep ---
        print("  tansho EV sweep...")
        score_std = _standardize_within_race(cal_score_table)
        margins = _linspace_inclusive(0.0, 0.5, n_sweep)
        tansho_ev_results = {}
        for margin in margins:
            actions = keiba_ai.decide_action(
                score_std, policies.BetPolicyEVTansho,
                margin=float(margin), tansho_odds=tansho_odds, max_bets_per_race=2)
            tansho_ev_results[float(margin)] = sim.calc_returns(actions)
        tansho_ev_df = pd.DataFrame.from_dict(tansho_ev_results, orient="index").sort_index()
        tansho_ev_df.index.name = "margin"

        # --- Fukusho threshold sweep ---
        print("  fukusho threshold sweep...")
        thresholds = _linspace_inclusive(0.0, 3.5, n_sweep)
        fukusho_results = {}
        for th in thresholds:
            actions = keiba_ai.decide_action(
                score_std, policies.BetPolicyFukusho,
                threshold=float(th), max_bets_per_race=3)
            fukusho_results[float(th)] = sim.calc_returns(actions)
        fukusho_df = pd.DataFrame.from_dict(fukusho_results, orient="index").sort_index()
        fukusho_df.index.name = "threshold"

        # --- Edge tansho sweep ---
        print("  edge tansho sweep...")
        edge_thresholds = _linspace_inclusive(-0.05, 0.15, n_sweep)
        edge_results = {}
        for edge_th in edge_thresholds:
            actions = _judge_edge_tansho(cal_score_table, tansho_odds, edge_threshold=float(edge_th))
            edge_results[float(edge_th)] = sim.calc_returns(actions)
        edge_df = pd.DataFrame.from_dict(edge_results, orient="index").sort_index()
        edge_df.index.name = "edge_threshold"

        # --- Fukusho EV sweep ---
        print("  fukusho EV sweep...")
        fukusho_ev_thresholds = _linspace_inclusive(0.5, 5.0, n_sweep)
        fukusho_ev_results = {}
        for th in fukusho_ev_thresholds:
            actions = keiba_ai.decide_action(
                cal_score_table, policies.BetPolicyEVFukusho,
                threshold=float(th), tansho_odds=tansho_odds, max_bets_per_race=3)
            fukusho_ev_results[float(th)] = sim.calc_returns(actions)
        fukusho_ev_df = pd.DataFrame.from_dict(fukusho_ev_results, orient="index").sort_index()
        fukusho_ev_df.index.name = "threshold"

        train_time = time.time() - t0

        # Save detail pickles
        detail_dir = os.path.join(out_dir, "sweep_detail")
        os.makedirs(detail_dir, exist_ok=True)
        tansho_ev_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_tansho_ev.pickle"))
        fukusho_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_fukusho.pickle"))
        edge_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_edge.pickle"))
        fukusho_ev_df.to_pickle(os.path.join(detail_dir, f"{combo_label}_fukusho_ev.pickle"))

        # Feature importance
        fi = keiba_ai.feature_importance(30)
        fi.to_csv(os.path.join(out_dir, f"{combo_label}_feature_importance.csv"), index=False)

        # Pick best
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

        te_p, te_rr, te_n, te_std = _pick_best(tansho_ev_df, min_bets)
        fk_p, fk_rr, fk_n, fk_std = _pick_best(fukusho_df, min_bets)
        ed_p, ed_rr, ed_n, ed_std = _pick_best(edge_df, min_bets)
        fe_p, fe_rr, fe_n, fe_std = _pick_best(fukusho_ev_df, min_bets)

        print(f"  tansho_ev best  (n>={min_bets}): margin={te_p}  RR={te_rr:.4f}  n={te_n}")
        print(f"  fukusho best    (n>={min_bets}): th={fk_p}  RR={fk_rr:.4f}  n={fk_n}")
        print(f"  fukusho_ev best (n>={min_bets}): th={fe_p}  RR={fe_rr:.4f}  n={fe_n}")
        print(f"  edge best       (n>={min_bets}): edge_th={ed_p}  RR={ed_rr:.4f}  n={ed_n}")
        print(f"  total time: {train_time:.1f}s")

        results.append({
            "combo": combo_label,
            "calibration": cal,
            "auc_test": auc_test,
            "auc_calibrated": auc_calibrated,
            "tansho_ev_rr": te_rr, "tansho_ev_n_bets": te_n, "tansho_ev_std": te_std,
            "fukusho_rr": fk_rr, "fukusho_n_bets": fk_n, "fukusho_std": fk_std,
            "fukusho_ev_rr": fe_rr, "fukusho_ev_n_bets": fe_n, "fukusho_ev_std": fe_std,
            "edge_rr": ed_rr, "edge_n_bets": ed_n, "edge_std": ed_std,
            "train_time_sec": train_time,
        })

    return results


# ---------------------------------------------------------------------------
# Single experiment runner
# ---------------------------------------------------------------------------

def run_hpo_experiment(
    label: str,
    featured_path: str,
    target: str,
    calibrations: list[str],
    n_trials: int,
    out_dir: str,
    test_start: str = "2024-07-01",
    test_end: str | None = None,
    valid_ratio: float = 0.2,
    min_bets: int = 50,
    n_sweep: int = 100,
    seed: int = 42,
) -> dict | None:
    """1つのHPO実験: Optunaチューニング → 全calibration sweep。"""

    exp_dir = os.path.join(out_dir, label)
    os.makedirs(exp_dir, exist_ok=True)

    banner = f"[{label}]  target={target}  featured={os.path.basename(featured_path)}"
    print(f"\n{'#'*70}")
    print(f"# {banner}")
    print(f"# out_dir={exp_dir}")
    print(f"{'#'*70}")

    # Load data
    print(f"loading featured_data: {featured_path}")
    featured_data = pd.read_pickle(featured_path)
    print(f"  shape: {featured_data.shape}")
    dates = pd.to_datetime(featured_data["date"])
    data_min_date = dates.min().strftime("%Y-%m-%d")
    print(f"  date range: {data_min_date} ~ {dates.max().strftime('%Y-%m-%d')}")

    if target == "win":
        featured_data = _ensure_win_column(featured_data)

    train_start = data_min_date  # all window

    # --- Phase 1: Optuna HPO ---
    print(f"\n{'='*60}")
    print(f"Phase 1: Optuna HPO ({n_trials} trials)")
    print(f"{'='*60}")

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, train_start, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]
    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    if target == "win":
        ds = WinDataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)
    else:
        ds = DataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)

    # Default AUC for comparison
    print("  computing default AUC...")
    objective = LGBMObjective(ds)
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

    # Run Optuna
    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        study_name=f"p7_{label}",
    )
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    t0 = time.time()
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    tune_time = time.time() - t0

    print(f"\n  Optuna completed: {len(study.trials)} trials in {tune_time:.0f}s")
    print(f"  Default AUC(valid): {default_auc:.6f}")
    print(f"  Best    AUC(valid): {study.best_value:.6f}")
    print(f"  Improvement:        {study.best_value - default_auc:+.6f}")
    print(f"  Best params:")
    for k, v in study.best_params.items():
        print(f"    {k}: {v}")

    # Save study + params
    with open(os.path.join(exp_dir, "optuna_study.pickle"), "wb") as f:
        pickle.dump(study, f)
    with open(os.path.join(exp_dir, "optuna_best_params.json"), "w") as f:
        json.dump(study.best_params, f, indent=2)

    # --- Phase 2: Full sweep with best params ---
    print(f"\n{'='*60}")
    print(f"Phase 2: Full sweep with HPO best params")
    print(f"{'='*60}")

    hpo_results = evaluate_with_sweep(
        featured_data=featured_data,
        train_start=train_start,
        test_start=test_start,
        test_end=test_end,
        valid_ratio=valid_ratio,
        best_params=study.best_params,
        target=target,
        calibrations=calibrations,
        out_dir=exp_dir,
        min_bets=min_bets,
        n_sweep=n_sweep,
        label="hpo",
    )

    # --- Phase 3: Default params sweep for comparison ---
    print(f"\n{'='*60}")
    print(f"Phase 3: Default params sweep (comparison)")
    print(f"{'='*60}")

    default_results = evaluate_with_sweep(
        featured_data=featured_data,
        train_start=train_start,
        test_start=test_start,
        test_end=test_end,
        valid_ratio=valid_ratio,
        best_params={},
        target=target,
        calibrations=calibrations,
        out_dir=exp_dir,
        min_bets=min_bets,
        n_sweep=n_sweep,
        label="default",
    )

    # --- Summary ---
    all_results = hpo_results + default_results
    summary_df = pd.DataFrame(all_results)
    summary_path = os.path.join(exp_dir, "p7_sweep_summary.pickle")
    summary_df.to_pickle(summary_path)

    print(f"\n{'='*70}")
    print(f"SUMMARY: {label}")
    print(f"{'='*70}")
    show_cols = ["combo", "auc_test", "auc_calibrated",
                 "tansho_ev_rr", "tansho_ev_n_bets",
                 "fukusho_rr", "fukusho_n_bets",
                 "edge_rr", "edge_n_bets",
                 "train_time_sec"]
    show_cols = [c for c in show_cols if c in summary_df.columns]
    print(summary_df[show_cols].to_string(index=False))

    # Best comparison
    print(f"\n--- HPO vs Default comparison (n_bets >= {min_bets}) ---")
    for policy in ["tansho_ev", "fukusho", "fukusho_ev", "edge"]:
        rr_col = f"{policy}_rr"
        n_col = f"{policy}_n_bets"
        if rr_col not in summary_df.columns:
            continue
        valid = summary_df[summary_df[n_col] >= min_bets]
        if len(valid) == 0:
            continue
        hpo_rows = valid[valid["combo"].str.startswith("hpo_")]
        def_rows = valid[valid["combo"].str.startswith("default_")]

        hpo_best = hpo_rows.loc[hpo_rows[rr_col].idxmax()] if len(hpo_rows) > 0 else None
        def_best = def_rows.loc[def_rows[rr_col].idxmax()] if len(def_rows) > 0 else None

        if hpo_best is not None and def_best is not None:
            delta = hpo_best[rr_col] - def_best[rr_col]
            print(f"  {policy:12s}: HPO={hpo_best[rr_col]:.4f}(n={hpo_best[n_col]:.0f})  "
                  f"Default={def_best[rr_col]:.4f}(n={def_best[n_col]:.0f})  "
                  f"Delta={delta:+.4f}")
        elif hpo_best is not None:
            print(f"  {policy:12s}: HPO={hpo_best[rr_col]:.4f}(n={hpo_best[n_col]:.0f})  Default=N/A")

    return {
        "label": label,
        "target": target,
        "best_params": study.best_params,
        "best_auc_valid": study.best_value,
        "default_auc_valid": default_auc,
        "n_trials": len(study.trials),
        "tune_time_sec": tune_time,
        "hpo_results": hpo_results,
        "default_results": default_results,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="P7: Optuna HPO → 校正sweep")
    ap.add_argument("--target", default="win", choices=["win", "rank"])
    ap.add_argument("--featured", default="data/tmp/featured_data_20260205.pickle")
    ap.add_argument("--calibrations", default="none,platt,isotonic")
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-trials", type=int, default=150)
    ap.add_argument("--min-bets", type=int, default=50)
    ap.add_argument("--n-sweep", type=int, default=100)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--run-all", action="store_true",
                    help="P7-1 (win) + P7-2 (rel_ext_rank) を順次実行")
    args = ap.parse_args()

    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd, "p7")
    calibrations = [c.strip() for c in args.calibrations.split(",")]

    if args.run_all:
        # --- P7-1: win model HPO (baseline features) ---
        print("\n" + "=" * 70)
        print("  P7-1: Win model HPO (baseline features)")
        print("=" * 70)
        r1 = run_hpo_experiment(
            label="p7_1_win",
            featured_path="data/tmp/featured_data_20260205.pickle",
            target="win",
            calibrations=calibrations,
            n_trials=args.n_trials,
            out_dir=out_dir,
            test_start=args.test_start,
            test_end=args.test_end,
            valid_ratio=args.valid_ratio,
            min_bets=args.min_bets,
            n_sweep=args.n_sweep,
            seed=args.seed,
        )

        # --- P7-2: rel_ext rank model HPO ---
        print("\n" + "=" * 70)
        print("  P7-2: rel_ext rank model HPO")
        print("=" * 70)
        r2 = run_hpo_experiment(
            label="p7_2_rel_ext_rank",
            featured_path="data/tmp/featured_data_20260205_rel_ext.pickle",
            target="rank",
            calibrations=calibrations,
            n_trials=args.n_trials,
            out_dir=out_dir,
            test_start=args.test_start,
            test_end=args.test_end,
            valid_ratio=args.valid_ratio,
            min_bets=args.min_bets,
            n_sweep=args.n_sweep,
            seed=args.seed,
        )

        # Combined summary
        print("\n" + "=" * 70)
        print("  P7 COMBINED RESULTS")
        print("=" * 70)
        for r in [r1, r2]:
            if r is None:
                continue
            print(f"\n  [{r['label']}] target={r['target']}")
            print(f"    AUC(valid): default={r['default_auc_valid']:.6f}  HPO={r['best_auc_valid']:.6f}  delta={r['best_auc_valid']-r['default_auc_valid']:+.6f}")
            print(f"    Tune time: {r['tune_time_sec']:.0f}s ({r['n_trials']} trials)")

        # Save combined
        combined = {"p7_1": r1, "p7_2": r2}
        with open(os.path.join(out_dir, "p7_combined.pickle"), "wb") as f:
            pickle.dump(combined, f)

    else:
        # Single experiment
        label = f"p7_{args.target}"
        run_hpo_experiment(
            label=label,
            featured_path=args.featured,
            target=args.target,
            calibrations=calibrations,
            n_trials=args.n_trials,
            out_dir=out_dir,
            test_start=args.test_start,
            test_end=args.test_end,
            valid_ratio=args.valid_ratio,
            min_bets=args.min_bets,
            n_sweep=args.n_sweep,
            seed=args.seed,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
