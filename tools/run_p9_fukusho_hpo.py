"""P9-1: 複勝 RR 直接最適化 HPO.

Optuna の objective を AUC ではなく **複勝 RR (return rate)** を直接最大化する。
的中率 ≈ 30% の複勝は単勝 (5.8%) より Bootstrap CI が大幅に狭くなる利点がある。

Phase 1: Optuna HPO (objective = fukusho RR on validation)
Phase 2: Fine-grain fukusho sweep on test (best params)
Phase 3: Bootstrap CI (race-level resampling)
Phase 4: Monthly + Venue breakdown
Phase 5: Comparison with default params

Usage:
  # Quick test (3 trials)
  .venv/Scripts/python.exe tools/run_p9_fukusho_hpo.py --n-trials 3 --n-sweep-obj 10

  # Full run
  .venv/Scripts/python.exe tools/run_p9_fukusho_hpo.py --n-trials 150 --n-boot 2000
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

# Force UTF-8 on Windows (environment-level, safe for backgrounding)
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass  # fallback: let PYTHONIOENCODING handle it

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

VENUE_NAMES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

FEATURED_PATH = "data/tmp/featured_data_20260205.pickle"
FEATURED_REL_EXT_PATH = "data/tmp/featured_data_20260205_rel_ext.pickle"


# ---------------------------------------------------------------------------
# Helpers
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


def _build_score_table_from_proba(
    proba: np.ndarray,
    data_df: pd.DataFrame,
) -> pd.DataFrame:
    """モデル出力 proba と元データから score_table を構築。"""
    st = pd.DataFrame(index=data_df.index)
    st["score"] = proba
    if "race_id" in data_df.columns:
        st["race_id"] = data_df["race_id"].values
    if ResultsCols.UMABAN in data_df.columns:
        st[ResultsCols.UMABAN] = data_df[ResultsCols.UMABAN].values
    elif "馬番" in data_df.columns:
        st[ResultsCols.UMABAN] = data_df["馬番"].values
    return st


def _fukusho_sweep(
    score_table: pd.DataFrame,
    sim: simulation.Simulator,
    thresholds: list[float],
    max_bets_per_race: int = 3,
    min_bets: int = 30,
) -> list[dict]:
    """複勝 threshold sweep → results list."""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyFukusho.judge(
            score_table, threshold=float(th), max_bets_per_race=max_bets_per_race,
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        results.append(ret)
    return results


def _fukusho_ev_sweep(
    score_table_raw: pd.DataFrame,
    tansho_odds: pd.Series,
    sim: simulation.Simulator,
    thresholds: list[float],
    max_bets_per_race: int = 3,
) -> list[dict]:
    """複勝 EV threshold sweep → results list."""
    results = []
    for th in thresholds:
        actions = policies.BetPolicyEVFukusho.judge(
            score_table_raw, threshold=float(th),
            tansho_odds=tansho_odds, max_bets_per_race=max_bets_per_race,
        )
        ret = sim.calc_returns(actions)
        ret["threshold"] = float(th)
        results.append(ret)
    return results


# ---------------------------------------------------------------------------
# Time-series CV splits helper
# ---------------------------------------------------------------------------

def _create_ts_cv_splits(
    train_df: pd.DataFrame,
    n_folds: int = 3,
) -> list[tuple]:
    """Create K-fold expanding-window time-series CV splits from training data.

    Divides data into n_folds+1 chronological segments.
    Fold i: train on segments 0..i, validate on segment i+1.

    Returns list of (X_train, y_train, X_valid, y_valid, valid_data_full) tuples.
    """
    drop_cols = ['rank', 'win', 'longshot', 'date',
                 ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']

    # Sort races chronologically
    date_per_race = train_df.groupby('race_id')['date'].first().sort_values()
    sorted_race_ids = date_per_race.index.tolist()
    n_races = len(sorted_race_ids)

    # Create n_folds+1 equal segments
    seg_size = n_races // (n_folds + 1)
    segments: list[list] = []
    for i in range(n_folds + 1):
        start = i * seg_size
        end = (i + 1) * seg_size if i < n_folds else n_races
        segments.append(sorted_race_ids[start:end])

    splits = []
    for fold in range(n_folds):
        # Expanding window: train on segments 0..fold, validate on segment fold+1
        train_ids = set()
        for s in range(fold + 1):
            train_ids |= set(segments[s])
        valid_ids = set(segments[fold + 1])

        train_fold = train_df[train_df['race_id'].isin(train_ids)]
        valid_fold = train_df[train_df['race_id'].isin(valid_ids)]

        X_train_fold = train_fold.drop(drop_cols, axis=1, errors='ignore')
        y_train_fold = train_fold['rank']
        X_valid_fold = valid_fold.drop(drop_cols, axis=1, errors='ignore')
        y_valid_fold = valid_fold['rank']

        splits.append((X_train_fold, y_train_fold, X_valid_fold, y_valid_fold, valid_fold))

        t_dates = pd.to_datetime(train_fold['date'])
        v_dates = pd.to_datetime(valid_fold['date'])
        print(f"    Fold {fold}: train={len(train_fold):,} rows ({len(train_ids)} races, "
              f"{t_dates.min().strftime('%Y-%m-%d')}~{t_dates.max().strftime('%Y-%m-%d')}), "
              f"valid={len(valid_fold):,} rows ({len(valid_ids)} races, "
              f"{v_dates.min().strftime('%Y-%m-%d')}~{v_dates.max().strftime('%Y-%m-%d')})")

    return splits


# ---------------------------------------------------------------------------
# Optuna Objective: 複勝 RR 直接最大化
# ---------------------------------------------------------------------------

class FukushoRRObjective:
    """Optuna objective: validation set での fukusho RR を最大化。

    各 trial で:
    1. LightGBM params を suggest
    2. train_optuna で学習
    3. valid で予測 → score_table → z-score
    4. threshold sweep → best RR を返す
    """

    def __init__(
        self,
        ds: DataSplitter,
        sim: simulation.Simulator,
        thresholds: list[float],
        max_bets_per_race: int = 3,
        min_bets: int = 30,
        objective_mode: str = "rr",  # "rr" or "profit"
        search_space: dict | None = None,
        cv_splits: list[tuple] | None = None,
    ):
        self.ds = ds
        self.cv_splits = cv_splits  # list of (X_tr, y_tr, X_val, y_val, val_data)
        self.sim = sim
        self.thresholds = thresholds
        self.max_bets_per_race = max_bets_per_race
        self.min_bets = min_bets
        self.objective_mode = objective_mode
        self.search_space = search_space or {}

        # Validation data (with race_id, 馬番 for score_table construction)
        self.valid_data = ds.valid_data_optuna
        self.X_valid = ds.X_valid
        self.y_valid = ds.y_valid

        # Training optuna data
        train_opt_df = ds.train_data_optuna
        drop_cols = ['rank', 'win', 'longshot', 'date',
                     ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']
        self.X_train_opt = train_opt_df.drop(drop_cols, axis=1, errors='ignore')
        self.y_train_opt = train_opt_df['rank']

        self.cat_cols = self.X_train_opt.select_dtypes(include="category").columns.tolist()

        # AUC tracking (for reference)
        self.best_auc = 0.0

    def _evaluate_cv(self, trial: optuna.Trial, params: dict) -> float:
        """Cross-validation RR objective: average RR across K time-series folds."""
        fold_results = []
        fold_aucs = []
        AUC_FLOOR = 0.72

        for fold_idx, (X_tr, y_tr, X_val, y_val, val_data) in enumerate(self.cv_splits):
            model = lgb.LGBMClassifier(**params)
            try:
                model.fit(
                    X_tr, y_tr,
                    eval_set=[(X_val, y_val)],
                    eval_metric="auc",
                    callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
                )
            except Exception:
                continue

            proba = model.predict_proba(X_val)[:, 1]
            try:
                auc = roc_auc_score(y_val, proba)
                fold_aucs.append(auc)
            except Exception:
                continue

            if auc < AUC_FLOOR:
                continue

            # Build score_table → z-score → sweep thresholds
            score_table = _build_score_table_from_proba(proba, val_data)
            score_table_std = _standardize_within_race(score_table)

            best_rr = 0.0
            best_th = None
            best_n = 0
            any_valid = False

            for th in self.thresholds:
                actions = policies.BetPolicyFukusho.judge(
                    score_table_std, threshold=float(th),
                    max_bets_per_race=self.max_bets_per_race,
                )
                ret = self.sim.calc_returns(actions)
                n_bets = ret.get("n_bets", 0)
                rr = ret.get("return_rate", 0)

                if n_bets < self.min_bets:
                    continue

                any_valid = True
                if rr > best_rr:
                    best_rr = rr
                    best_th = th
                    best_n = n_bets

            if any_valid:
                fold_results.append({
                    'fold': fold_idx, 'auc': auc,
                    'best_rr': best_rr, 'best_th': best_th, 'best_n': best_n,
                })

        if not fold_results:
            return -999.0

        mean_rr = float(np.mean([r['best_rr'] for r in fold_results]))
        mean_auc = float(np.mean(fold_aucs)) if fold_aucs else 0.0

        trial.set_user_attr("auc_valid", mean_auc)
        trial.set_user_attr("cv_fold_rrs", [r['best_rr'] for r in fold_results])
        trial.set_user_attr("cv_fold_aucs", fold_aucs)
        trial.set_user_attr("cv_n_valid_folds", len(fold_results))
        trial.set_user_attr("best_rr", mean_rr)
        trial.set_user_attr("best_threshold", fold_results[-1]['best_th'])
        trial.set_user_attr("best_n_bets", int(np.mean([r['best_n'] for r in fold_results])))

        self.best_auc = max(self.best_auc, mean_auc)

        return mean_rr

    def __call__(self, trial: optuna.Trial) -> float:
        ss = self.search_space  # shorthand
        params = {
            "objective": "binary",
            "metric": "auc",
            "verbosity": -1,
            "boosting_type": "gbdt",
            "num_leaves": trial.suggest_int("num_leaves", *ss.get("num_leaves", [15, 127])),
            "max_depth": trial.suggest_int("max_depth", *ss.get("max_depth", [3, 8])),
            "learning_rate": trial.suggest_float("learning_rate", *ss.get("learning_rate", [0.01, 0.2]), log=True),
            "n_estimators": trial.suggest_int("n_estimators", *ss.get("n_estimators", [200, 1000]), step=50),
            "min_child_samples": trial.suggest_int("min_child_samples", *ss.get("min_child_samples", [10, 200])),
            "subsample": trial.suggest_float("subsample", *ss.get("subsample", [0.5, 1.0])),
            "subsample_freq": trial.suggest_int("subsample_freq", *ss.get("subsample_freq", [0, 7])),
            "colsample_bytree": trial.suggest_float("colsample_bytree", *ss.get("colsample_bytree", [0.3, 1.0])),
            "reg_alpha": trial.suggest_float("reg_alpha", *ss.get("reg_alpha", [1e-8, 10.0]), log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", *ss.get("reg_lambda", [1e-8, 10.0]), log=True),
            "min_split_gain": trial.suggest_float("min_split_gain", *ss.get("min_split_gain", [0.0, 1.0])),
        }

        # Dispatch to CV evaluation if cv_splits provided
        if self.cv_splits is not None:
            return self._evaluate_cv(trial, params)

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

        proba = model.predict_proba(self.X_valid)[:, 1]

        # Track AUC for reference — reject if AUC too low (overfitting guard)
        try:
            auc = roc_auc_score(self.y_valid, proba)
            trial.set_user_attr("auc_valid", auc)
            self.best_auc = max(self.best_auc, auc)
        except Exception:
            auc = 0.0

        # AUC floor: reject trials with validation AUC < 0.72
        AUC_FLOOR = 0.72
        if auc < AUC_FLOOR:
            trial.set_user_attr("rejected", f"auc_valid={auc:.4f} < {AUC_FLOOR}")
            return 0.0

        # Build score_table → z-score → sweep
        score_table = _build_score_table_from_proba(proba, self.valid_data)
        score_table_std = _standardize_within_race(score_table)

        best_val = -float("inf")  # allow negative profit
        best_th = None
        best_n = 0
        any_valid = False
        for th in self.thresholds:
            actions = policies.BetPolicyFukusho.judge(
                score_table_std, threshold=float(th),
                max_bets_per_race=self.max_bets_per_race,
            )
            ret = self.sim.calc_returns(actions)
            n_bets = ret.get("n_bets", 0)
            rr = ret.get("return_rate", 0)

            if n_bets < self.min_bets:
                continue

            any_valid = True
            if self.objective_mode in ("rr", "hybrid"):
                val = rr
            elif self.objective_mode == "profit":
                val = (rr - 1.0) * n_bets  # total profit in units
            else:
                val = rr

            if val > best_val:
                best_val = val
                best_th = th
                best_n = n_bets

        trial.set_user_attr("best_threshold", best_th)
        trial.set_user_attr("best_n_bets", best_n)
        trial.set_user_attr("best_rr", best_val if self.objective_mode in ("rr", "hybrid") else 0)

        # If no threshold meets min_bets, return large penalty
        if not any_valid:
            return -999.0

        # Hybrid: 0.5 * normalized_auc + 0.5 * best_rr
        if self.objective_mode == "hybrid":
            # Normalize AUC to ~same scale as RR (AUC ~0.7-0.8 → shift to ~1.0)
            auc_component = auc / 0.75  # ~1.0 when AUC=0.75
            rr_component = best_val     # ~1.0 when RR=1.0
            hybrid_val = 0.5 * auc_component + 0.5 * rr_component
            trial.set_user_attr("hybrid_auc_comp", float(auc_component))
            trial.set_user_attr("hybrid_rr_comp", float(rr_component))
            return hybrid_val

        return best_val


def _convert_to_sklearn_params(params: dict) -> dict:
    out = {"objective": "binary"}
    for k in ["num_leaves", "max_depth", "learning_rate", "n_estimators",
              "min_child_samples", "subsample", "subsample_freq",
              "colsample_bytree", "reg_alpha", "reg_lambda", "min_split_gain"]:
        if k in params:
            out[k] = params[k]
    return out


# ---------------------------------------------------------------------------
# Post-HPO: Full test evaluation
# ---------------------------------------------------------------------------

def evaluate_on_test(
    featured_data: pd.DataFrame,
    test_start: str,
    test_end: str | None,
    valid_ratio: float,
    best_params: dict,
    sim: simulation.Simulator,
    out_dir: str,
    n_sweep: int = 200,
    max_bets_per_race: int = 3,
    min_bets: int = 50,
    label: str = "hpo",
) -> dict:
    """HPO best params で学習 → テスト set で fukusho sweep。"""

    data_min_date = pd.to_datetime(featured_data["date"]).min().strftime("%Y-%m-%d")
    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, data_min_date, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    ds = DataSplitter(combined, test_size=test_ratio, valid_size=valid_ratio)

    keiba_ai = training.KeibaAI(ds)
    if best_params:
        keiba_ai.set_params(_convert_to_sklearn_params(best_params))

    keiba_ai.train_without_tuning()

    # AUC
    X_test = ds.X_test
    y_test = ds.y_test
    raw_score_table = keiba_ai.calc_score(X_test, policies.BasicScorePolicy)
    raw_scores = pd.to_numeric(raw_score_table["score"], errors="coerce").astype(float)
    try:
        auc_train = roc_auc_score(ds.y_train, keiba_ai.calc_score(ds.X_train, policies.BasicScorePolicy)["score"].astype(float))
    except Exception:
        auc_train = float("nan")
    try:
        auc_test = roc_auc_score(y_test, raw_scores)
    except Exception:
        auc_test = float("nan")

    print(f"  AUC: {auc_train:.3f}(train), {auc_test:.3f}(test)")

    # Calibration: fit on valid, apply to test
    calibrations = {"none": raw_scores.copy()}
    for cal_name in ["platt", "isotonic"]:
        try:
            X_valid = ds.X_valid
            y_valid = ds.y_valid
            valid_score = keiba_ai.calc_score(X_valid, policies.BasicScorePolicy)
            valid_raw = pd.to_numeric(valid_score["score"], errors="coerce").astype(float)
            calibrator = fit_calibrator(cal_name, y_valid.values, valid_raw.values)
            calibrations[cal_name] = pd.Series(calibrator(raw_scores.values), index=raw_scores.index)
        except Exception as e:
            print(f"  calibration {cal_name} failed: {e}")

    # Tansho odds for EV strategy
    tansho_odds = ds.tansho_odds_test

    # Sweep thresholds
    thresholds_std = _linspace_inclusive(0.0, 4.0, n_sweep)
    thresholds_ev = _linspace_inclusive(0.5, 5.0, n_sweep)

    all_results = {}

    for cal_name, cal_scores in calibrations.items():
        cal_score_table = raw_score_table.copy()
        cal_score_table["score"] = cal_scores.values

        combo = f"{label}_{cal_name}"
        fk_df = pd.DataFrame()
        ev_df = pd.DataFrame()

        # --- Fukusho z-score threshold sweep ---
        try:
            score_std = _standardize_within_race(cal_score_table)
            fk_results = _fukusho_sweep(
                score_std, sim, thresholds_std,
                max_bets_per_race=max_bets_per_race, min_bets=min_bets,
            )
            fk_df = pd.DataFrame(fk_results)
            if len(fk_df) > 0:
                fk_df.index = fk_df["threshold"]
        except Exception as e:
            print(f"  [{combo}] fukusho_std sweep FAILED: {e}")

        # --- Fukusho EV sweep ---
        try:
            ev_results = _fukusho_ev_sweep(
                cal_score_table, tansho_odds, sim, thresholds_ev,
                max_bets_per_race=max_bets_per_race,
            )
            ev_df = pd.DataFrame(ev_results)
            if len(ev_df) > 0:
                ev_df.index = ev_df["threshold"]
        except Exception as e:
            print(f"  [{combo}] fukusho_ev sweep FAILED: {e}")

        all_results[combo] = {
            "fukusho_std": fk_df,
            "fukusho_ev": ev_df,
            "auc_test": auc_test,
            "auc_train": auc_train,
        }

        # Pick best
        def _pick_best(df, min_n):
            valid = df[df["n_bets"] >= min_n] if "n_bets" in df.columns else df
            if len(valid) == 0:
                return None, float("nan"), 0
            idx = valid["return_rate"].idxmax()
            row = valid.loc[idx]
            return idx, row["return_rate"], row.get("n_bets", 0)

        fk_th, fk_rr, fk_n = _pick_best(fk_df, min_bets)
        ev_th, ev_rr, ev_n = _pick_best(ev_df, min_bets)

        print(f"  [{combo}] fukusho_std: th={fk_th}  RR={fk_rr:.4f}  n={fk_n}")
        print(f"  [{combo}] fukusho_ev:  th={ev_th}  RR={ev_rr:.4f}  n={ev_n}")

        # Save
        fk_df.to_pickle(os.path.join(out_dir, f"{combo}_fukusho_std.pickle"))
        fk_df.to_csv(os.path.join(out_dir, f"{combo}_fukusho_std.csv"))
        ev_df.to_pickle(os.path.join(out_dir, f"{combo}_fukusho_ev.pickle"))
        ev_df.to_csv(os.path.join(out_dir, f"{combo}_fukusho_ev.csv"))

    return all_results, ds, keiba_ai


# ---------------------------------------------------------------------------
# Bootstrap CI (fukusho)
# ---------------------------------------------------------------------------

def bootstrap_fukusho_ci(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    max_bets_per_race: int,
    n_boot: int,
    seed: int = 42,
) -> dict:
    """レース単位 Bootstrap で fukusho RR の CI を推定。"""

    # まず per-race returns を取得
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return {
            "mean": 0, "ci_lower": 0, "ci_upper": 0, "p_value_lt_1": 1.0,
            "median": 0, "std": 0, "rr_distribution": np.array([]),
            "n_races": 0, "n_bets": 0, "n_hits": 0, "point_rr": 0.0,
        }

    race_ids = per_race.index.values
    n_races = len(race_ids)

    rng = np.random.RandomState(seed)
    rr_dist = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.choice(n_races, size=n_races, replace=True)
        sampled = per_race.iloc[idx]
        total_bet = sampled["bet_amount"].sum()
        total_ret = sampled["return_amount"].sum()
        rr_dist[b] = total_ret / total_bet if total_bet > 0 else 0.0

    ci_lower = float(np.percentile(rr_dist, 2.5))
    ci_upper = float(np.percentile(rr_dist, 97.5))
    p_lt_1 = float(np.mean(rr_dist < 1.0))

    return {
        "mean": float(np.mean(rr_dist)),
        "median": float(np.median(rr_dist)),
        "std": float(np.std(rr_dist)),
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "p_value_lt_1": p_lt_1,
        "rr_distribution": rr_dist,
        "n_races": n_races,
        "n_bets": int(per_race["n_bets"].sum()),
        "n_hits": int(per_race["hit_or_not"].sum()),
        "point_rr": float(per_race["return_amount"].sum() / per_race["bet_amount"].sum()),
    }


# ---------------------------------------------------------------------------
# Monthly + Venue breakdown
# ---------------------------------------------------------------------------

def monthly_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    max_bets_per_race: int,
    test_data: pd.DataFrame,
) -> pd.DataFrame:
    """月別の fukusho 成績。"""
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    # race_id → date mapping
    if "race_id" in test_data.columns:
        date_map = test_data.groupby("race_id")["date"].first()
    else:
        date_map = test_data.groupby(level=0)["date"].first()
    date_map = pd.to_datetime(date_map)

    per_race["month"] = date_map.reindex(per_race.index).dt.to_period("M").astype(str)
    per_race = per_race.dropna(subset=["month"])

    monthly = per_race.groupby("month").agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    monthly["return_rate"] = monthly["total_return"] / monthly["total_bet"].replace(0, np.nan)
    monthly["profit"] = monthly["total_return"] - monthly["total_bet"]
    return monthly.sort_index()


def venue_breakdown(
    score_table_std: pd.DataFrame,
    sim: simulation.Simulator,
    threshold: float,
    max_bets_per_race: int,
) -> pd.DataFrame:
    """場別の fukusho 成績。"""
    actions = policies.BetPolicyFukusho.judge(
        score_table_std, threshold=float(threshold),
        max_bets_per_race=max_bets_per_race,
    )
    per_race = sim.calc_returns_per_race(actions)
    if per_race.empty:
        return pd.DataFrame()

    race_ids = per_race.index.astype(str)
    per_race["venue_code"] = race_ids.str[4:6]
    per_race["venue_name"] = per_race["venue_code"].map(VENUE_NAMES).fillna("不明")

    venue = per_race.groupby(["venue_code", "venue_name"]).agg(
        n_races=("n_bets", "count"),
        n_bets=("n_bets", "sum"),
        n_hits=("hit_or_not", "sum"),
        total_bet=("bet_amount", "sum"),
        total_return=("return_amount", "sum"),
    )
    venue["return_rate"] = venue["total_return"] / venue["total_bet"].replace(0, np.nan)
    venue["profit"] = venue["total_return"] - venue["total_bet"]
    return venue.sort_index()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="P9-1: 複勝 RR 直接最適化 HPO")
    ap.add_argument("--featured", default=None,
                    help="featured_data path (default: try rel_ext, fallback baseline)")
    ap.add_argument("--test-start", default="2024-07-01")
    ap.add_argument("--test-end", default=None)
    ap.add_argument("--valid-ratio", type=float, default=0.2)
    ap.add_argument("--n-trials", type=int, default=150)
    ap.add_argument("--n-sweep-obj", type=int, default=30,
                    help="Objective内のthreshold sweep点数")
    ap.add_argument("--n-sweep-test", type=int, default=200,
                    help="Test evaluationのsweep点数")
    ap.add_argument("--min-bets-obj", type=int, default=30,
                    help="Objective内のmin_bets条件")
    ap.add_argument("--min-bets-test", type=int, default=50,
                    help="Test evaluationのmin_bets条件")
    ap.add_argument("--max-bets-per-race", type=int, default=3)
    ap.add_argument("--objective-mode", default="rr", choices=["rr", "profit", "hybrid"],
                    help="Optuna objective: rr=return rate, profit=(rr-1)*n, hybrid=0.5*AUC+0.5*RR")
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--resume", action="store_true",
                    help="Skip Phase 0-1, load saved study from out-dir and run Phase 2-4 only")
    ap.add_argument("--selected-features", default=None,
                    help="JSON file with selected feature column names. Non-selected cols are dropped before training.")
    ap.add_argument("--search-space", default=None,
                    help="JSON file with custom search space bounds. Keys are param names, values are [low, high].")
    ap.add_argument("--cv-folds", type=int, default=1,
                    help="Number of time-series CV folds in objective (1=single split, 3=recommended for CV)")
    args = ap.parse_args()

    t_total = time.time()

    # Determine featured data path
    if args.featured:
        featured_path = args.featured
    elif os.path.isfile(FEATURED_REL_EXT_PATH):
        featured_path = FEATURED_REL_EXT_PATH
        print(f"Using rel_ext features: {FEATURED_REL_EXT_PATH}")
    else:
        featured_path = FEATURED_PATH
        print(f"rel_ext not found, using baseline: {FEATURED_PATH}")

    yyyymmdd = datetime.date.today().strftime("%Y%m%d")
    out_dir = args.out_dir or os.path.join("models", yyyymmdd, "p9")
    os.makedirs(out_dir, exist_ok=True)

    print(f"\n{'#'*70}")
    print(f"# P9-1: 複勝 RR 直接最適化 HPO")
    print(f"# featured: {featured_path}")
    if args.selected_features:
        print(f"# selected_features: {args.selected_features}")
    if args.search_space:
        print(f"# search_space: {args.search_space}")
    print(f"# trials: {args.n_trials}  objective: {args.objective_mode}")
    print(f"# out_dir: {out_dir}")
    print(f"{'#'*70}")

    # ================================================================
    # Load data
    # ================================================================
    print(f"\nLoading featured_data: {featured_path}")
    featured_data = pd.read_pickle(featured_path)
    print(f"  shape (raw): {featured_data.shape}")

    # --- Selected features filtering ---
    if args.selected_features:
        with open(args.selected_features, "r", encoding="utf-8") as f:
            sel_cols = json.load(f)
        # Always keep system columns needed by DataSplitter / scoring
        keep_always = {'rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS,
                       ResultsCols.UMABAN, 'race_id', 'horse_id'}
        keep = list(keep_always | set(sel_cols))
        keep = [c for c in keep if c in featured_data.columns]
        featured_data = featured_data[keep]
        n_feat = len([c for c in sel_cols if c in featured_data.columns])
        print(f"  Selected features: {n_feat} cols (from {args.selected_features})")

    print(f"  shape: {featured_data.shape}")
    dates = pd.to_datetime(featured_data["date"])
    print(f"  date range: {dates.min().strftime('%Y-%m-%d')} ~ {dates.max().strftime('%Y-%m-%d')}")

    # Train/test split
    test_start = args.test_start
    test_end = args.test_end
    data_min_date = dates.min().strftime("%Y-%m-%d")

    test_df = _filter_by_date(featured_data, test_start, test_end)
    train_df = _filter_by_date(featured_data, data_min_date, test_start)
    train_df = train_df[pd.to_datetime(train_df["date"]) < pd.Timestamp(test_start)]
    print(f"  train rows: {len(train_df):,}  test rows: {len(test_df):,}")

    combined = pd.concat([train_df, test_df])
    test_ratio = len(test_df) / len(combined)

    # Simulator (reused across all phases)
    return_processor = preprocessing.ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    sim = simulation.Simulator(return_processor)

    # ================================================================
    # Phase 0 & Phase 1: HPO (or resume from saved study)
    # ================================================================

    if args.resume:
        # ------ Resume: load saved study ------
        study_path = os.path.join(out_dir, "optuna_study.pickle")
        params_path = os.path.join(out_dir, "optuna_best_params.json")
        if not os.path.isfile(study_path):
            print(f"  ERROR: {study_path} not found. Cannot resume.")
            return 1
        print(f"\n  Resuming from saved study: {study_path}")
        with open(study_path, "rb") as f:
            study = pickle.load(f)
        print(f"  Loaded study: {len(study.trials)} trials, best_value={study.best_value:.4f}")
        best_trial = study.best_trial
        print(f"  Best params:")
        for k, v in study.best_params.items():
            print(f"    {k}: {v}")
        tune_time = 0.0
        default_auc = float("nan")
        default_best_th = None
        default_best_rr = 0.0
        default_auc_valid = float("nan")
        default_rr_valid = 0.0

    else:
        # ------ Phase 0: Default baseline ------
        print(f"\n{'='*60}")
        print(f"Phase 0: Default params baseline")
        print(f"{'='*60}")

        ds_default = DataSplitter(combined.copy(), test_size=test_ratio, valid_size=args.valid_ratio)
        keiba_default = training.KeibaAI(ds_default)
        keiba_default.train_without_tuning()

        default_score = keiba_default.calc_score(ds_default.X_test, policies.BasicScorePolicy)
        default_scores_raw = pd.to_numeric(default_score["score"], errors="coerce").astype(float)
        try:
            default_auc = roc_auc_score(ds_default.y_test, default_scores_raw)
        except Exception:
            default_auc = float("nan")
        print(f"  Default AUC(test): {default_auc:.4f}")

        # Default fukusho sweep (z-score)
        default_std = _standardize_within_race(default_score)
        default_fk_results = _fukusho_sweep(
            default_std, sim, _linspace_inclusive(0.0, 4.0, args.n_sweep_test),
            max_bets_per_race=args.max_bets_per_race, min_bets=args.min_bets_test,
        )
        default_fk_df = pd.DataFrame(default_fk_results)
        valid_default = default_fk_df[default_fk_df["n_bets"] >= args.min_bets_test]
        if len(valid_default) > 0:
            best_idx = valid_default["return_rate"].idxmax()
            best_row = valid_default.loc[best_idx]
            print(f"  Default best: th={best_row['threshold']:.3f}  RR={best_row['return_rate']:.4f}  n={best_row['n_bets']:.0f}")
            default_best_th = best_row["threshold"]
            default_best_rr = best_row["return_rate"]
        else:
            default_best_th = None
            default_best_rr = 0.0

        # ------ Phase 1: Optuna HPO ------
        print(f"\n{'='*60}")
        print(f"Phase 1: Optuna HPO ({args.n_trials} trials, objective={args.objective_mode})")
        print(f"{'='*60}")

        ds_hpo = DataSplitter(combined.copy(), test_size=test_ratio, valid_size=args.valid_ratio)
        obj_thresholds = _linspace_inclusive(0.5, 3.5, args.n_sweep_obj)

        # Load custom search space if provided
        search_space = None
        if args.search_space:
            with open(args.search_space, "r", encoding="utf-8") as f:
                search_space = json.load(f)
            print(f"  Custom search space loaded: {list(search_space.keys())}")

        # Create CV splits if requested
        cv_splits = None
        if args.cv_folds > 1:
            print(f"  Creating {args.cv_folds}-fold time-series CV splits ...")
            cv_splits = _create_ts_cv_splits(ds_hpo.train_data, n_folds=args.cv_folds)
            print(f"  CV mode: {args.cv_folds} folds (objective = mean RR across folds)")

        objective = FukushoRRObjective(
            ds=ds_hpo,
            sim=sim,
            thresholds=obj_thresholds,
            max_bets_per_race=args.max_bets_per_race,
            min_bets=args.min_bets_obj,
            objective_mode=args.objective_mode,
            search_space=search_space,
            cv_splits=cv_splits,
        )

        # Default baseline on valid (for comparison)
        default_model = lgb.LGBMClassifier(objective="binary", verbosity=-1)
        default_model.fit(
            objective.X_train_opt, objective.y_train_opt,
            eval_set=[(objective.X_valid, objective.y_valid)],
            eval_metric="auc",
            callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
        )
        default_proba_valid = default_model.predict_proba(objective.X_valid)[:, 1]
        try:
            default_auc_valid = roc_auc_score(objective.y_valid, default_proba_valid)
        except Exception:
            default_auc_valid = float("nan")
        print(f"  Default AUC(valid): {default_auc_valid:.4f}")

        # Default fukusho RR on valid
        default_valid_st = _build_score_table_from_proba(default_proba_valid, objective.valid_data)
        default_valid_std = _standardize_within_race(default_valid_st)
        default_valid_fk = _fukusho_sweep(
            default_valid_std, sim, obj_thresholds,
            max_bets_per_race=args.max_bets_per_race, min_bets=args.min_bets_obj,
        )
        default_fk_valid_df = pd.DataFrame(default_valid_fk)
        valid_d = default_fk_valid_df[default_fk_valid_df["n_bets"] >= args.min_bets_obj]
        if len(valid_d) > 0:
            default_rr_valid = valid_d["return_rate"].max()
        else:
            default_rr_valid = 0.0
        print(f"  Default fukusho RR(valid): {default_rr_valid:.4f}")

        # Run Optuna
        sampler = optuna.samplers.TPESampler(seed=args.seed)
        study = optuna.create_study(direction="maximize", sampler=sampler, study_name="p9_fukusho_rr")
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        t0 = time.time()
        study.optimize(objective, n_trials=args.n_trials, show_progress_bar=True)
        tune_time = time.time() - t0

        print(f"\n  Optuna completed: {len(study.trials)} trials in {tune_time:.0f}s")
        if args.objective_mode == "rr":
            print(f"  Default RR(valid): {default_rr_valid:.4f}")
            print(f"  Best    RR(valid): {study.best_value:.4f}")
            print(f"  Improvement:       {study.best_value - default_rr_valid:+.4f}")
        else:
            print(f"  Best objective:    {study.best_value:.4f}")

        best_trial = study.best_trial
        print(f"  Best trial AUC(valid): {best_trial.user_attrs.get('auc_valid', 'N/A')}")
        print(f"  Best trial th:         {best_trial.user_attrs.get('best_threshold', 'N/A')}")
        print(f"  Best trial n_bets:     {best_trial.user_attrs.get('best_n_bets', 'N/A')}")
        print(f"  Best params:")
        for k, v in study.best_params.items():
            print(f"    {k}: {v}")

        # Save study + params
        with open(os.path.join(out_dir, "optuna_study.pickle"), "wb") as f:
            pickle.dump(study, f)
        with open(os.path.join(out_dir, "optuna_best_params.json"), "w") as f:
            json.dump(study.best_params, f, indent=2)

    # ================================================================
    # Phase 2: Full test evaluation with best params
    # ================================================================
    print(f"\n{'='*60}")
    print(f"Phase 2: Full test evaluation (HPO best params)")
    print(f"{'='*60}")

    all_results, ds_test, keiba_hpo = evaluate_on_test(
        featured_data=featured_data,
        test_start=test_start,
        test_end=test_end,
        valid_ratio=args.valid_ratio,
        best_params=study.best_params,
        sim=sim,
        out_dir=out_dir,
        n_sweep=args.n_sweep_test,
        max_bets_per_race=args.max_bets_per_race,
        min_bets=args.min_bets_test,
        label="hpo",
    )

    # Find overall best fukusho result
    best_combo = None
    best_rr_overall = 0.0
    best_th_overall = None
    best_n_overall = 0
    best_strategy = None

    for combo, combo_data in all_results.items():
        for strat_name in ["fukusho_std", "fukusho_ev"]:
            df = combo_data[strat_name]
            valid = df[df["n_bets"] >= args.min_bets_test] if "n_bets" in df.columns else df
            if len(valid) == 0:
                continue
            idx = valid["return_rate"].idxmax()
            row = valid.loc[idx]
            if row["return_rate"] > best_rr_overall:
                best_rr_overall = row["return_rate"]
                best_th_overall = idx
                best_n_overall = row.get("n_bets", 0)
                best_combo = combo
                best_strategy = strat_name

    print(f"\n  Overall best: [{best_combo}] {best_strategy}")
    print(f"    th={best_th_overall}  RR={best_rr_overall:.4f}  n={best_n_overall}")

    # ================================================================
    # Phase 3: Bootstrap CI on best threshold
    # ================================================================
    if best_th_overall is not None and best_strategy == "fukusho_std":
        print(f"\n{'='*60}")
        print(f"Phase 3: Bootstrap CI ({args.n_boot} resamples, th={best_th_overall})")
        print(f"{'='*60}")

        # Reconstruct the test score_table for the best combo
        cal_name = best_combo.split("_", 1)[1] if "_" in best_combo else "none"
        test_score = keiba_hpo.calc_score(ds_test.X_test, policies.BasicScorePolicy)
        test_raw = pd.to_numeric(test_score["score"], errors="coerce").astype(float)

        if cal_name != "none":
            try:
                X_valid = ds_test.X_valid
                y_valid = ds_test.y_valid
                valid_score = keiba_hpo.calc_score(X_valid, policies.BasicScorePolicy)
                valid_raw = pd.to_numeric(valid_score["score"], errors="coerce").astype(float)
                calibrator = fit_calibrator(cal_name, y_valid.values, valid_raw.values)
                test_cal = pd.Series(calibrator(test_raw.values), index=test_raw.index)
                test_score_cal = test_score.copy()
                test_score_cal["score"] = test_cal.values
            except Exception as e:
                print(f"  Calibration {cal_name} failed: {e}, using raw")
                test_score_cal = test_score
        else:
            test_score_cal = test_score

        test_score_std = _standardize_within_race(test_score_cal)

        # Bootstrap CI at best threshold
        t0 = time.time()
        boot_result = bootstrap_fukusho_ci(
            test_score_std, sim,
            threshold=best_th_overall,
            max_bets_per_race=args.max_bets_per_race,
            n_boot=args.n_boot,
            seed=args.seed,
        )
        boot_time = time.time() - t0

        print(f"  Point RR:   {boot_result['point_rr']:.4f}")
        print(f"  n_bets:     {boot_result['n_bets']}")
        print(f"  n_hits:     {boot_result['n_hits']}  (hit rate: {boot_result['n_hits']/max(boot_result['n_bets'],1)*100:.1f}%)")
        print(f"  95% CI:     [{boot_result['ci_lower']:.4f}, {boot_result['ci_upper']:.4f}]")
        print(f"  P(RR<1):    {boot_result['p_value_lt_1']:.4f}")
        print(f"  Boot mean:  {boot_result['mean']:.4f}")
        print(f"  Boot time:  {boot_time:.1f}s")

        # Also bootstrap at a few nearby thresholds
        print(f"\n  Multi-threshold bootstrap:")
        boot_multi = {}
        step = 0.1
        for delta in [-0.3, -0.2, -0.1, 0.0, 0.1, 0.2, 0.3]:
            th_try = round(best_th_overall + delta, 3)
            if th_try < 0:
                continue
            br = bootstrap_fukusho_ci(
                test_score_std, sim,
                threshold=th_try,
                max_bets_per_race=args.max_bets_per_race,
                n_boot=args.n_boot,
                seed=args.seed,
            )
            flag = "(*)" if br["point_rr"] >= 1.0 else ""
            p_flag = ""
            if br["ci_lower"] > 1.0:
                p_flag = "[OK] CI>1"
            elif br["p_value_lt_1"] < 0.05:
                p_flag = "[WARN] P<0.05"
            else:
                p_flag = "[NG]"
            print(f"    th={th_try:.3f}: RR={br['point_rr']:.4f} n={br['n_bets']} "
                  f"CI=[{br['ci_lower']:.4f},{br['ci_upper']:.4f}] "
                  f"P(RR<1)={br['p_value_lt_1']:.3f} hits={br['n_hits']} {flag} {p_flag}")
            boot_multi[th_try] = br

        # Save bootstrap results
        boot_save = {k: {kk: vv for kk, vv in v.items() if kk != "rr_distribution"}
                     for k, v in boot_multi.items()}
        boot_save[best_th_overall] = {k: v for k, v in boot_result.items() if k != "rr_distribution"}
        boot_df = pd.DataFrame(boot_save).T
        boot_df.index.name = "threshold"
        boot_df.to_csv(os.path.join(out_dir, "bootstrap_ci.csv"))
        boot_df.to_pickle(os.path.join(out_dir, "bootstrap_ci.pickle"))

        # Save distribution
        with open(os.path.join(out_dir, f"bootstrap_dist_th{best_th_overall:.3f}.pickle"), "wb") as f:
            pickle.dump(boot_result["rr_distribution"], f)

        # ================================================================
        # Phase 4: Monthly + Venue breakdown
        # ================================================================
        print(f"\n{'='*60}")
        print(f"Phase 4: Monthly + Venue breakdown (th={best_th_overall})")
        print(f"{'='*60}")

        test_data = ds_test.test_data
        monthly = monthly_breakdown(
            test_score_std, sim, best_th_overall,
            args.max_bets_per_race, test_data,
        )
        print(f"\n  Monthly breakdown:")
        print(monthly.to_string())
        profit_months = (monthly["profit"] > 0).sum()
        loss_months = (monthly["profit"] <= 0).sum()
        print(f"\n  Profit months: {profit_months}/{len(monthly)}  Loss months: {loss_months}/{len(monthly)}")

        venue = venue_breakdown(test_score_std, sim, best_th_overall, args.max_bets_per_race)
        print(f"\n  Venue breakdown:")
        print(venue.to_string())

        monthly.to_csv(os.path.join(out_dir, "monthly_breakdown.csv"))
        venue.to_csv(os.path.join(out_dir, "venue_breakdown.csv"))

    elif best_th_overall is not None and best_strategy == "fukusho_ev":
        print(f"\n  Note: Best strategy is EV-based. Bootstrap for EV strategy not implemented yet.")
        print(f"  Best: th={best_th_overall}  RR={best_rr_overall:.4f}  n={best_n_overall}")

    # ================================================================
    # Summary
    # ================================================================
    print(f"\n{'='*70}")
    print(f"P9-1 SUMMARY")
    print(f"{'='*70}")

    print(f"  Featured: {os.path.basename(featured_path)}")
    print(f"  Optuna: {len(study.trials)} trials in {tune_time:.0f}s")
    print(f"  Objective mode: {args.objective_mode}")
    print(f"\n  --- Validation ---")
    print(f"  Default AUC(valid):      {default_auc_valid:.4f}")
    print(f"  HPO best AUC(valid):     {best_trial.user_attrs.get('auc_valid', 'N/A')}")
    print(f"  Default best RR(valid):  {default_rr_valid:.4f}")
    print(f"  HPO best RR(valid):      {study.best_value:.4f}")

    print(f"\n  --- Test ---")
    print(f"  Default AUC(test):       {default_auc:.4f}")
    if best_combo:
        auc_hpo = all_results[best_combo]["auc_test"]
        print(f"  HPO AUC(test):           {auc_hpo:.4f}")
    print(f"  Default best RR(test):   {default_best_rr:.4f} (th={default_best_th})")
    print(f"  HPO best RR(test):       {best_rr_overall:.4f} (th={best_th_overall}, {best_strategy}, {best_combo})")

    if best_th_overall is not None and best_strategy == "fukusho_std" and 'boot_result' in dir():
        print(f"\n  --- Bootstrap CI (th={best_th_overall}) ---")
        print(f"  95% CI: [{boot_result['ci_lower']:.4f}, {boot_result['ci_upper']:.4f}]")
        print(f"  P(RR<1): {boot_result['p_value_lt_1']:.4f}")
        print(f"  Hit rate: {boot_result['n_hits']}/{boot_result['n_bets']} "
              f"({boot_result['n_hits']/max(boot_result['n_bets'],1)*100:.1f}%)")

        if boot_result["ci_lower"] > 1.0:
            print(f"\n  [OK] CONCLUSION: CI lower > 1.0 -> Significant -> Production candidate")
        elif boot_result["p_value_lt_1"] < 0.05:
            print(f"\n  [WARN] CONCLUSION: P(RR<1)={boot_result['p_value_lt_1']:.4f} < 0.05 -> Conditionally significant")
        else:
            print(f"\n  [NG] CONCLUSION: P(RR<1)={boot_result['p_value_lt_1']:.4f} >= 0.05 -> Not significant")

    total_time = time.time() - t_total
    print(f"\n  Total time: {total_time:.1f}s")
    print(f"  Output dir: {out_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
