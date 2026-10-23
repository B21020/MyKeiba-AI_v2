"""
Optuna HPO 結果の可視化
- Trial history (AUC推移)
- パラメータ重要度
- Default vs Optuna 比較バーチャート
- 特徴量重要度 Top20
"""
import pickle, json, pathlib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

plt.rcParams["font.family"] = "MS Gothic"
OUT = pathlib.Path("images")
OUT.mkdir(exist_ok=True)
MODEL_DIR = pathlib.Path("models/20260208")

# ── 1. Trial history ──────────────────────────────────────
study = pickle.load(open(MODEL_DIR / "optuna_study.pickle", "rb"))
trials = study.trials
vals = [t.value for t in trials if t.value is not None]
best_so_far = np.maximum.accumulate(vals)

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(range(len(vals)), vals, "o", ms=4, alpha=0.5, label="各Trial AUC(valid)")
ax.plot(range(len(vals)), best_so_far, "-", color="red", lw=2, label="Best so far")
ax.axhline(0.741165, color="gray", ls="--", lw=1, label="Default AUC(valid)=0.7412")
ax.set_xlabel("Trial")
ax.set_ylabel("AUC (validation)")
ax.set_title("Optuna Trial History (100 trials)")
ax.legend()
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig(OUT / "optuna_trial_history.png", dpi=150)
print(f"Saved: {OUT / 'optuna_trial_history.png'}")
plt.close()

# ── 2. HP importance (Optuna built-in) ────────────────────
try:
    from optuna.importance import get_param_importances
    importances = get_param_importances(study)
    params_sorted = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    names = [p[0] for p in params_sorted]
    values = [p[1] for p in params_sorted]

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(names[::-1], values[::-1], color="steelblue")
    ax.set_xlabel("Importance")
    ax.set_title("Hyperparameter Importance (fANOVA)")
    ax.grid(True, alpha=0.3, axis="x")
    fig.tight_layout()
    fig.savefig(OUT / "optuna_hp_importance.png", dpi=150)
    print(f"Saved: {OUT / 'optuna_hp_importance.png'}")
    plt.close()
except Exception as e:
    print(f"HP importance skip: {e}")

# ── 3. Default vs Optuna 比較 ─────────────────────────────
summary = pickle.load(open(MODEL_DIR / "optuna_comparison_summary.pickle", "rb"))
default = summary["default"]
optuna_best = summary["optuna_best"]

metrics = ["auc_test", "tansho_rr", "fukusho_rr"]
labels = ["AUC (test)", "単勝 回収率", "複勝 回収率"]
def_vals = [default[m] for m in metrics]
opt_vals = [optuna_best[m] for m in metrics]

x = np.arange(len(metrics))
w = 0.35

fig, ax = plt.subplots(figsize=(8, 5))
bars1 = ax.bar(x - w/2, def_vals, w, label="Default", color="#4C72B0", alpha=0.85)
bars2 = ax.bar(x + w/2, opt_vals, w, label="Optuna Best", color="#DD8452", alpha=0.85)
ax.axhline(1.0, color="red", ls="--", lw=1, alpha=0.5, label="回収率=1.0")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.set_ylabel("Value")
ax.set_title("Default vs Optuna Best — 主要指標比較")
ax.legend()

# 値のラベル
for bar in bars1:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
            f"{bar.get_height():.4f}", ha="center", va="bottom", fontsize=9)
for bar in bars2:
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
            f"{bar.get_height():.4f}", ha="center", va="bottom", fontsize=9)

ax.set_ylim(0, 1.15)
ax.grid(True, alpha=0.3, axis="y")
fig.tight_layout()
fig.savefig(OUT / "optuna_comparison_bar.png", dpi=150)
print(f"Saved: {OUT / 'optuna_comparison_bar.png'}")
plt.close()

# ── 4. 特徴量重要度 Top20 ────────────────────────────────
fi = pd.read_csv(MODEL_DIR / "optuna_best_feature_importance.csv")
fi_top = fi.head(20).sort_values("importance")

fig, ax = plt.subplots(figsize=(8, 7))
ax.barh(fi_top["features"], fi_top["importance"], color="forestgreen", alpha=0.8)
ax.set_xlabel("Feature Importance (split count)")
ax.set_title("Feature Importance Top 20 (Optuna Best Model)")
ax.grid(True, alpha=0.3, axis="x")
fig.tight_layout()
fig.savefig(OUT / "optuna_feature_importance_top20.png", dpi=150)
print(f"Saved: {OUT / 'optuna_feature_importance_top20.png'}")
plt.close()

# ── 5. Best params vs Default params テーブル出力 ──────────
best_params = json.load(open(MODEL_DIR / "optuna_best_params.json"))
default_params = {
    "num_leaves": 31, "max_depth": -1, "learning_rate": 0.1,
    "n_estimators": 100, "min_child_samples": 20, "subsample": 1.0,
    "subsample_freq": 0, "colsample_bytree": 1.0, "reg_alpha": 0.0,
    "reg_lambda": 0.0, "min_split_gain": 0.0,
}
print("\n===== Best Params vs Default =====")
print(f"{'Parameter':<22} {'Default':>12} {'Optuna Best':>16}")
print("-" * 52)
for k in best_params:
    d = default_params.get(k, "N/A")
    b = best_params[k]
    print(f"{k:<22} {str(d):>12} {str(round(b,6) if isinstance(b,float) else b):>16}")

print("\nDone.")
