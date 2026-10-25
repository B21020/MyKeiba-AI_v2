"""確率と回収の評価指標（再開計画 5.3）。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

EPS = 1e-12


def _codes(race_ids) -> np.ndarray:
    return pd.factorize(pd.Series(race_ids).astype(str))[0]


def race_log_loss(p, race_ids, win) -> float:
    """レース単位の対数損失（勝ち馬に付けた確率の -log の平均）。1着同着は勝ち馬で平均する。"""
    p, y, codes = np.asarray(p, float), np.asarray(win, float), _codes(race_ids)
    n = np.bincount(codes, weights=y)
    w = y / np.maximum(n[codes], 1)
    return float(-(w * np.log(np.clip(p, EPS, 1))).sum() / (n > 0).sum())


def pseudo_r2(p, race_ids, win) -> float:
    """Bolton & Chapman / Benter の pseudo-R² = 1 - LL(model) / LL(頭数に応じた一様)。"""
    codes = _codes(race_ids)
    size = np.bincount(codes)
    uniform = 1.0 / size[codes]
    return 1.0 - race_log_loss(p, race_ids, win) / race_log_loss(uniform, race_ids, win)


def race_loglik(p, race_ids, win) -> pd.Series:
    """レースごとの勝ち馬の対数確率（同着は勝ち馬で平均）。index = race_id。"""
    r = pd.Series(race_ids).astype(str).to_numpy()
    y = np.asarray(win, float)
    df = pd.DataFrame({'r': r, 'll': y * np.log(np.clip(np.asarray(p, float), EPS, 1)), 'y': y})
    g = df.groupby('r', sort=False)
    return g['ll'].sum() / g['y'].sum().clip(lower=1)


def paired_delta_r2(p_a, p_b, race_ids, win, n: int = 2000, seed: int = 0) -> dict:
    """ΔR² = pseudo-R²(a) − pseudo-R²(b) と、レース単位の再標本化による95%区間（同じレースで対にする）。"""
    codes = pd.factorize(pd.Series(race_ids).astype(str))[0]
    uniform = 1.0 / np.bincount(codes)[codes]
    la, lb, lu = (race_loglik(p, race_ids, win) for p in (p_a, p_b, uniform))
    d, u = (la - lb).to_numpy(), lu.to_numpy()
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(n, len(d)))
    dist = d[idx].mean(axis=1) / -u[idx].mean(axis=1)
    return {'delta_r2': float(d.mean() / -u.mean()), 'ci95': [float(np.percentile(dist, 2.5)), float(np.percentile(dist, 97.5))],
            'races': int(len(d)), 'share_races_a_better': float((d > 0).mean())}


def win_auc(p, win) -> float:
    return float(roc_auc_score(np.asarray(win), np.asarray(p)))


def brier(p, win) -> float:
    return float(np.mean((np.asarray(p, float) - np.asarray(win, float)) ** 2))


def top1_accuracy(p, race_ids, win) -> float:
    """各レースで確率が最大の馬が勝った割合。"""
    df = pd.DataFrame({'r': pd.Series(race_ids).astype(str).to_numpy(), 'p': np.asarray(p), 'w': np.asarray(win)})
    top = df.loc[df.groupby('r')['p'].idxmax()]
    return float(top['w'].mean())


def bootstrap_by_day(daily: pd.DataFrame, n: int = 2000, seed: int = 0,
                     stake_col: str = 'stake', return_col: str = 'payout', compare: tuple[str, str] | None = None) -> dict:
    """開催日単位のブロック再標本化で回収率の区間を出す。

    daily: 1行 = 1開催日。stake_col・return_col の列を持つ。
    compare=('a', 'b') を渡すと、a_stake・a_payout と b_stake・b_payout の列から回収率の差（a - b）の区間も出す。
    """
    rng = np.random.default_rng(seed)
    m = len(daily)
    idx = rng.integers(0, m, size=(n, m))
    out = {}

    def rr(stake, ret):
        s = stake[idx].sum(axis=1)
        r = ret[idx].sum(axis=1)
        return np.where(s > 0, r / np.maximum(s, EPS), np.nan)

    st, rt = daily[stake_col].to_numpy(float), daily[return_col].to_numpy(float)
    dist = rr(st, rt)
    out['roi'] = float(rt.sum() / st.sum()) if st.sum() > 0 else float('nan')
    out['roi_ci95'] = [float(np.nanpercentile(dist, 2.5)), float(np.nanpercentile(dist, 97.5))]
    if compare is not None:
        a, b = compare
        da = rr(daily[f'{a}_stake'].to_numpy(float), daily[f'{a}_payout'].to_numpy(float))
        db = rr(daily[f'{b}_stake'].to_numpy(float), daily[f'{b}_payout'].to_numpy(float))
        diff = da - db
        out['diff_ci95'] = [float(np.nanpercentile(diff, 2.5)), float(np.nanpercentile(diff, 97.5))]
    return out
