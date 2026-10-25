"""レース内の条件付きロジット（Benter 1994 の第2段、市場の補正）。

勝率 p_i = exp(Σ_k β_k x_ik) / Σ_j exp(Σ_k β_k x_jk)  （j は同じレースの出走馬）
説明変数は log(市場確率)・log(能力モデル確率) など少数。係数は最尤推定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import minimize


def race_softmax(score: np.ndarray, race_codes: np.ndarray) -> np.ndarray:
    """レースごとの softmax。race_codes は 0..R-1 の整数。"""
    score = np.asarray(score, dtype=float)
    m = np.full(race_codes.max() + 1, -np.inf)
    np.maximum.at(m, race_codes, score)
    e = np.exp(score - m[race_codes])
    s = np.bincount(race_codes, weights=e)
    return e / s[race_codes]


class ConditionalLogit:
    def __init__(self, l2: float = 0.0):
        self.l2 = l2
        self.coef_: np.ndarray | None = None
        self.columns_: list[str] | None = None

    @staticmethod
    def _codes(race_ids) -> np.ndarray:
        return pd.factorize(pd.Series(race_ids).astype(str))[0]

    def fit(self, X: pd.DataFrame, race_ids, win) -> 'ConditionalLogit':
        Xv = X.to_numpy(dtype=float)
        codes = self._codes(race_ids)
        y = np.asarray(win, dtype=float)
        n_races = codes.max() + 1

        def nll(beta):
            s = Xv @ beta
            m = np.full(n_races, -np.inf)
            np.maximum.at(m, codes, s)
            e = np.exp(s - m[codes])
            denom = np.bincount(codes, weights=e, minlength=n_races)
            logp = s - m[codes] - np.log(denom[codes])
            p = e / denom[codes]
            # 1着同着は両馬を勝ちとして数える（レースの勝者数で重みを割る）
            w = y / np.maximum(np.bincount(codes, weights=y, minlength=n_races)[codes], 1)
            loss = -(w * logp).sum() + self.l2 * (beta @ beta)
            # 勾配: Σ_r Σ_i w_i (x_i - Σ_j p_j x_j)
            wr = np.bincount(codes, weights=w, minlength=n_races)
            ex = np.vstack([np.bincount(codes, weights=p * Xv[:, k], minlength=n_races) for k in range(Xv.shape[1])]).T
            grad = -(w @ Xv - wr @ ex) + 2 * self.l2 * beta
            return loss, grad

        res = minimize(nll, np.ones(Xv.shape[1]), jac=True, method='L-BFGS-B')
        if not res.success:
            raise RuntimeError(f'conditional logit did not converge: {res.message}')
        self.coef_ = res.x
        self.columns_ = list(X.columns)
        self.n_races_ = int(n_races)
        self.nll_ = float(res.fun)
        return self

    def predict_proba(self, X: pd.DataFrame, race_ids) -> np.ndarray:
        if self.coef_ is None:
            raise RuntimeError('not fitted')
        return race_softmax(X[self.columns_].to_numpy(dtype=float) @ self.coef_, self._codes(race_ids))
