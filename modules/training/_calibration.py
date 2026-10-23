from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

import numpy as np


def _clip_proba(p: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    return np.clip(p, eps, 1.0 - eps)


def _logit(p: np.ndarray) -> np.ndarray:
    p = _clip_proba(p)
    return np.log(p / (1.0 - p))


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    # stable sigmoid
    out = np.empty_like(x, dtype=float)
    pos = x >= 0
    out[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    expx = np.exp(x[~pos])
    out[~pos] = expx / (1.0 + expx)
    return out


CalibrationMethod = Literal["platt", "isotonic"]


@dataclass(frozen=True)
class Calibrator:
    method: CalibrationMethod
    transform: Callable[[np.ndarray], np.ndarray]

    def __call__(self, p: np.ndarray) -> np.ndarray:
        return self.transform(p)


def fit_calibrator(method: CalibrationMethod, y_true, p_pred) -> Calibrator:
    """Fit a probability calibrator on a validation set.

    Inputs:
    - y_true: array-like of 0/1
    - p_pred: array-like of predicted probabilities in [0,1]

    Returns:
    - Calibrator: callable mapping p -> calibrated_p
    """
    y = np.asarray(y_true, dtype=int)
    p = _clip_proba(np.asarray(p_pred, dtype=float))

    if method == "platt":
        # LogisticRegression on logit(p)
        from sklearn.linear_model import LogisticRegression

        x = _logit(p).reshape(-1, 1)
        lr = LogisticRegression(solver="lbfgs", max_iter=1000)
        lr.fit(x, y)

        a = float(lr.coef_[0, 0])
        b = float(lr.intercept_[0])

        def _f(pp: np.ndarray) -> np.ndarray:
            xx = _logit(pp)
            return _sigmoid(a * xx + b)

        return Calibrator(method="platt", transform=_f)

    if method == "isotonic":
        from sklearn.isotonic import IsotonicRegression

        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(p, y)

        def _f(pp: np.ndarray) -> np.ndarray:
            pp = _clip_proba(pp)
            return np.asarray(iso.predict(pp), dtype=float)

        return Calibrator(method="isotonic", transform=_f)

    raise ValueError(f"Unknown calibration method: {method}")
