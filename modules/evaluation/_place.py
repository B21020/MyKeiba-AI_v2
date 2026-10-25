"""複勝（3着以内、7頭以下は2着以内）の確率：単勝の市場から Harville で出すものと、複勝のオッズから逆算するもの。"""
from __future__ import annotations

import numpy as np
import pandas as pd

PLACE_PAYBACK = 0.8   # 複勝の払戻率（控除率20%）
FLOOR_ODDS = 1.1      # これ以下は最低払戻に張り付いていて、売れた割合が読めない
MAX_PROB = 0.95       # 張り付いた本命は残りの割合をすべて受け取り1に近づくので、ここで止める


def place_slots(runners: int) -> int:
    """JRA の複勝の対象着順：8頭以上は3着まで、5〜7頭は2着まで（4頭以下は発売なし → 0）。"""
    return 3 if runners >= 8 else 2 if runners >= 5 else 0


def harville_place(p: np.ndarray, k: int) -> np.ndarray:
    """勝つ確率 p（合計1）から、Harville の式で k 着以内に入る確率を出す（k = 1, 2, 3）。"""
    p = np.asarray(p, float)
    out = p.copy()
    if k == 1:
        return out
    rest = 1.0 - p
    a = np.outer(p, p) / rest[:, None]           # a[j, i] = j が1着で i が2着
    np.fill_diagonal(a, 0.0)
    out += a.sum(axis=0)
    if k == 2:
        return out
    if k != 3:
        raise ValueError('k must be 1, 2 or 3')
    # b[j, m] = j が1着・m が2着で、残りの中から i が3着になる確率の分母を除いた部分
    denom = 1.0 - p[:, None] - p[None, :]
    b = a / np.where(denom > 0, denom, np.inf)
    np.fill_diagonal(b, 0.0)
    total = b.sum()
    out += p * (total - b.sum(axis=0) - b.sum(axis=1))
    return out


def discounted_place(p: np.ndarray, k: int, lam2: float, lam3: float) -> np.ndarray:
    """べき乗で割り引いた Harville（Lo・Bacon-Shone による Henery のモデルの近似）。

    2着は 1着以外の中で p^lam2 に比例、3着は 1・2着以外の中で p^lam3 に比例する。lam2 = lam3 = 1 で Harville と同じ。"""
    p = np.asarray(p, float)
    out = p.copy()
    if k == 1:
        return out
    w2 = p ** lam2
    a = np.outer(p, w2) / (w2.sum() - w2)[:, None]   # a[j, m] = j が1着・m が2着
    np.fill_diagonal(a, 0.0)
    out += a.sum(axis=0)
    if k == 2:
        return out
    if k != 3:
        raise ValueError('k must be 1, 2 or 3')
    w3 = p ** lam3
    rest = w3.sum() - w3[:, None] - w3[None, :]
    b = a / np.where(rest > 0, rest, np.inf)
    np.fill_diagonal(b, 0.0)
    out += w3 * (b.sum() - b.sum(axis=0) - b.sum(axis=1))
    return out


def simulated_place(p: np.ndarray, k: int, model: str = 'normal', shape: float = 4.0, n_sims: int = 20000,
                    n_iter: int = 200, seed: int = 0) -> np.ndarray:
    """走破タイムのモデルを、勝つ確率が p に合うように合わせ、k 着以内に入る確率をシミュレーションで出す。

    - 'normal'：Henery（1981）。タイム = 標準正規 − 強さ
    - 'gamma' ：Stern（1990）。タイム = Gamma(shape) × exp(−強さ)。shape = 1 で Harville と同じ
    同じ乱数を使い回して、強さを log(p / 勝つ割合) で直していく。"""
    p = np.asarray(p, float)
    n = len(p)
    rng = np.random.default_rng(seed)
    noise = rng.standard_normal((n_sims, n)) if model == 'normal' else rng.gamma(shape, size=(n_sims, n))
    if model not in ('normal', 'gamma'):
        raise ValueError('model must be normal or gamma')

    def times(m):
        return noise - m if model == 'normal' else noise * np.exp(-m)

    m = np.log(p)
    for _ in range(n_iter):
        win = np.bincount(times(m).argmin(axis=1), minlength=n) / n_sims
        step = np.log(p) - np.log(np.maximum(win, 0.5 / n_sims))
        m += 0.5 * step / (1.0 if model == 'normal' else shape)  # 半分ずつ直す（行き過ぎて振動しないように）
        m -= m.mean()
        if np.abs(step[p > 0.01]).max() < 0.005:
            break
    top = np.argpartition(times(m), k - 1, axis=1)[:, :k]
    return np.bincount(top.ravel(), minlength=n) / n_sims


def place_pool_shares(high, slots: int, n_iter: int = 200) -> np.ndarray:
    """1レースの複勝オッズの上限から、各馬に売れた割合 x（合計1）を逆算する。

    上限は、ほかの的中馬が最も売れていない2頭（2着までなら1頭）のときのオッズ:
      high_i − 1 = (払戻率 − x_i − 相手の x の合計) / (slots · x_i)
      → x_i = (払戻率 − 相手の x の合計) / (slots · (high_i − 1) + 1)
    最低払戻に張り付いた馬（上限 ≤ 1.1）は式で決まらないので、合計1になる残りを等分する。"""
    high = np.asarray(high, float)
    n, partners = len(high), slots - 1
    floored = high <= FLOOR_ODDS + 1e-9
    x = np.full(n, 1.0 / n)
    for _ in range(n_iter):
        new = x.copy()
        for i in np.flatnonzero(~floored):
            others = np.sort(np.delete(x, i))[:partners].sum()
            new[i] = max(PLACE_PAYBACK - others, 1e-6) / (slots * (high[i] - 1.0) + 1.0)
        if floored.any():
            new[floored] = max(1.0 - new[~floored].sum(), 1e-6) / floored.sum()
        new /= new.sum()
        if np.abs(new - x).max() < 1e-10:
            x = new
            break
        x = new
    return x


def place_market_probs(high, race_ids, slots) -> np.ndarray:
    """複勝の売れた割合 × 対象着順の数を、複勝の市場の確率とする（MAX_PROB で止める）。"""
    r = pd.Series(race_ids).astype(str).to_numpy()
    high, slots = np.asarray(high, float), np.asarray(slots)
    out = np.empty(len(high))
    for idx in pd.Series(np.arange(len(r))).groupby(r, sort=False).groups.values():
        idx = np.asarray(idx)
        out[idx] = place_pool_shares(high[idx], int(slots[idx[0]])) * slots[idx[0]]
    return np.clip(out, 1e-6, MAX_PROB)
