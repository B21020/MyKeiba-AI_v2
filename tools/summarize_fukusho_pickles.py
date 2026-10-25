"""複勝sweep結果pickleのbestを集計して表示するユーティリティ。"""

from __future__ import annotations

import argparse
import pandas as pd


def summarize(path: str, cutoffs: list[int]) -> list[dict]:
    df = pd.read_pickle(path)
    out = []
    for cutoff in cutoffs:
        cand = df[df["n_bets"] >= int(cutoff)]
        if len(cand) == 0:
            out.append({"cutoff": int(cutoff), "ok": False})
            continue
        b = cand.sort_values("return_rate", ascending=False).head(1)
        out.append(
            {
                "cutoff": int(cutoff),
                "ok": True,
                "threshold": float(b.index[0]),
                "return_rate": float(b["return_rate"].iloc[0]),
                "n_bets": int(b["n_bets"].iloc[0]),
                "n_races": int(b["n_races"].iloc[0]),
                "n_hits": int(b["n_hits"].iloc[0]),
                "std": float(b["std"].iloc[0]),
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths", nargs="+", required=True)
    ap.add_argument("--cutoffs", nargs="+", type=int, default=[500, 1000])
    args = ap.parse_args()

    for path in args.paths:
        print("===", path)
        for row in summarize(path, args.cutoffs):
            if not row["ok"]:
                print(f"cutoff={row['cutoff']}: NO CAND")
            else:
                print(
                    "cutoff={cutoff}: threshold={threshold:.6f} return_rate={return_rate:.6f} "
                    "n_bets={n_bets} n_races={n_races} n_hits={n_hits} std={std:.6f}".format(**row)
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
