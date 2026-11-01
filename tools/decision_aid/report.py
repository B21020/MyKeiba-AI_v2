"""紙上の検証（複勝・固定した規則）の累計の成績を出す。

- 購入数・的中・回収率と開催日単位の95%区間・上位3回を除いた回収率、月ごとの内訳
- 開発期間の同じ規則の結果（frozen_rule.json の backtest）と並べる
- 評価の時点（事前に決めたもの）：3か月ごとに途中報告、1年または300件で判定。
  区間の下限が100%を上回らない限り、お金を使う運用には進まない

使い方:
  .venv/Scripts/python.exe tools/decision_aid/report.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.decision_aid.core import Store, load_rule  # noqa: E402
from tools.decision_aid.live import DB, RULE  # noqa: E402

DECISION_BETS = 300
DECISION_DAYS = 365


def monthly(conn: sqlite3.Connection) -> list[dict]:
    df = pd.read_sql_query(
        'SELECT substr(s.date, 1, 7) AS month, b.stake, r.payout, r.refunded FROM paper_bets b JOIN schedule s USING (race_id) '
        'JOIN bet_results r USING (race_id, horse_number) WHERE r.refunded = 0', conn)
    if df.empty:
        return []
    g = df.groupby('month').agg(bets=('stake', 'size'), stake=('stake', 'sum'), payout=('payout', 'sum'),
                                hits=('payout', lambda s: int((s > 0).sum())))
    g['roi'] = g['payout'] / g['stake']
    return g.reset_index().round(4).to_dict('records')


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--db', default=str(DB))
    ap.add_argument('--rule', default=str(RULE))
    a = ap.parse_args()
    rule, store = load_rule(Path(a.rule)), Store(Path(a.db))
    total = store.paper_summary()
    first = store.conn.execute('SELECT MIN(s.date) FROM paper_bets b JOIN schedule s USING (race_id)').fetchone()[0]
    days_since = (pd.Timestamp.now() - pd.Timestamp(first)).days if first else 0
    due = total['bets'] >= DECISION_BETS or days_since >= DECISION_DAYS
    res = {'rule_hash': rule['rule_hash'], 'first_date': first, 'days_since_first': days_since, 'total': total,
           'monthly': monthly(store.conn), 'backtest': rule['backtest'],
           'decision_point_reached': due,
           'verdict': None if not due else (
               'CI lower bound above 100%: consider a small real-money trial' if total.get('roi_ci95', [0])[0] > 1.0
               else 'CI includes or is below 100%: keep as paper only')}
    print(json.dumps(res, ensure_ascii=False, indent=2, default=float))


if __name__ == '__main__':
    main()
