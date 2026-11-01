"""意思決定の補助の中身：6分前の配信から各馬の数字を出し、紙上の検証の対象を決めて、記録する。

数字はすべて固定した規則（freeze_rule.py の JSON）から出す。規則のハッシュが合わなければ使わない。
紙上の検証は、レースごとに1回だけ決める（同じレースの2回目の判断は記録を拒否する）。
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from modules.evaluation import bootstrap_by_day, harville_place, place_slots  # noqa: E402
from tools.decision_aid.freeze_rule import rule_hash  # noqa: E402

VENUES = {'01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京', '06': '中山', '07': '中京', '08': '京都',
          '09': '阪神', '10': '小倉'}
WITHDRAWN = {'取消', '取', '除外', '除'}

SCHEMA = """
CREATE TABLE IF NOT EXISTS schedule (race_id TEXT PRIMARY KEY, date TEXT NOT NULL, post_time TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (
    race_id TEXT PRIMARY KEY, published_at TEXT, received_at TEXT, decided_at TEXT NOT NULL,
    skipped INTEGER NOT NULL, reason TEXT, runners INTEGER, win_pool INTEGER, rule_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS horses (
    race_id TEXT NOT NULL, horse_number INTEGER NOT NULL, win_odds REAL, win_popularity INTEGER,
    place_low REAL, place_high REAL, p_win REAL, p_place REAL, ev_win REAL, ev_place REAL, pick INTEGER NOT NULL,
    PRIMARY KEY (race_id, horse_number));
CREATE TABLE IF NOT EXISTS paper_bets (
    race_id TEXT NOT NULL, horse_number INTEGER NOT NULL, stake REAL NOT NULL, p_place REAL, place_low REAL,
    place_high REAL, ev REAL, decided_at TEXT NOT NULL, rule_hash TEXT NOT NULL, PRIMARY KEY (race_id, horse_number));
CREATE TABLE IF NOT EXISTS settlements (race_id TEXT PRIMARY KEY, settled_at TEXT NOT NULL, source TEXT, race_cancelled INTEGER);
CREATE TABLE IF NOT EXISTS bet_results (
    race_id TEXT NOT NULL, horse_number INTEGER NOT NULL, payout REAL NOT NULL, refunded INTEGER NOT NULL,
    PRIMARY KEY (race_id, horse_number));
"""


class RuleError(ValueError):
    pass


def load_rule(path: Path) -> dict:
    rule = json.loads(Path(path).read_text(encoding='utf-8'))
    if rule_hash(rule) != rule.get('rule_hash'):
        raise RuleError('frozen rule hash mismatch; the rule file was edited')
    return rule


def race_label(race_id: str) -> str:
    return f"{VENUES.get(race_id[4:6], race_id[4:6])}{int(race_id[-2:])}R"


def latest_t6(records: list[dict], post_time: datetime, before_min: int) -> dict | None:
    """発表が発走 before_min 分前以前の、最新の中間配信（データ区分1）。"""
    cutoff = post_time - timedelta(minutes=before_min)
    best = None
    for r in records:
        if r['data_kind'] != '1' or not r['source_published_at']:
            continue
        pub = datetime.fromisoformat(r['source_published_at'])
        if pub <= cutoff and (best is None or pub > datetime.fromisoformat(best['source_published_at'])):
            best = r
    return best


def race_view(rec: dict, rule: dict) -> list[dict]:
    """各馬の数字。単勝オッズの無い馬（取消など）は除く。"""
    quotes = [q for q in rec['quotes'] if q['win_odds']]
    if not quotes:
        return []
    odds = np.array([q['win_odds'] for q in quotes], float)
    inv = 1.0 / odds
    q6 = inv / inv.sum()
    beta = rule['win_calibration']['beta_log_q6']
    w = q6 ** beta
    p_win = w / w.sum()
    slots = place_slots(len(quotes))
    pc = rule['place_calibration']
    sale = rec.get('place_sale') == '7' and slots > 0
    if slots > 0:
        h = np.clip(harville_place(q6, slots), 1e-6, 1 - 1e-6)
        p_place = 1 / (1 + np.exp(-(pc['intercept'] + pc['slope_logit_harville'] * np.log(h / (1 - h)))))
    else:
        p_place = np.full(len(quotes), np.nan)
    thr = rule['paper_rule']['threshold']
    out = []
    for i, q in enumerate(quotes):
        ev_place = float(p_place[i] * q['place_low']) if sale and q['place_low'] else None
        out.append({'horse_number': q['horse_number'], 'win_odds': q['win_odds'], 'win_popularity': q['win_popularity'],
                    'place_low': q['place_low'], 'place_high': q['place_high'], 'p_win': float(p_win[i]),
                    'p_place': None if np.isnan(p_place[i]) else float(p_place[i]),
                    'ev_win': float(p_win[i] * q['win_odds']), 'ev_place': ev_place,
                    'pick': int(ev_place is not None and ev_place >= thr)})
    return out


class Store:
    def __init__(self, path: Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def save_schedule(self, date: str, sched: pd.DataFrame):
        with self.conn:
            self.conn.executemany('INSERT OR IGNORE INTO schedule VALUES (?, ?, ?)',
                                  [(r.race_id, date, pd.Timestamp(r.post_time).isoformat()) for r in sched.itertuples()])

    def decided(self, race_id: str) -> bool:
        return self.conn.execute('SELECT 1 FROM decisions WHERE race_id = ?', (race_id,)).fetchone() is not None

    def record_decision(self, race_id: str, rec: dict | None, received_at: datetime | None, decided_at: datetime,
                        post_time: datetime, rule: dict) -> dict:
        """判断を1回だけ記録する。受け取りが発走 skip_after_min 分前より後、または配信が無ければ見送り。"""
        skip_after = post_time - timedelta(minutes=rule['paper_rule']['skip_after_min'])
        if rec is None:
            skipped, reason, horses = 1, 'no T-6 snapshot received', []
        elif received_at > skip_after:
            skipped, reason, horses = 1, 'T-6 snapshot received after T-4', race_view(rec, rule)
        else:
            skipped, reason, horses = 0, None, race_view(rec, rule)
        h = rule['rule_hash']
        with self.conn:   # 1つの取引：既に判断済みなら IntegrityError で何も書かない
            self.conn.execute('INSERT INTO decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (
                race_id, rec['source_published_at'] if rec else None, received_at.isoformat() if received_at else None,
                decided_at.isoformat(), skipped, reason, len(horses) or None, rec.get('win_pool') if rec else None, h))
            for x in horses:
                self.conn.execute('INSERT INTO horses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (
                    race_id, x['horse_number'], x['win_odds'], x['win_popularity'], x['place_low'], x['place_high'],
                    x['p_win'], x['p_place'], x['ev_win'], x['ev_place'], x['pick'] if not skipped else 0))
                if x['pick'] and not skipped:
                    self.conn.execute('INSERT INTO paper_bets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (
                        race_id, x['horse_number'], float(rule['paper_rule']['stake_yen']), x['p_place'], x['place_low'],
                        x['place_high'], x['ev_place'], decided_at.isoformat(), h))
        return {'skipped': skipped, 'reason': reason, 'picks': sum(x['pick'] for x in horses) if not skipped else 0}

    def record_settlement(self, race_id: str, payouts: dict[int, float], withdrawn: set[int], race_cancelled: bool,
                          source: str, settled_at: datetime) -> int:
        """紙上の購入を精算する。取消・除外の馬とレースの中止は返還（払戻 = 賭け金）。"""
        bets = self.conn.execute('SELECT horse_number, stake FROM paper_bets WHERE race_id = ?', (race_id,)).fetchall()
        with self.conn:
            self.conn.execute('INSERT INTO settlements VALUES (?, ?, ?, ?)', (race_id, settled_at.isoformat(), source,
                                                                               int(race_cancelled)))
            for b in bets:
                refunded = race_cancelled or b['horse_number'] in withdrawn
                pay = b['stake'] if refunded else payouts.get(b['horse_number'], 0.0) * b['stake'] / 100
                self.conn.execute('INSERT INTO bet_results VALUES (?, ?, ?, ?)', (race_id, b['horse_number'], pay, int(refunded)))
        return len(bets)

    def unsettled(self, date: str) -> list[str]:
        rows = self.conn.execute(
            'SELECT s.race_id FROM schedule s JOIN decisions d USING (race_id) LEFT JOIN settlements t USING (race_id) '
            'WHERE s.date = ? AND t.race_id IS NULL ORDER BY s.post_time', (date,)).fetchall()
        return [r['race_id'] for r in rows]

    def paper_summary(self, date: str | None = None) -> dict:
        q = ('SELECT b.race_id, s.date, b.stake, r.payout, r.refunded FROM paper_bets b JOIN schedule s USING (race_id) '
             'LEFT JOIN bet_results r USING (race_id, horse_number)')
        df = pd.read_sql_query(q + (' WHERE s.date = ?' if date else ''), self.conn, params=(date,) if date else ())
        res = {'bets': len(df), 'settled': int(df['payout'].notna().sum()), 'pending': int(df['payout'].isna().sum())}
        s = df[df['payout'].notna() & (df['refunded'] == 0)]
        res['refunded'] = int((df['refunded'] == 1).sum())
        if s.empty:
            return res
        pay = s['payout'] / s['stake']
        res.update(hits=int((s['payout'] > 0).sum()), stake=float(s['stake'].sum()), payout=float(s['payout'].sum()),
                   roi=float(s['payout'].sum() / s['stake'].sum()),
                   roi_without_top3=float((pay.sum() - pay.nlargest(3).sum()) / len(s)) if len(s) > 3 else None)
        daily = s.groupby('date').agg(stake=('stake', 'sum'), payout=('payout', 'sum'))
        res['days'] = len(daily)
        if len(daily) >= 2:
            res['roi_ci95'] = bootstrap_by_day(daily)['roi_ci95']
        return res

    def state(self, date: str, now: datetime, rule: dict) -> dict:
        sched = self.conn.execute(
            'SELECT s.race_id, s.post_time, d.published_at, d.received_at, d.skipped, d.reason, d.win_pool, t.race_id AS settled '
            'FROM schedule s LEFT JOIN decisions d USING (race_id) LEFT JOIN settlements t USING (race_id) '
            'WHERE s.date = ? ORDER BY s.post_time', (date,)).fetchall()
        races = []
        for r in sched:
            post = datetime.fromisoformat(r['post_time'])
            status = ('settled' if r['settled'] else 'skipped' if r['skipped'] == 1 else 'decided' if r['skipped'] == 0
                      else 'waiting')
            horses = [dict(h) for h in self.conn.execute(
                'SELECT * FROM horses WHERE race_id = ? ORDER BY win_odds', (r['race_id'],)).fetchall()]
            races.append({'race_id': r['race_id'], 'label': race_label(r['race_id']), 'post_time': r['post_time'],
                          'seconds_to_post': (post - now).total_seconds(),
                          'seconds_to_decision': (post - timedelta(minutes=rule['paper_rule']['skip_after_min']) - now).total_seconds(),
                          'status': status, 'published_at': r['published_at'], 'received_at': r['received_at'],
                          'reason': r['reason'], 'win_pool_yen': r['win_pool'] * 100 if r['win_pool'] else None,
                          'picks': sum(h['pick'] for h in horses), 'horses': horses})
        return {'date': date, 'now': now.isoformat(), 'races': races, 'paper_today': self.paper_summary(date),
                'paper_total': self.paper_summary(), 'rule': {k: rule[k] for k in ('paper_rule', 'rule_hash', 'backtest')},
                'reference': rule['reference']}
