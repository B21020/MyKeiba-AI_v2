"""紙上の検証を精算する：判断したレースの結果ページ（netkeiba の公開ページ）を取り、複勝の払戻で精算する。

- 1レース1リクエスト、前のリクエストから最低1.2秒空ける。取得したページは原文のまま保存し、再実行時は保存済みを使う
- 払戻は modules/preparing/_get_rawdata.get_rawdata_return、着順は get_rawdata_results で読む
- 取消・除外の馬、レースの中止は返還（払戻 = 賭け金）
- 結果がまだ出ていないレース（払戻表が無い）は精算せず、次回に回す

使い方:
  .venv/Scripts/python.exe tools/decision_aid/settle.py --date 20261107
"""
from __future__ import annotations

import argparse
import contextlib
import io
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from modules.evaluation import fukusho_payouts  # noqa: E402
from modules.preparing._get_rawdata import get_rawdata_results, get_rawdata_return  # noqa: E402
from tools.decision_aid.core import WITHDRAWN, Store  # noqa: E402
from tools.odds_snapshots import JST  # noqa: E402

URL = 'https://race.netkeiba.com/race/result.html?race_id={race_id}'
DB = ROOT / 'data/decision_aid/decision_aid.sqlite'
PAGES = ROOT / 'data/decision_aid/results/race'
MIN_GAP = 1.2


def parse_result(path: Path) -> tuple[dict[int, float], set[int], bool] | None:
    """(馬番 → 複勝の払戻（100円あたり）, 取消・除外の馬番, レース中止) 。払戻表が無ければ None。"""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        rt = get_rawdata_return([str(path)])
        res = get_rawdata_results([str(path)])
    if rt is None or len(rt) == 0:
        return None
    rt = rt.copy()
    rt[2] = rt[2].astype(str).map(lambda s: re.sub(r'[^\dbr,]', '', s))   # '190円' のような単位を落とす
    fk = fukusho_payouts(rt)
    withdrawn = set()
    if res is not None and len(res):
        col = '着順' if '着順' in res.columns else res.columns[0]
        num = '馬番' if '馬番' in res.columns else None
        if num:
            withdrawn = {int(n) for n, c in zip(res[num], res[col]) if str(c).strip() in WITHDRAWN}
    if fk.empty:
        return {}, withdrawn, True   # 払戻表はあるが複勝が無い：中止または発売なし → 返還として扱う
    return dict(zip(fk['馬番'], fk['fukusho_payout'])), withdrawn, False


def settle(date: str, db: Path = DB, session=None, sleep=time.sleep) -> dict:
    store = Store(db)
    PAGES.mkdir(parents=True, exist_ok=True)
    session = session or requests.Session()
    last = 0.0
    done, waiting = 0, []
    for rid in store.unsettled(date):
        path = PAGES / f'{rid}.bin'
        if not path.exists():
            sleep(max(0.0, MIN_GAP - (time.monotonic() - last)))
            r = session.get(URL.format(race_id=rid), headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            last = time.monotonic()
            r.raise_for_status()
            path.write_bytes(r.content)
        parsed = parse_result(path)
        if parsed is None:
            path.unlink()   # 結果がまだ出ていない：次回に取り直す
            waiting.append(rid)
            continue
        payouts, withdrawn, cancelled = parsed
        store.record_settlement(rid, payouts, withdrawn, cancelled, URL.format(race_id=rid), datetime.now(JST))
        done += 1
    return {'settled_races': done, 'waiting': waiting, 'summary_today': store.paper_summary(f'{date[:4]}-{date[4:6]}-{date[6:]}')}


def settle_from_tables(db: Path, return_tables: pd.DataFrame, dates: list[str]) -> int:
    """再生用：保存済みの払戻表（data/raw/return_tables.pickle）で精算する。取消の情報は使わない。"""
    store = Store(db)
    fk = fukusho_payouts(return_tables)
    pays = {rid: dict(zip(g['馬番'], g['fukusho_payout'])) for rid, g in fk.groupby('race_id')}
    n = 0
    for date in dates:
        for rid in store.unsettled(date):
            if rid in pays:
                store.record_settlement(rid, pays[rid], set(), False, 'data/raw/return_tables.pickle', datetime.now(JST))
                n += 1
    return n


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--date', required=True)
    ap.add_argument('--db', default=str(DB))
    a = ap.parse_args()
    print(settle(a.date, Path(a.db)))


if __name__ == '__main__':
    main()
