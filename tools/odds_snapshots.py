"""Section 4.3: JV-Link O1 ingestion with distinct publication/observation times.

Format references: official JV-Data 4.9.0.1, O1; SDK 5.0.0 64bit Python sample.
Historical replay explicitly differs from a quote actually observed before a bet.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
JST = timezone(timedelta(hours=9))


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp must include a UTC offset")
    return result.astimezone(JST)


def netkeiba_id(jv_key: str) -> str:
    if not re.fullmatch(r"\d{16}", jv_key):
        raise ValueError("JV race key must have 16 digits")
    datetime.strptime(jv_key[:8], "%Y%m%d")
    if not 1 <= int(jv_key[8:10]) <= 10 or not 1 <= int(jv_key[-2:]) <= 12:
        raise ValueError("Not a JRA race key")
    return jv_key[:4] + jv_key[8:]


def quote_value(raw: str):
    special = {"": "unregistered", "0000": "no_votes", "----": "cancelled_before_sale", "****": "cancelled_after_sale"}
    raw = raw.strip()
    if raw in special:
        return None, special[raw]
    if not re.fullmatch(r"\d{4}", raw):
        raise ValueError(f"Invalid odds field: {raw!r}")
    return int(raw) / 10, "capped" if raw == "9999" else "quoted"


def popularity(raw: str):
    text = raw.strip()
    if not text or text in ("--", "**"):
        return None
    if not text.isdigit() or not 1 <= int(text) <= 28:
        raise ValueError(f"Invalid popularity: {raw!r}")
    return int(text)


def parse_o1(raw: bytes) -> dict:
    if len(raw) == 960:
        raw += b"\r\n"
    if len(raw) != 962 or raw[-2:] != b"\r\n":
        raise ValueError("O1 must be 962 bytes including CRLF")
    text = raw.decode("ascii")  # O1 is a numeric/ASCII record, not a horse-name record.
    if text[:2] != "O1" or text[2] not in "0123459":
        raise ValueError("Not a supported O1 record")
    key = text[11:27]
    rid = netkeiba_id(key)
    race_date = datetime.strptime(key[:8], "%Y%m%d").replace(tzinfo=JST)
    published = None
    if text[2] == "1":
        field = text[27:35]
        if not re.fullmatch(r"\d{8}", field) or field == "00000000":
            raise ValueError("Interim odds have no valid publication time")
        candidates = []
        for year in (race_date.year - 1, race_date.year, race_date.year + 1):
            try:
                date = datetime.strptime(str(year) + field, "%Y%m%d%H%M").replace(tzinfo=JST)
                if race_date - timedelta(days=7) <= date < race_date + timedelta(days=1):
                    candidates.append(date)
            except ValueError:
                pass
        if len(candidates) != 1:
            raise ValueError("Cannot resolve publication year safely")
        published = candidates[0]
    registered, runners = int(text[35:37]), int(text[37:39])
    if not 0 <= runners <= registered <= 28:
        raise ValueError("Invalid registered/running horse counts")
    flags = text[39:42]
    if any(c not in "0137" for c in flags) or text[42] not in "023":
        raise ValueError("Invalid sale or place payout flags")
    rows = {}
    for i in range(28):
        win = text[43 + 8 * i:51 + 8 * i]
        place = text[267 + 12 * i:279 + 12 * i]
        numbers = {int(v) for v in [win[:2].strip(), place[:2].strip()] if v}
        if not numbers:
            continue
        if len(numbers) != 1 or not 1 <= next(iter(numbers)) <= 28:
            raise ValueError("Win/place horse numbers are inconsistent")
        number = next(iter(numbers))
        if number in rows:
            raise ValueError("Duplicate horse slot")
        win_odds, win_status = quote_value(win[2:6])
        low, low_status = quote_value(place[2:6])
        high, high_status = quote_value(place[6:10])
        if low is not None and high is not None and low > high:
            raise ValueError("Place odds lower bound exceeds upper bound")
        rows[number] = {
            "horse_number": number, "win_odds": win_odds, "win_status": win_status,
            "win_popularity": popularity(win[6:8]), "place_low": low, "place_high": high,
            "place_low_status": low_status, "place_high_status": high_status,
            "place_popularity": popularity(place[10:12]),
        }
    if len(rows) > registered:
        raise ValueError("More horse slots than registrations")
    return {
        "race_id": rid, "jv_key": key, "race_date": race_date.date().isoformat(),
        "data_kind": text[2], "source_published_at": published.isoformat() if published else None,
        "registered": registered, "runners": runners, "win_sale": flags[0],
        "place_sale": flags[1], "place_paid_to": int(text[42]), "quotes": list(rows.values()),
        # 票数合計（単位百円、返還分を含む）。JV-Data 仕様書 O1 の 928・939・950 バイト目から各11桁
        "win_pool": pool_value(text[927:938]), "place_pool": pool_value(text[938:949]),
        "wakuren_pool": pool_value(text[949:960]),
    }


def parse_o2(raw: bytes) -> dict:
    """馬連オッズ（O2、2042バイト）。JV-Data 仕様書 8．オッズ2（馬連）。

    組番 4 桁（例 0102）・オッズ 6 桁（0.1倍単位）・人気 3 桁 × 153 組。馬連票数合計 11 桁（単位百円）。"""
    if len(raw) == 2040:
        raw += b"\r\n"
    if len(raw) != 2042 or raw[-2:] != b"\r\n":
        raise ValueError("O2 must be 2042 bytes including CRLF")
    text = raw.decode("ascii")
    if text[:2] != "O2" or text[2] not in "0123459":
        raise ValueError("Not a supported O2 record")
    key = text[11:27]
    rid = netkeiba_id(key)
    published = None
    if text[2] == "1":
        published = resolve_published(text[27:35], datetime.strptime(key[:8], "%Y%m%d").replace(tzinfo=JST))
    registered, runners = int(text[35:37]), int(text[37:39])
    combos = []
    for i in range(153):
        f = text[40 + 13 * i:53 + 13 * i]
        pair = f[:4].strip()
        if not pair:
            continue
        a, b = int(pair[:2]), int(pair[2:])
        if not 1 <= a < b <= 18:
            raise ValueError(f"Invalid quinella pair: {pair!r}")
        odds_raw = f[4:10].strip()
        special = {"": "unregistered", "000000": "no_votes", "------": "cancelled_before_sale", "******": "cancelled_after_sale"}
        if odds_raw in special:
            odds, status = None, special[odds_raw]
        elif re.fullmatch(r"\d{6}", odds_raw):
            odds, status = int(odds_raw) / 10, "capped" if odds_raw == "099999" else "quoted"
        else:
            raise ValueError(f"Invalid quinella odds: {odds_raw!r}")
        combos.append({"first": a, "second": b, "odds": odds, "status": status})
    return {"race_id": rid, "jv_key": key, "data_kind": text[2],
            "source_published_at": published.isoformat() if published else None,
            "registered": registered, "runners": runners, "sale": text[39],
            "combos": combos, "pool": pool_value(text[2029:2040])}


def resolve_published(field: str, race_date: datetime) -> datetime:
    """発表月日時分（mmddHHMM）に年を補う。レース日の7日前〜翌日に入る年がちょうど1つであること。"""
    if not re.fullmatch(r"\d{8}", field) or field == "00000000":
        raise ValueError("Interim odds have no valid publication time")
    candidates = []
    for year in (race_date.year - 1, race_date.year, race_date.year + 1):
        try:
            date = datetime.strptime(str(year) + field, "%Y%m%d%H%M").replace(tzinfo=JST)
        except ValueError:
            continue
        if race_date - timedelta(days=7) <= date < race_date + timedelta(days=1):
            candidates.append(date)
    if len(candidates) != 1:
        raise ValueError("Cannot resolve publication year safely")
    return candidates[0]


def pool_value(raw: str):
    raw = raw.strip()
    if not raw:
        return None
    if not raw.isdigit():
        raise ValueError(f"Invalid pool total: {raw!r}")
    return int(raw)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id TEXT PRIMARY KEY, source TEXT NOT NULL, mode TEXT NOT NULL,
        race_id TEXT NOT NULL, jv_key TEXT NOT NULL, race_date TEXT NOT NULL,
        data_kind TEXT NOT NULL, source_published_at TEXT, published_epoch REAL,
        observed_at TEXT NOT NULL, observed_epoch REAL NOT NULL,
        registered INTEGER NOT NULL, runners INTEGER,
        win_sale TEXT NOT NULL, place_sale TEXT NOT NULL, place_paid_to INTEGER,
        payload_sha256 TEXT NOT NULL, payload_path TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS snapshots_asof ON snapshots(race_id, mode, published_epoch);
    CREATE TABLE IF NOT EXISTS quotes (
        snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id), horse_number INTEGER NOT NULL,
        win_odds REAL, win_status TEXT NOT NULL, win_popularity INTEGER,
        place_low REAL, place_high REAL, place_low_status TEXT NOT NULL,
        place_high_status TEXT NOT NULL, place_popularity INTEGER,
        PRIMARY KEY(snapshot_id, horse_number)
    );
    CREATE TABLE IF NOT EXISTS observations (
        snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id), observed_at TEXT NOT NULL,
        PRIMARY KEY(snapshot_id, observed_at)
    );
    CREATE TABLE IF NOT EXISTS decisions (
        decision_id TEXT PRIMARY KEY, snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
        horse_number INTEGER NOT NULL, decision_at TEXT NOT NULL, scheduled_start_at TEXT,
        schedule_observed_at TEXT, model_version TEXT NOT NULL, feature_version TEXT NOT NULL,
        predicted_win REAL, predicted_place REAL, action TEXT NOT NULL, reason TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS schedules (
        schedule_id TEXT PRIMARY KEY, race_id TEXT NOT NULL, scheduled_start_at TEXT NOT NULL,
        observed_at TEXT NOT NULL, source TEXT NOT NULL, source_url TEXT NOT NULL,
        page_sha256 TEXT NOT NULL
    );
    """)
    return conn


def store(conn, raw: bytes, *, observed_at: str, mode: str, archive: Path, expected_race_id: str | None = None):
    return store_normalized(conn, parse_o1(raw), raw, observed_at=observed_at, mode=mode,
                            archive=archive, source="jra_van", expected_race_id=expected_race_id)


def store_normalized(conn, record: dict, raw: bytes, *, observed_at: str, mode: str,
                     archive: Path, source: str, expected_race_id: str | None = None):
    """Provider-specific parsers share storage without disguising the source."""
    if source not in ("jra_van", "netkeiba_public"):
        raise ValueError("Unknown provider")
    if mode not in ("historical", "live"):
        raise ValueError("Mode must be historical or live")
    observation = timestamp(observed_at)
    if expected_race_id and record["race_id"] != expected_race_id:
        raise ValueError("Returned record does not match requested race")
    publication = timestamp(record["source_published_at"]) if record["source_published_at"] else None
    if publication and publication > observation:
        raise ValueError("Publication time is in the future relative to observation")
    digest = hashlib.sha256(raw).hexdigest()
    sid = hashlib.sha256(f"{source}:{mode}:{digest}".encode()).hexdigest()
    archive.mkdir(parents=True, exist_ok=True)
    payload = archive / (digest + (".jvd" if source == "jra_van" else ".jsonp"))
    if payload.exists() and payload.read_bytes() != raw:
        raise ValueError("Payload hash collision")
    if not payload.exists():
        payload.write_bytes(raw)
    with conn:
        conn.execute("""INSERT OR IGNORE INTO snapshots VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            sid, source, mode, record["race_id"], record["jv_key"], record["race_date"],
            record["data_kind"], record["source_published_at"], publication.timestamp() if publication else None,
            observation.isoformat(), observation.timestamp(), record["registered"], record["runners"],
            record["win_sale"], record["place_sale"], record["place_paid_to"], digest, str(payload.resolve()),
        ))
        for row in record["quotes"]:
            conn.execute("INSERT OR IGNORE INTO quotes VALUES (?,?,?,?,?,?,?,?,?,?)", (
                sid, row["horse_number"], row["win_odds"], row["win_status"], row["win_popularity"],
                row["place_low"], row["place_high"], row["place_low_status"],
                row["place_high_status"], row["place_popularity"],
            ))
        conn.execute("INSERT OR IGNORE INTO observations VALUES (?,?)", (sid, observation.isoformat()))
    return sid


def select_asof(conn, race_id: str, decision_at: str, *, mode: str, max_age_seconds: int = 600, source: str = "jra_van"):
    if mode not in ("historical", "live") or max_age_seconds < 0:
        raise ValueError("Invalid mode or maximum age")
    decision = timestamp(decision_at)
    observation_filter = " AND observed_epoch <= ?" if mode == "live" else ""
    params = [race_id, mode, source, decision.timestamp()]
    if mode == "live":
        params.append(decision.timestamp())
    snapshot = conn.execute("""SELECT * FROM snapshots WHERE race_id=? AND mode=? AND source=?
        AND data_kind='1' AND published_epoch<=?""" + observation_filter +
        " ORDER BY published_epoch DESC, observed_epoch DESC LIMIT 1", params).fetchone()
    if snapshot is None:
        raise ValueError("No eligible interim snapshot at decision time")
    if decision.timestamp() - snapshot["published_epoch"] > max_age_seconds:
        raise ValueError("Latest eligible snapshot is stale")
    # Select one complete publication, never backfill a cancelled/missing horse
    # with an older quote from a different publication.
    rows = conn.execute("SELECT * FROM quotes WHERE snapshot_id=? ORDER BY horse_number", (snapshot["snapshot_id"],)).fetchall()
    return {"snapshot": dict(snapshot), "quotes": [dict(row) for row in rows]}


def collect(args):
    import win32com.client
    # Official SDK 5.0.0 Python sample uses Dispatch, JVInit and JVGets.
    try:
        jv = win32com.client.Dispatch("JVDTLab.JVLink")
    except Exception as error:
        raise RuntimeError("JV-Link is not available. Install/configure the matching official SDK first.") from error
    code = jv.JVInit("UNKNOWN")
    if code != 0:
        raise RuntimeError(f"JVInit failed ({code}); configure the official client, not a key in this script")
    import csv
    targets = list(csv.DictReader(Path(args.targets).open(encoding="utf-8-sig")))
    conn = connect(Path(args.db))
    log = Path(args.db).parent / "collection_log.jsonl"
    last_end = 0.0
    try:
        for target in targets[:args.limit] if args.limit else targets:
            key = target["jv_key"]
            rid = netkeiba_id(key)
            time.sleep(max(0, 1.2 - (time.monotonic() - last_end)))
            code = jv.JVRTOpen("0B41" if args.mode == "historical" else "0B31", key)
            if code not in (0, -1):
                raise RuntimeError(f"JVRTOpen failed ({code}) for {rid}")
            count = 0
            try:
                if code == 0:
                    while True:
                        result = jv.JVGets(bytearray(110000), 110000, bytearray())
                        read_code, buffer, filename = result
                        read_code = int(read_code)
                        if read_code == 0:
                            break
                        if read_code == -1:
                            continue
                        if read_code < 0:
                            raise RuntimeError(f"JVGets failed ({read_code}) for {rid}")
                        raw = bytes(buffer)[:read_code]
                        observed = datetime.now(JST).isoformat()
                        store(conn, raw, observed_at=observed, mode=args.mode,
                              archive=Path(args.db).parent / "raw", expected_race_id=rid)
                        count += 1
            finally:
                jv.JVClose()
                last_end = time.monotonic()
            with log.open("a", encoding="utf-8") as out:
                out.write(json.dumps({"race_id": rid, "mode": args.mode, "at": datetime.now(JST).isoformat(), "records_read": count, "open_code": code}) + "\n")
            print(json.dumps({"race_id": rid, "records_read": count, "mode": args.mode}), flush=True)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/odds/snapshots.sqlite")
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare-targets")
    prep.add_argument("--start", default="2026-02-16")
    prep.add_argument("--end", default="2026-10-21")
    prep.add_argument("--out", default="data/odds/20261023_setup/targets.csv")
    imp = commands.add_parser("import-o1")
    imp.add_argument("path")
    imp.add_argument("--observed-at", required=True)
    get = commands.add_parser("collect")
    get.add_argument("--targets", required=True)
    get.add_argument("--mode", choices=["historical", "live"], required=True)
    get.add_argument("--limit", type=int, default=1)
    select = commands.add_parser("select")
    select.add_argument("--race-id", required=True)
    select.add_argument("--decision-at", required=True)
    select.add_argument("--mode", choices=["historical", "live"], required=True)
    select.add_argument("--max-age-seconds", type=int, default=600)
    select.add_argument("--source", choices=["jra_van", "netkeiba_public"], default="jra_van")
    args = parser.parse_args()
    if args.command == "prepare-targets":
        import pandas as pd
        progress = json.loads((ROOT / "data/updates/20261023_results/progress.json").read_text(encoding="utf-8"))
        if progress.get("stage") != "complete" or progress.get("unresolved_races") != 0:
            raise ValueError("Section 4.2 must finish before preparing odds targets")
        info = pd.read_pickle(ROOT / "data/raw/race_info.pickle")
        dates = pd.to_datetime(info["date"], format="%Y年%m月%d日", errors="raise")
        mask = dates.between(args.start, args.end)
        rows = [{"race_id": str(rid), "race_date": dates.loc[rid].date().isoformat(),
                 "jv_key": dates.loc[rid].strftime("%Y%m%d") + str(rid)[4:]} for rid in info.index[mask]]
        for row in rows:
            assert netkeiba_id(row["jv_key"]) == row["race_id"]
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).sort_values(["race_date", "race_id"]).to_csv(out, index=False, encoding="utf-8-sig")
        print(json.dumps({"target_races": len(rows), "out": str(out)}))
    elif args.command == "collect":
        if args.limit < 0:
            parser.error("limit must be nonnegative; zero means all")
        collect(args)
    else:
        conn = connect(Path(args.db))
        try:
            if args.command == "import-o1":
                # An imported historical file never claims it was observed live.
                count = 0
                for raw in Path(args.path).read_bytes().splitlines(keepends=True):
                    store(conn, raw, observed_at=args.observed_at, mode="historical", archive=Path(args.db).parent / "raw")
                    count += 1
                print(json.dumps({"imported_records": count, "mode": "historical"}))
            else:
                print(json.dumps(select_asof(conn, args.race_id, args.decision_at, mode=args.mode, max_age_seconds=args.max_age_seconds, source=args.source), ensure_ascii=False, indent=2))
        finally:
            conn.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
