"""Capture one anonymous public netkeiba win/place quote, without premium flags.

No background watcher, registration, paid endpoint or prediction is created.
Provider publication time and actual observation time are stored separately.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sys
import time

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.odds_snapshots import JST, connect, netkeiba_id, store_normalized, timestamp  # noqa: E402


def parse_context(raw: bytes, race_id: str) -> dict:
    soup = BeautifulSoup(raw, "lxml")
    canonical = soup.select_one('meta[property="og:url"]')
    if canonical is None or f"race_id={race_id}" not in canonical.get("content", ""):
        raise ValueError("Public page identity mismatch")
    title = soup.title.get_text() if soup.title else ""
    match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", title)
    item = soup.select_one(".RaceList_Item02")
    post = re.search(r"(\d{1,2}):(\d{2})発走", item.get_text(" ", strip=True) if item else "")
    count = re.search(r"(\d+)頭", item.get_text(" ", strip=True) if item else "")
    if match is None or post is None or count is None:
        raise ValueError("Cannot identify race date, post time or registered runners")
    year, month, day = map(int, match.groups())
    scheduled = datetime(year, month, day, int(post[1]), int(post[2]), tzinfo=JST)
    key = scheduled.strftime("%Y%m%d") + race_id[4:]
    if netkeiba_id(key) != race_id:
        raise ValueError("Race date and race ID disagree")
    return {"race_id": race_id, "race_date": scheduled.date().isoformat(), "jv_key": key,
            "scheduled_start_at": scheduled.isoformat(), "registered": int(count[1])}


def numeric_quote(value):
    text = str(value).strip()
    if text in ("", "---.-", "--.-", "-", "0", "0.0"):
        return None, "unavailable"
    if any(x in text for x in ("取消", "除外", "****", "----")):
        return None, "cancelled_or_unavailable"
    if not re.fullmatch(r"\d+(?:\.\d+)?", text) or float(text) < 1:
        raise ValueError(f"Invalid public odds: {text!r}")
    return float(text), "capped" if float(text) >= 999.9 else "quoted"


def parse_public(raw: bytes, context: dict) -> dict:
    match = re.fullmatch(r"\s*quoteCapture\((.*)\);?\s*", raw.decode("utf-8"), flags=re.S)
    if match is None:
        raise ValueError("Unexpected JSONP wrapper; never execute the response")
    payload = json.loads(match[1])
    if payload.get("status") != "middle":
        raise ValueError(f"Not actual interim market odds: {payload.get('status')!r}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Public odds body missing")
    published = datetime.strptime(data["official_datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST)
    race_date = datetime.strptime(context["race_date"], "%Y-%m-%d").replace(tzinfo=JST)
    if not 0 <= (race_date.date() - published.date()).days <= 7:
        raise ValueError("Publication date is not within the race's pre-sale window")
    maps = {}
    for bet_type in ("1", "2"):
        quotes = data["odds"].get(bet_type)
        if not isinstance(quotes, dict) or not quotes:
            raise ValueError("Win or place quotes are unavailable")
        maps[bet_type] = {}
        # The public page requests sort=ninki: keys are ranks, row[3] is horse number.
        for row in quotes.values():
            if not isinstance(row, list) or len(row) != 4:
                raise ValueError("Public odds row format changed")
            number = int(row[3])
            if not 1 <= number <= 18 or number in maps[bet_type]:
                raise ValueError("Invalid or duplicated public horse number")
            maps[bet_type][number] = row
    if set(maps["1"]) != set(maps["2"]):
        raise ValueError("Win/place horse sets disagree")
    if len(maps["1"]) != context["registered"]:
        raise ValueError("Quote coverage differs from the public entry list")
    rows = []
    for number, win in sorted(maps["1"].items()):
        place = maps["2"][number]
        value, state = numeric_quote(win[0])
        low, low_state = numeric_quote(place[0])
        high, high_state = numeric_quote(place[1])
        if low is not None and high is not None and low > high:
            raise ValueError("Invalid public place odds bounds")
        def rank(value):
            return int(value) if str(value).isdigit() and int(value) > 0 else None
        rows.append({"horse_number": number, "win_odds": value, "win_status": state,
                     "win_popularity": rank(win[2]), "place_low": low, "place_high": high,
                     "place_low_status": low_state, "place_high_status": high_state,
                     "place_popularity": rank(place[2])})
    # Public pages do not expose all JV-Data sale/refund flags. Mark them unknown,
    # rather than manufacturing provider-specific flags from the presence of prices.
    return {**context, "data_kind": "1", "source_published_at": published.isoformat(),
            "runners": None, "win_sale": "unknown", "place_sale": "unknown",
            "place_paid_to": None, "quotes": rows}


def capture(race_id: str, db: Path):
    if not re.fullmatch(r"\d{12}", race_id):
        raise ValueError("Race ID must have 12 digits")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Accept-Language": "ja-JP,ja;q=0.9"})
    page_url = f"https://race.netkeiba.com/odds/index.html?race_id={race_id}"
    page = session.get(page_url, timeout=30)
    page.raise_for_status()
    context_observed = datetime.now(JST).isoformat()
    context = parse_context(page.content, race_id)
    if timestamp(context["scheduled_start_at"]) <= timestamp(context_observed):
        raise ValueError("Past/post-time races are not captured as live pre-race quotes")
    time.sleep(1.2)
    api_url = "https://race.netkeiba.com/api/api_get_jra_odds.html"
    response = session.get(api_url, params={
        "pid": "api_get_jra_odds", "input": "UTF-8", "output": "jsonp",
        "race_id": race_id, "type": "all", "action": "init", "sort": "ninki",
        "compress": 0, "callback": "quoteCapture",
    }, headers={"Referer": page_url}, timeout=30)
    response.raise_for_status()  # No retries, rotating identities or premium parameters.
    observed = datetime.now(JST).isoformat()
    if timestamp(observed) >= timestamp(context["scheduled_start_at"]):
        raise ValueError("Response arrived after known post time; do not store as a pre-race observation")
    record = parse_public(response.content, context)
    conn = connect(db)
    try:
        sid = store_normalized(conn, record, response.content, observed_at=observed, mode="live",
                               archive=db.parent / "raw", source="netkeiba_public", expected_race_id=race_id)
        page_hash = hashlib.sha256(page.content).hexdigest()
        archive = db.parent / "raw" / (page_hash + ".html")
        if not archive.exists():
            archive.write_bytes(page.content)
        schedule_id = hashlib.sha256(f"{race_id}:{context_observed}:{page_hash}".encode()).hexdigest()
        with conn:
            conn.execute("INSERT INTO schedules VALUES (?,?,?,?,?,?,?)", (
                schedule_id, race_id, context["scheduled_start_at"], context_observed,
                "netkeiba_public", page_url, page_hash,
            ))
        metadata = {"source": "netkeiba_public", "snapshot_id": sid, "race_id": race_id,
                    "observed_at": observed, "source_published_at": record["source_published_at"],
                    "schedule_observed_at": context_observed, "scheduled_start_at": context["scheduled_start_at"],
                    "horses": len(record["quotes"]), "api_url": response.url,
                    "timezone_assumption": "Provider timestamps interpreted as JST; compared with actual observation.",
                    "cancellation_flags_available": False, "historical_backfill": False,
                    "prediction_performed": False, "premium_parameters_used": False}
        path = db.parent / "last_public_capture.json"
        path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return metadata
    finally:
        conn.close()
        session.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race-id", required=True)
    parser.add_argument("--db", default="data/odds/snapshots.sqlite")
    args = parser.parse_args()
    print(json.dumps(capture(args.race_id, Path(args.db)), ensure_ascii=False, indent=2))
