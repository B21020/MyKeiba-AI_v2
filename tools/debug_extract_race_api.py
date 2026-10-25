import re
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO


def fetch_result_html(race_id: str) -> str:
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    r = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
        },
        timeout=20,
    )
    r.raise_for_status()
    # netkeibaはEUC-JPが混ざる
    return r.content.decode("euc-jp", errors="ignore")


def main() -> None:
    race_id = "202606010501"
    html = fetch_result_html(race_id)

    soup = BeautifulSoup(html, "lxml")
    candidates = []
    for tag in soup.find_all(True):
        tid = (tag.get("id") or "")
        tcls = " ".join(tag.get("class") or [])
        key = f"{tid} {tcls}".lower()
        if any(k in key for k in ("pay", "return", "hara", "refund")):
            # 大量になるので短く
            candidates.append((tag.name, tid, tcls))

    print("payout placeholder candidates:", len(candidates))
    if candidates:
        print("candidates sample:", candidates[:30])

    print("\n--- pandas.read_html test ---")
    try:
        dfs = pd.read_html(StringIO(html))
        print("tables:", len(dfs))
        print("shapes sample:", [df.shape for df in dfs[:10]])
        # payoutっぽい表だけ抽出して先頭を表示
        payout_keys = ("単勝", "複勝", "枠連", "馬連", "ワイド", "馬単", "三連複", "三連単", "WIN5")
        payout_dfs = []
        for dfi in dfs:
            if dfi.empty or dfi.shape[1] < 2:
                continue
            s0 = dfi[dfi.columns[0]].astype(str)
            if s0.str.contains("|".join(payout_keys), regex=True).any():
                payout_dfs.append(dfi)
        print("payout tables found by pandas:", len(payout_dfs))
        if payout_dfs:
            print(payout_dfs[0].head())
    except Exception as e:
        print("read_html failed:", type(e).__name__, e)

    iframes = re.findall(r"<iframe[^>]+src=\"([^\"]+)\"", html)
    scripts = re.findall(r"<script[^>]+src=\"([^\"]+)\"", html)

    endpoints = sorted(set(re.findall(r"/api/api_get_[^\"']+?\\.html", html)))
    api_get_tokens = sorted(set(re.findall(r"api_get_[a-zA-Z0-9_]+", html)))
    print("race_id:", race_id)
    print("iframes:", len(iframes))
    if iframes:
        print("iframe sample:", iframes[:10])
    print("external scripts:", len(scripts))
    if scripts:
        print("script sample:", scripts[:10])
    print("found endpoints:", len(endpoints))
    for ep in endpoints:
        print("-", ep)

    print("found api_get tokens:", len(api_get_tokens))
    if api_get_tokens:
        print("tokens sample:", api_get_tokens[:50])

    if not endpoints:
        return

    print("\n--- probing endpoints ---")
    base = "https://race.netkeiba.com"
    for ep in endpoints:
        full = base + ep
        tried = []
        for params in (
            {"id": race_id},
            {"race_id": race_id},
            {"id": race_id, "input": "UTF-8", "output": "json"},
            {"race_id": race_id, "input": "UTF-8", "output": "json"},
        ):
            qs = urlencode(params)
            url = full + "?" + qs
            try:
                r = requests.get(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
                        "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
                        "Referer": f"https://race.netkeiba.com/race/result.html?race_id={race_id}",
                    },
                    timeout=20,
                )
                content = r.content
                txt = content.decode("utf-8", errors="ignore")
                # 目視に便利な軽い特徴量
                flags = {
                    "table": "<table" in txt,
                    "payout": ("払戻" in txt) or ("単勝" in txt) or ("複勝" in txt),
                    "json": txt.strip().startswith("{") or txt.strip().startswith("["),
                }
                tried.append((r.status_code, len(content), flags, url))
            except Exception as e:
                tried.append(("ERR", 0, {"err": str(e)}, url))

        print("\nendpoint:", ep)
        for item in tried:
            print(" ", item[0], item[1], item[2], item[3])


if __name__ == "__main__":
    main()
