import random
import time
from typing import Optional

import requests


_DEFAULT_UA_CANDIDATES = [
    # 固定UAだけだと弾かれるケースがあるので軽くローテーション
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
]


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    return s


def fetch_bytes(
    session: requests.Session,
    url: str,
    *,
    referer: Optional[str] = None,
    timeout: int = 20,
    max_retries: int = 3,
    backoff_base: float = 1.6,
    sleep_seconds: float = 1.0,
) -> bytes:
    """netkeiba向けに、ヘッダ付きGETでHTML(bytes)を取得。

    - 400/429/5xx等が出やすいので軽いリトライあり
    - サーバ負荷軽減のため、各リクエスト間にsleep
    """
    headers = {"User-Agent": random.choice(_DEFAULT_UA_CANDIDATES)}
    if referer:
        headers["Referer"] = referer

    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(sleep_seconds)
            resp = session.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_exc = e
            if attempt >= max_retries:
                raise
            time.sleep(backoff_base**attempt)

    # 到達しないが型上
    assert last_exc is not None
    raise last_exc
