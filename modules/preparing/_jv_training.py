"""JV-Data の調教レコード（HC：坂路調教、WC：ウッドチップ調教）を読む。

位置は SDK 5.0.0 の JVData_Struct.py（JV_HC_HANRO・JV_WC_WOOD の SetDataB、1始まり）に合わせる。
タイムは 0.1 秒単位（合計4桁、ラップ3桁）。0 や空白は計測なし（欠損）。
"""
from __future__ import annotations

HC_LEN = 60    # CRLF を含む
WC_LEN = 105


def _sec(s: str) -> float | None:
    s = s.strip()
    if not s.isdigit() or int(s) == 0:
        return None
    return int(s) / 10


def _common(t: str) -> dict:
    return {'record': t[0:2], 'data_kubun': t[2], 'make_date': t[3:11], 'tresen': t[11],
            'date': f'{t[12:16]}-{t[16:18]}-{t[18:20]}', 'time': t[20:24], 'horse_id': t[24:34]}


def parse_hc(raw: bytes) -> dict:
    t = raw.decode('ascii')
    if t[:2] != 'HC' or len(raw) < HC_LEN - 2:
        raise ValueError('not an HC record')
    r = _common(t)
    r.update(h4=_sec(t[34:38]), l4=_sec(t[38:41]), h3=_sec(t[41:45]), l3=_sec(t[45:48]),
             h2=_sec(t[48:52]), l2=_sec(t[52:55]), l1=_sec(t[55:58]))
    return r


def parse_wc(raw: bytes) -> dict:
    t = raw.decode('ascii')
    if t[:2] != 'WC' or len(raw) < WC_LEN - 2:
        raise ValueError('not a WC record')
    r = _common(t)
    r.update(course=t[34], around=t[35])
    pos = 37
    for n in range(10, 1, -1):  # 10F〜2F：合計4桁＋ラップ3桁
        r[f'h{n}'] = _sec(t[pos:pos + 4])
        r[f'l{n}'] = _sec(t[pos + 4:pos + 7])
        pos += 7
    r['l1'] = _sec(t[pos:pos + 3])
    return r
