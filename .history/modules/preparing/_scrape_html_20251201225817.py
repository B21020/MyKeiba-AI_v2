import datetime
import re
import pandas as pd
import time
import os
import random
import json
import requests
from tqdm.auto import tqdm
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

from modules.constants import UrlPaths, LocalPaths

def scrape_html_race(race_id_list: list, skip: bool = True):
    """
    netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for race_id in tqdm(race_id_list):
        # 保存するファイル名
        filename = os.path.join(LocalPaths.HTML_RACE_DIR, race_id+'.bin')
        # skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
        if skip and os.path.isfile(filename):
            print('race_id {} skipped'.format(race_id))
        else:
            # race_idからurlを作る
            url = UrlPaths.RACE_URL + race_id
            # 相手サーバーに負担をかけないように1秒待機する
            time.sleep(1)
            # スクレイピング実行
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            html = urlopen(req).read()
            # htmlをsoupオブジェクトに変換
            soup = BeautifulSoup(html, "lxml")
            # レースデータが存在するかどうかをチェック
            data_intro_exists = bool(soup.find("div", attrs={"class": "data_intro"}))

            if not data_intro_exists:
                print('race_id {} skipped. This page is not valid.'.format(race_id))
                continue
            # 保存するファイルパスを指定
            with open(filename, 'wb') as f:
                # 保存
                f.write(html)
            updated_html_path_list.append(filename)
    return updated_html_path_list

def _build_session():
    """requests.Sessionを構築してUser-Agentなどのヘッダーを設定"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0 Safari/537.36"),
        "Accept-Language": "ja,en;q=0.9",
    })
    return s

def _decode_best(bytes_body, enc_candidates=("euc-jp","cp932","utf-8")):
    """バイトデータから適切なエンコーディングで文字列をデコード"""
    for enc in enc_candidates:
        try:
            return bytes_body.decode(enc), enc
        except Exception:
            pass
    # 最後はignoreで
    return bytes_body.decode(enc_candidates[-1], errors="ignore"), enc_candidates[-1]

def _merge_results_into_base_html(base_html_text: str, results_fragment_html: str) -> str:
    """
    base_html_text（EUC-JPのことが多い）に、results_fragment_html（UTF-8想定）を挿入。
    #horse_results_box があれば置換、無ければ末尾にセクションを追加。
    返り値はUTF-8の文字列（保存時に .encode('utf-8') する）。
    """
    soup = BeautifulSoup(base_html_text, "lxml")

    target = soup.select_one("#horse_results_box")
    frag_soup = BeautifulSoup(results_fragment_html, "lxml")
    
    # lxmlパーサーは自動的にhtml/bodyタグを追加することがあるので、
    # body内のコンテンツを取得する
    if frag_soup.body:
        fragment_content = frag_soup.body.contents
    else:
        fragment_content = frag_soup.contents

    if target:
        # 既存コンテナの中身を差し替え
        target.clear()
        for child in fragment_content:
            target.append(child)
    else:
        # 念のため末尾に追加（構造変更時の保険）
        wrap = soup.new_tag("div", id="horse_results_box")
        for child in fragment_content:
            wrap.append(child)
        if soup.body:
            soup.body.append(wrap)
        else:
            soup.append(wrap)

    # meta charset をUTF-8に差し替え（後工程が扱いやすいように）
    head = soup.head or soup.new_tag("head")
    soup.html.insert(0, head) if not soup.head else None
    # 既存のcontent-typeを削除
    for m in soup.find_all("meta", attrs={"http-equiv": re.compile("^content-type$", re.I)}):
        m.decompose()
    meta = soup.new_tag("meta")
    meta.attrs["charset"] = "utf-8"
    head.insert(0, meta)

    return str(soup)

def scrape_html_horse(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをAJAX直接叩きでスクレイピングしてdata/html/horseに保存する関数。
    1) 本体HTML（馬ページ）を取得（EUC-JPなど）
    2) AJAX（/horse/ajax_horse_results.html?id=...）で過去成績のHTML断片を取得
    3) 断片を本体に挿入してUTF-8で .bin 保存
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    session = _build_session()
    
    for horse_id in tqdm(horse_id_list):
        # 保存するファイル名
        filename = os.path.join(LocalPaths.HTML_HORSE_DIR, horse_id+'.bin')
        # skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
        if skip and os.path.isfile(filename):
            print('horse_id {} skipped'.format(horse_id))
        else:
            try:
                # --- 1) 本体HTML ---
                base_url = UrlPaths.HORSE_URL + horse_id
                r = session.get(base_url, timeout=20)
                r.raise_for_status()
                
                base_text, used_enc = _decode_best(r.content)
                
                # --- 2) AJAX（過去成績） ---
                ajax_url = "https://db.netkeiba.com/horse/ajax_horse_results.html"
                params = {"id": horse_id, "input": "UTF-8", "output": "json"}
                headers = {"Referer": base_url}
                frag_html = ""
                success = False
                
                max_retries = 3
                backoff = 1.5
                
                for attempt in range(1, max_retries + 1):
                    try:
                        rr = session.get(ajax_url, params=params, headers=headers, timeout=20)
                        rr.raise_for_status()
                        js = rr.json()
                        if js.get("status") == "OK":
                            frag_html = js.get("data", "")
                            success = True
                            break
                    except Exception as e:
                        if attempt == max_retries:
                            print(f"[ERROR] ajax GET {horse_id} attempt={attempt}: {e}")
                        time.sleep(backoff ** attempt)
                
                if not success or not frag_html:
                    # 成績が無い or 取得失敗 → 本体のみUTF-8で保存
                    print(f"[WARN] results fragment missing for horse_id {horse_id}; saving base only.")
                    merged = _merge_results_into_base_html(base_text, "")  # 空でもコンテナは整える
                else:
                    merged = _merge_results_into_base_html(base_text, frag_html)
                
                # --- 3) 保存（UTF-8バイト） ---
                with open(filename, "wb") as f:
                    f.write(merged.encode("utf-8", errors="ignore"))
                
                updated_html_path_list.append(filename)
                
            except Exception as e:
                print('horse_id {} error: {}'.format(horse_id, str(e)))
                continue
                
            # 相手サーバーに負担をかけないように待機
            time.sleep(2.0 + random.random())
            
    return updated_html_path_list

def scrape_html_ped(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for horse_id in tqdm(horse_id_list):
        # 保存するファイル名
        filename = os.path.join(LocalPaths.HTML_PED_DIR, horse_id+'.bin')
        # skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
        if skip and os.path.isfile(filename):
            print('horse_id {} skipped'.format(horse_id))
        else:
            # horse_idからurlを作る
            url = UrlPaths.PED_URL + horse_id
            # 相手サーバーに負担をかけないように1秒待機する
            time.sleep(1)
            # スクレイピング実行
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            html = urlopen(req).read()
            # 保存するファイルパスを指定
            with open(filename, 'wb') as f:
                # 保存
                f.write(html)
            updated_html_path_list.append(filename)
    return updated_html_path_list

def scrape_html_horse_with_master(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    また、horse_idごとに、最後にスクレイピングした日付を記録し、data/master/horse_results_updated_at.csvに保存する。
    """
    ### スクレイピング実行 ###
    print('scraping')
    updated_html_path_list = scrape_html_horse(horse_id_list, skip)
    # パスから正規表現でhorse_id_listを取得
    horse_id_list = [
        re.findall('horse\W(\d+).bin', html_path)[0] for html_path in updated_html_path_list
        ]
    # DataFrameにしておく
    horse_id_df = pd.DataFrame({'horse_id': horse_id_list})
    
    ### 取得日マスタの更新 ###
    print('updating master')
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # 現在日時を取得
    # ファイルが存在しない場合は、作成する
    if not os.path.isfile(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH):
        pd.DataFrame(columns=['horse_id', 'updated_at']).to_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, index=None)
    # マスタを読み込み
    master = pd.read_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, dtype=object)
    # horse_id列に新しい馬を追加
    new_master = master.merge(horse_id_df, on='horse_id', how='outer')
    # マスタ更新
    new_master.loc[new_master['horse_id'].isin(horse_id_list), 'updated_at'] = now
    # 列が入れ替わってしまう場合があるので、修正しつつ保存
    new_master[['horse_id', 'updated_at']].to_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, index=None)
    return updated_html_path_list
#TODO: scrape_html_horse_with_updated_atのテスト