# -*- coding: utf-8 -*-
import datetime
import re
import pandas as pd
import time
import os
from tqdm.auto import tqdm
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import random
import requests
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait


from modules.constants import UrlPaths, LocalPaths

# 追加：User-Agent一覧
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
]

def _build_driver(headless: bool = True, user_agent: str | None = None):
    """Selenium WebDriver 構築用ヘルパー"""
    opts = Options()
    if headless:
        # Chrome 109 以降推奨の新ヘッドレス
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,2000")
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    # 追加で安定化オプション
    opts.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=opts)


def scrape_html_race(
    race_id_list: list,
    skip: bool = True,
    headless: bool = True,
    wait_sec: int = 20,
    per_request_sleep: tuple[float, float] = (1.5, 2.0),
):
    """Selenium で JS 実行後のレースページ HTML を取得し保存する。

    引数:
        race_id_list: 取得対象レースIDリスト (文字列)
        skip: True の場合既存ファイルは再取得しない
        headless: ヘッドレスモードで起動するか
        wait_sec: 各ページでの最大待機秒数
        per_request_sleep: (min,max) ランダムスリープ秒レンジ

    戻り値:
        保存 (新規) したファイルパスのリスト
    """
    os.makedirs(LocalPaths.HTML_RACE_DIR, exist_ok=True)
    updated_html_path_list: list[str] = []

    driver = _build_driver(headless=headless, user_agent=random.choice(USER_AGENTS))

    def _tables_populated(d):
        # 代表的な結果テーブル / 払戻テーブルの td が 1 個以上出現するのを待機条件にする
        return d.execute_script(
            """
            const sel = [
              '.result_info .result_table_02 td',
              '.result_info .race_table_01 td',
              '.PayBack_Table td'
            ];
            return sel.some(s => document.querySelectorAll(s).length >= 1);
            """
        ) is True

    try:
        for race_id in tqdm(race_id_list, desc="race HTML (selenium)"):
            filename = os.path.join(LocalPaths.HTML_RACE_DIR, f"{race_id}.bin")
            if skip and os.path.isfile(filename):
                print(f"{race_id} skipped (exists)")
                continue

            url = UrlPaths.RACE_URL + race_id + "/"
            try:
                driver.get(url)
            except Exception as e:
                print(f"{race_id} driver.get error: {e}")
                continue

            # DOM 完全ロード待機
            try:
                WebDriverWait(driver, wait_sec).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except Exception:
                print(f"{race_id} page readyState timeout")

            # テーブル or 払戻テーブル出現待機
            try:
                WebDriverWait(driver, wait_sec).until(_tables_populated)
            except Exception:
                # 多少遅延の余地
                time.sleep(3)

            # 有効ページ確認 (data_intro が無い=存在しない/準備中ページ想定)
            try:
                valid = driver.execute_script(
                    "return !!document.querySelector('div.data_intro');"
                )
            except Exception:
                valid = False

            if not valid:
                print(f"{race_id} invalid (no data_intro)")
                continue

            # HTML 保存
            try:
                html = driver.page_source
                with open(filename, "wb") as f:
                    f.write(html.encode("utf-8", errors="ignore"))
                updated_html_path_list.append(filename)
            except Exception as e:
                print(f"{race_id} save error: {e}")
                continue

            # アクセス間隔 (負荷/ブロック回避)
            sleep_time = random.uniform(*per_request_sleep)
            time.sleep(sleep_time)

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return updated_html_path_list

def scrape_html_horse(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for horse_id in tqdm(horse_id_list):
        # 保存するファイル名
        filename = os.path.join(LocalPaths.HTML_HORSE_DIR, horse_id+'.bin')
        # skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
        if skip and os.path.isfile(filename):
            print('horse_id {} skipped'.format(horse_id))
        else:
            # horse_idからurlを作る
            url = UrlPaths.HORSE_URL + horse_id
            time.sleep(2)
            agent = random.choice(USER_AGENTS)
            req = Request(url, headers={'User-Agent': agent})
            # スクレイピング実行
            html = urlopen(req).read()
            # 保存するファイルパスを指定
            with open(filename, 'wb') as f:
                # 保存
                f.write(html)
            updated_html_path_list.append(filename)
    return updated_html_path_list

def fetch_horse_results_html(horse_id: str, retries: int = 3, backoff: float = 1.0) -> str | None:
    """
    AJAX エンドポイントから競走成績の HTML 断片を取得する。
    成功すると HTML 部分文字列を返し、失敗すると None を返す。
    """
    url = "https://db.netkeiba.com/horse/ajax_horse_results.html"
    params = {
        "input": "UTF-8",
        "output": "json",
        "id": horse_id,
    }
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": f"https://db.netkeiba.com/horse/{horse_id}/",  # 参照元を付けておくと安定しやすい
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            # 成功のステータスコードを確認
            if resp.status_code != 200:
                print(f"[{horse_id}] HTTP {resp.status_code} (attempt {attempt})")
                time.sleep(backoff * attempt)
                continue

            # JSON をパース
            json_data = resp.json()
            if json_data.get("status") != "OK":
                print(f"[{horse_id}] ステータスが OK でない: {json_data.get('status')} (attempt {attempt})")
                time.sleep(backoff * attempt)
                continue

            # 成績 HTML 断片を取得（内部は多くの場合 EUC-JP のテキストだが JSON が UTF-8 で来る）
            fragment = json_data.get("data", "")
            if not fragment:
                print(f"[{horse_id}] data field が空です (attempt {attempt})")
                time.sleep(backoff * attempt)
                continue

            return fragment  # ここを BeautifulSoup に流すなど次の処理へ

        except Exception as e:
            print(f"[{horse_id}] 例外: {e} (attempt {attempt})")
            time.sleep(backoff * attempt)

    print(f"[{horse_id}] 競走成績の取得に失敗しました。")
    return None

def parse_horse_results_deprecated(horse_id: str):
    """
    馬IDから競走成績をDataFrame形式で取得する。（非推奨：parse_html_fragment_to_dataframeを使用してください）
    """
    html_fragment = fetch_horse_results_html(horse_id)
    if html_fragment is None:
        return None  # 取得失敗

    soup = BeautifulSoup(html_fragment, "html.parser")
    
    # 成績テーブルを探す（複数のセレクタを試す）
    table_selectors = [
        "table.db_h_race_results",
        "table[summary*='レース結果']",
        "table[summary*='出走履歴']",
        "table.race_table",
        "table"  # 最後の手段
    ]
    
    table = None
    for selector in table_selectors:
        table = soup.select_one(selector)
        if table:
            break
    
    if not table:
        print(f"[{horse_id}] 成績テーブルが見つかりません。")
        return None

    try:
        # pandas に変換
        df = pd.read_html(str(table))[0]
        df["horse_id"] = horse_id
        return df
    except Exception as e:
        print(f"[{horse_id}] テーブル解析エラー: {e}")
        return None

def scrape_html_horse_results_ajax(horse_id_list: list, skip: bool = True):
    """
    AJAXエンドポイントを使用して馬の競走成績HTMLを取得し、binファイルで保存してDataFrameとして返す。
    skip=Trueにすると、すでにHTMLが存在する場合はスキップされ、Falseにすると上書きされる。
    """
    print('fetching horse results via AJAX and saving HTML as bin files')
    horse_results = {}
    saved_html_files = []
    
    for horse_id in tqdm(horse_id_list):
        # 成績HTMLは horse_results ディレクトリに保存（プロフィールと分離）
        os.makedirs(LocalPaths.HTML_HORSE_RESULTS_DIR, exist_ok=True)
        filename = os.path.join(LocalPaths.HTML_HORSE_RESULTS_DIR, f"{horse_id}.bin")
        
        # skipがTrueで、かつbinファイルがすでに存在する場合は既存ファイルから読み込み
        if skip and os.path.isfile(filename):
            try:
                # ファイルサイズをチェック（空ファイルでないか確認）
                if os.path.getsize(filename) > 0:
                    with open(filename, 'r', encoding='utf-8') as f:
                        html_fragment = f.read()
                    # 既存HTMLからDataFrameを作成
                    df = parse_html_fragment_to_dataframe(horse_id, html_fragment)
                    if df is not None and not df.empty:
                        horse_results[horse_id] = df
                        print(f'horse_id {horse_id} loaded from existing file')
                        continue
                    else:
                        print(f'horse_id {horse_id} existing file has no valid data, re-fetching')
                else:
                    print(f'horse_id {horse_id} existing file is empty, re-fetching')
            except Exception as e:
                print(f'Error loading existing file for {horse_id}: {e}, re-fetching')
                # ファイル読み込みに失敗した場合は新規取得する
        
        try:
            # AJAXからHTMLを取得
            html_fragment = fetch_horse_results_html(horse_id)
            if html_fragment:
                # HTMLをbinファイルとして保存
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(html_fragment)
                saved_html_files.append(filename)
                
                # DataFrameを作成
                df = parse_html_fragment_to_dataframe(horse_id, html_fragment)
                if df is not None and not df.empty:
                    horse_results[horse_id] = df
                    print(f'horse_id {horse_id} HTML saved and processed')
                else:
                    print(f'horse_results empty for {horse_id}')
                
                # ランダムなリクエスト間隔（1.5～2.0秒）
                sleep_time = random.uniform(1.5, 2.0)
                time.sleep(sleep_time)
            else:
                print(f'Failed to fetch HTML for {horse_id}')
        except Exception as e:
            print(f'Error processing {horse_id}: {e}')
            continue

    # pd.DataFrame型にして一つのデータにまとめる
    if not horse_results:
        print('警告: すべての馬の成績が取得できませんでした。空のDataFrameを返します。')
        return pd.DataFrame()
    
    horse_results_df = pd.concat([horse_results[key] for key in horse_results])

    # 列名に半角スペースがあれば除去する
    horse_results_df = horse_results_df.rename(columns=lambda x: x.replace(' ', ''))

    print(f'保存されたHTMLファイル数: {len(saved_html_files)}')
    return horse_results_df

def parse_html_fragment_to_dataframe(horse_id: str, html_fragment: str):
    """
    HTMLフラグメントからDataFrameを作成するヘルパー関数
    """
    if html_fragment is None:
        return None

    soup = BeautifulSoup(html_fragment, "html.parser")
    
    # 成績テーブルを探す（複数のセレクタを試す）
    table_selectors = [
        "table.db_h_race_results",
        "table[summary*='レース結果']",
        "table[summary*='出走履歴']",
        "table.race_table",
        "table"  # 最後の手段
    ]
    
    table = None
    for selector in table_selectors:
        table = soup.select_one(selector)
        if table:
            break
    
    if not table:
        print(f"[{horse_id}] 成績テーブルが見つかりません。")
        return None

    try:
        # pandas に変換
        df = pd.read_html(str(table))[0]
        df["horse_id"] = horse_id
        return df
    except Exception as e:
        print(f"[{horse_id}] テーブル解析エラー: {e}")
        return None

def scrape_html_ped(horse_id_list: list, skip: bool = False):
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
            # ランダムなリクエスト間隔（1.0～2.0秒）
            sleep_time = random.uniform(1.0, 2.0)
            time.sleep(sleep_time)
            agent = random.choice(USER_AGENTS)
            req = Request(url, headers={'User-Agent': agent})
            # スクレイピング実行
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
    ### スクレイピング実行（静的HTML） ###
    print('scraping (static horse page)')
    updated_html_path_list = scrape_html_horse(horse_id_list, skip)

    # パスから安全にhorse_idを抽出
    horse_id_extracted = []
    for html_path in updated_html_path_list:
        base = os.path.basename(html_path)
        m = re.match(r'(\d+)\.bin$', base)
        if m:
            horse_id_extracted.append(m.group(1))
        else:
            print(f"WARNING: could not parse horse_id from path {html_path}")
    # DataFrameにしておく
    horse_id_df = pd.DataFrame({'horse_id': horse_id_extracted})

    ### 追加: AJAXで競走成績も取得・保存 ###
    print('scraping (AJAX horse results)')
    from tqdm.auto import tqdm
    import random
    import time
    import requests
    # 既存の競走成績ファイルがなければ作成
    for horse_id in tqdm(horse_id_list, desc='AJAX horse results'):
        # 成績ファイルは horse_results ディレクトリへ
        os.makedirs(LocalPaths.HTML_HORSE_RESULTS_DIR, exist_ok=True)
        results_filename = os.path.join(LocalPaths.HTML_HORSE_RESULTS_DIR, f"{horse_id}.bin")
        # skipがTrueで既にファイルがあればスキップ
        if skip and os.path.isfile(results_filename):
            print(f'horse_id {horse_id} skipped (already exists)')
            continue
        # AJAX取得
        url = "https://db.netkeiba.com/horse/ajax_horse_results.html"
        params = {"input": "UTF-8", "output": "json", "id": horse_id}
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": f"https://db.netkeiba.com/horse/{horse_id}/",
        }
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                # JSONレスポンスからHTMLフラグメントを抽出して保存
                json_data = resp.json()
                if json_data.get("status") == "OK":
                    html_fragment = json_data.get("data", "")
                    with open(results_filename, 'w', encoding='utf-8') as f:
                        f.write(html_fragment)
                    print(f'horse_id {horse_id} HTML saved via AJAX')
                else:
                    print(f"[AJAX] {horse_id} status not OK: {json_data.get('status')}")
            else:
                print(f"[AJAX] {horse_id} HTTP {resp.status_code}")
        except Exception as e:
            print(f"[AJAX] {horse_id} error: {e}")
        time.sleep(random.uniform(1.0, 2.0))

    ### 取得日マスタの更新 ###
    print('updating master')
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not os.path.isfile(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH):
        pd.DataFrame(columns=['horse_id', 'updated_at']).to_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, index=None)
    master = pd.read_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, dtype=object)
    new_master = master.merge(horse_id_df, on='horse_id', how='outer')
    new_master.loc[new_master['horse_id'].isin(horse_id_extracted), 'updated_at'] = now
    new_master[['horse_id', 'updated_at']].to_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, index=None)
    return updated_html_path_list
#TODO: scrape_html_horse_with_updated_atのテスト

def fetch_pedigree_data_optimized(horse_id):
    """最適化された血統データ取得"""
    
    # 調査結果で最も確実だったエンドポイント
    url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
    
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    try:
        time.sleep(0.8)  # 適度なレート制限
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            content = response.content.decode('euc-jp', errors='ignore')
            return content
        else:
            return None
            
    except Exception as e:
        return None

def parse_pedigree_comprehensive(html_content):
    """血統HTMLから包括的にデータを抽出"""
    
    if not html_content:
        return {}
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        pedigree_data = {}
        
        # 血統テーブルを検索
        blood_table = soup.find('table', class_='blood_table')
        
        if blood_table:
            # 血統表の構造を解析
            rows = blood_table.find_all('tr')
            
            for i, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                
                # セルの内容を抽出
                cell_data = []
                for cell in cells:
                    # リンク情報も保持
                    links = cell.find_all('a', href=True)
                    cell_info = {
                        'text': cell.get_text(strip=True),
                        'links': []
                    }
                    
                    for link in links:
                        href = link.get('href', '')
                        if '/horse/' in href:
                            # 馬IDを抽出
                            horse_id_match = href.split('/horse/')[-1].split('/')[0]
                            if horse_id_match.isdigit():
                                cell_info['links'].append({
                                    'name': link.get_text(strip=True),
                                    'horse_id': horse_id_match,
                                    'url': href
                                })
                    
                    cell_data.append(cell_info)
                
                if cell_data:
                    pedigree_data[f'row_{i}'] = cell_data
        
        # 基本情報も抽出
        horse_title = soup.find('h1')
        if horse_title:
            pedigree_data['horse_name'] = horse_title.get_text(strip=True)
        
        # 血統情報のサマリー作成
        summary = {
            'total_rows': len([k for k in pedigree_data.keys() if k.startswith('row_')]),
            'total_horses': 0,
            'generation_depth': 0
        }
        
        # 関連馬の数をカウント
        for key, value in pedigree_data.items():
            if key.startswith('row_'):
                for cell in value:
                    summary['total_horses'] += len(cell['links'])
        
        pedigree_data['summary'] = summary
        
        return pedigree_data
        
    except Exception as e:
        print(f"⚠️  血統解析エラー: {str(e)}")
        return {}

def scrape_pedigree_batch_optimized(horse_ids, max_horses=None, show_progress=True, skip_existing=True):
    """最適化された血統データ一括取得"""
    
    print(f"📊 対象馬数: {len(horse_ids)} 頭")
    
    # 既存の血統データファイルをチェック
    if skip_existing:
        print("🔍 既存の血統データファイルをチェック中...")
        existing_horse_ids = set()
        missing_horse_ids = []
        
        for horse_id in horse_ids:
            ped_file_path = os.path.join(LocalPaths.HTML_PED_DIR, f"{horse_id}.bin")
            if os.path.exists(ped_file_path):
                existing_horse_ids.add(horse_id)
            else:
                missing_horse_ids.append(horse_id)
        
        print(f"✅ 既存血統ファイル: {len(existing_horse_ids)} 頭")
        print(f"❌ 不足している血統ファイル: {len(missing_horse_ids)} 頭")
        
        # 不足しているファイルのみを対象とする
        horse_ids = missing_horse_ids
        
        if len(horse_ids) == 0:
            print("🎉 すべての血統データが既に存在します！")
            return {}
    
    if max_horses:
        horse_ids = horse_ids[:max_horses]
        print(f"🎯 実行対象: {len(horse_ids)} 頭（制限適用）")
    
    all_pedigree_data = {}
    success_count = 0
    error_count = 0
    
    # tqdmでプログレスバーを表示
    progress_bar = tqdm(horse_ids, desc="🧬 血統データ取得", unit="頭") if show_progress else horse_ids

    for i, horse_id in enumerate(progress_bar, 1):
        # 血統データ取得
        html_content = fetch_pedigree_data_optimized(horse_id)
        
        if html_content:
            # HTMLファイルとして保存（既存のped保存形式と統一）
            ped_file_path = os.path.join(LocalPaths.HTML_PED_DIR, f"{horse_id}.bin")
            try:
                with open(ped_file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
            except Exception as e:
                print(f"⚠️ ファイル保存エラー {horse_id}: {e}")
            
            pedigree_data = parse_pedigree_comprehensive(html_content)
            
            if pedigree_data and 'summary' in pedigree_data:
                all_pedigree_data[horse_id] = pedigree_data
                success_count += 1
                
                # tqdmの説明を動的に更新
                if show_progress:
                    progress_bar.set_postfix({
                        '成功': success_count,
                        'エラー': error_count,
                        '成功率': f"{success_count/(success_count+error_count)*100:.1f}%" if (success_count+error_count) > 0 else "0%"
                    })
                
                if show_progress and i % 100 == 0:
                    print(f"\n  ✅ 成功: {success_count} | ❌ エラー: {error_count}")
            else:
                error_count += 1
        else:
            error_count += 1
        
        # 過度なアクセスを避ける
        sleep_time = random.uniform(1.0, 2.0)
        time.sleep(sleep_time)
    
    print(f"\n\n=== 🧬 血統データ取得完了 ===")
    print(f"✅ 成功: {success_count} 頭")
    print(f"❌ エラー: {error_count} 頭")
    if success_count + error_count > 0:
        print(f"📊 成功率: {success_count/(success_count+error_count)*100:.1f}%")
    
    return all_pedigree_data

def normalize_numeric_id_from_path(path_or_id: str) -> str:
    """ファイル名/ID文字列から数字だけ抜き出して返す"""
    s = os.path.basename(str(path_or_id))
    m = re.search(r'(\d+)', s)
    return m.group(1) if m else s

def scrape_jockey_html(jockey_id_list: list, skip: bool = True):
    """
    騎手ページのHTMLをスクレイピング（ID正規化対応版）
    """
    print('scraping jockey HTML pages (improved)')
    
    # ディレクトリ確認・作成
    jockey_html_dir = os.path.join(LocalPaths.HTML_DIR, 'jockey')
    os.makedirs(jockey_html_dir, exist_ok=True)
    
    updated_html_path_list = []
    
    for jockey_id in tqdm(jockey_id_list, desc="騎手HTMLスクレイピング"):
        # IDを正規化
        jid = normalize_numeric_id_from_path(jockey_id)
        
        # 保存ファイルパス
        file_path = os.path.join(jockey_html_dir, f"jockey_{jid}.bin")
        
        # skipチェック
        if skip and os.path.exists(file_path):
            updated_html_path_list.append(file_path)
            continue
        
        try:
            # 騎手トップページURL
            url = f"https://db.netkeiba.com/jockey/{jid}/"
            
            # ランダムなUser-Agent
            agent = random.choice(USER_AGENTS)
            headers = {
                'User-Agent': agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
            }
            
            # リクエスト実行
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # pickleで保存（既存形式に合わせる）
            import pickle
            with open(file_path, 'wb') as f:
                pickle.dump(response.text, f)
            
            updated_html_path_list.append(file_path)
            
            # 適度な間隔
            sleep_time = random.uniform(1.5, 2.5)
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"❌ 騎手HTML取得エラー {jockey_id}: {e}")
            continue
    
    return updated_html_path_list

def scrape_jockey_result_html(jockey_id_list: list, skip: bool = True, wait_time: float = 1.0):
    """
    騎手の結果ページ (/jockey/result/{id}/) を保存し、パスを返す
    """
    print('scraping jockey result pages')
    
    # ディレクトリ確認・作成
    jockey_html_dir = os.path.join(LocalPaths.HTML_DIR, 'jockey')
    os.makedirs(jockey_html_dir, exist_ok=True)
    
    paths = []

    for raw_id in tqdm(jockey_id_list, desc="騎手RESULTページスクレイピング"):
        # IDを正規化
        jid = normalize_numeric_id_from_path(raw_id)
        
        # 保存ファイルパス
        file_path = os.path.join(jockey_html_dir, f"jockey_result_{jid}.bin")
        
        # skipチェック
        if skip and os.path.exists(file_path):
            paths.append(file_path)
            continue
        
        try:
            # 騎手結果ページURL
            url = f"https://db.netkeiba.com/jockey/result/{jid}/"
            
            # ヘッダー設定
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Referer': f"https://db.netkeiba.com/jockey/{jid}/",
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            }
            
            # リクエスト実行
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # pickleで保存（既存形式に合わせる）
            import pickle
            with open(file_path, 'wb') as f:
                pickle.dump(response.text, f)
            
            paths.append(file_path)
            
            # 間隔調整
            time.sleep(wait_time + random.uniform(0, 0.5))
            
        except Exception as e:
            print(f"❌ 騎手結果ページ取得エラー {raw_id}: {e}")
            continue
    
    return paths