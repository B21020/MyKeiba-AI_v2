import time
import re
import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import random
from selenium.webdriver.common.by import By
from modules.constants import UrlPaths
from modules.constants import ResultsCols as Cols
from modules.constants import Master
from tqdm.auto import tqdm
from ._prepare_chrome_driver import prepare_chrome_driver

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

def scrape_shutuba_table(race_id: str, date: str, file_path: str):
    """
    当日の出馬表をスクレイピング。
    dateはyyyy/mm/ddの形式。
    """
    driver = prepare_chrome_driver()
    # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機を延長
    driver.implicitly_wait(10)
    query = '?race_id=' + race_id
    url = UrlPaths.SHUTUBA_TABLE + query
    df = pd.DataFrame()
    try:
        driver.get(url)
        
        # ページロード完了を待機
        time.sleep(10)
        
        # 出馬表テーブルを取得（修正: 現在のWebページ構造に対応）
        shutuba_table = None
        table_candidates = ['Shutuba_Table', 'ShutubaTable', 'RaceTable01']
        
        for candidate in table_candidates:
            try:
                shutuba_table = driver.find_element(By.CLASS_NAME, candidate)
                print(f"✅ テーブル発見: {candidate}")
                break
            except:
                continue
        
        if not shutuba_table:
            print("❌ 出馬表テーブルが見つかりません")
            return

        # データ行の読み込み完了を待機
        max_wait_time = 30
        start_time = time.time()
        data_loaded = False
        
        while time.time() - start_time < max_wait_time:
            try:
                # テーブル内のTD要素をチェック
                all_tds = shutuba_table.find_elements(By.TAG_NAME, 'td')
                if len(all_tds) > 10:  # 十分なデータがある
                    data_loaded = True
                    break
            except:
                pass
            time.sleep(1)
        
        if not data_loaded:
            print("⚠️ データ読み込みタイムアウト")
        
        # 追加待機
        time.sleep(5)

        # メインのテーブルの取得（修正: Shutuba_Table内のtr要素を処理）
        all_rows = shutuba_table.find_elements(By.TAG_NAME, 'tr')
        
        for tr in all_rows:
            row_class = tr.get_attribute('class')
            tds = tr.find_elements(By.TAG_NAME, 'td')
            ths = tr.find_elements(By.TAG_NAME, 'th')
            
            # ヘッダー行をスキップ（TH要素があるまたはHeaderクラス）
            if len(ths) > 0 or 'Header' in row_class:
                continue
            
            # データ行を処理（TD要素が十分にある）
            if len(tds) >= 8:
                row = []
                for td in tds:
                    if td.get_attribute('class') in ['HorseInfo']:
                        try:
                            href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                            row.append(re.findall(r'horse/(\d*)', href)[0])
                        except:
                            row.append('')
                    elif td.get_attribute('class') in ['Jockey']:
                        try:
                            href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                            row.append(re.findall(r'jockey/result/recent/(\w*)', href)[0])
                        except:
                            row.append('')
                    elif td.get_attribute('class') in ['Trainer']:
                        try:
                            href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                            row.append(re.findall(r'trainer/result/recent/(\w*)', href)[0])
                        except:
                            row.append('')
                    row.append(td.text)
                
                if len(row) > 0:  # 有効なデータがある場合のみ追加
                    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

        # データが取得できなかった場合の処理
        if df.empty:
            print("❌ 出馬表データが取得できませんでした")
            return
        
        print(f"✅ {len(df)}行のデータを取得")
        print(f"📊 データ列数: {df.shape[1]}")
        
        # データ構造の確認とカラム設定（安全なアプローチ）
        try:
            # 期待される列数をチェック
            expected_cols = [Cols.WAKUBAN, Cols.UMABAN, Cols.SEX_AGE, Cols.KINRYO, 
                           Cols.TANSHO_ODDS, Cols.POPULARITY, Cols.WEIGHT_AND_DIFF, 
                           'horse_id', 'jockey_id', 'trainer_id']
            
            if df.shape[1] >= len(expected_cols):
                # 最初の10列を使用（従来の形式に合わせる）
                # 列のインデックスを動的に調整
                available_cols = list(range(df.shape[1]))
                
                # 安全な列選択（利用可能な列数に基づいて調整）
                if len(available_cols) >= 10:
                    # 元のロジック: [0, 1, 5, 6, 12, 13, 11, 3, 7, 9]
                    # データ構造に応じて調整
                    col_mapping = [0, 1, 5, 6, min(12, len(available_cols)-1), 
                                 min(13, len(available_cols)-1), min(11, len(available_cols)-1), 
                                 3, 7, 9]
                    col_mapping = [c for c in col_mapping if c < len(available_cols)]
                    
                    if len(col_mapping) == len(expected_cols):
                        df = df[col_mapping]
                        df.columns = expected_cols
                    else:
                        print(f"⚠️ 列数不一致: 期待{len(expected_cols)}, 実際{len(col_mapping)}")
                        # フォールバック: 最初のN列を使用
                        n_cols = min(len(expected_cols), df.shape[1])
                        df = df.iloc[:, :n_cols]
                        df.columns = expected_cols[:n_cols]
                else:
                    print(f"⚠️ 列数不足: 期待10列以上, 実際{len(available_cols)}列")
                    # 最低限の列名を設定
                    n_cols = min(len(expected_cols), df.shape[1])
                    df.columns = expected_cols[:n_cols]
            else:
                print(f"⚠️ 列数不足: 期待{len(expected_cols)}列, 実際{df.shape[1]}列")
                # 動的に列名を設定
                df.columns = [f'col_{i}' for i in range(df.shape[1])]
            
            df.index = [race_id] * len(df)
            
        except Exception as e:
            print(f"⚠️ カラム設定エラー: {e}")
            # 最低限の処理を続行

        # レース情報の取得
        texts = driver.find_element(By.CLASS_NAME, 'RaceList_Item02').text
        texts = re.findall(r'\w+', texts)
        # 障害レースフラグを初期化
        hurdle_race_flg = False
        for text in texts:
            if '0m' in text:
                # 20211212：[0]→[-1]に修正
                df['course_len'] = [int(re.findall(r'\d+', text)[-1])] * len(df)
            if text in Master.WEATHER_LIST:
                df["weather"] = [text] * len(df)
            if text in Master.GROUND_STATE_LIST:
                df["ground_state"] = [text] * len(df)
            if '稍' in text:
                df["ground_state"] = [Master.GROUND_STATE_LIST[1]] * len(df)
            if '不' in text:
                df["ground_state"] = [Master.GROUND_STATE_LIST[3]] * len(df)
            if '芝' in text:
                df['race_type'] = [list(Master.RACE_TYPE_DICT.values())[0]] * len(df)
            if 'ダ' in text:
                df['race_type'] = [list(Master.RACE_TYPE_DICT.values())[1]] * len(df)
            if '障' in text:
                df['race_type'] = [list(Master.RACE_TYPE_DICT.values())[2]] * len(df)
                hurdle_race_flg = True
            if "右" in text:
                df["around"] = [Master.AROUND_LIST[0]] * len(df)
            if "左" in text:
                df["around"] = [Master.AROUND_LIST[1]] * len(df)
            if "直線" in text:
                df["around"] = [Master.AROUND_LIST[2]] * len(df)
            if "新馬" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[0]] * len(df)
            if "未勝利" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[1]] * len(df)
            if "１勝クラス" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[2]] * len(df)
            if "２勝クラス" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[3]] * len(df)
            if "３勝クラス" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[4]] * len(df)
            if "オープン" in text:
                df["race_class"] = [Master.RACE_CLASS_LIST[5]] * len(df)

        # グレードレース情報の取得
        if len(driver.find_elements(By.CLASS_NAME, 'Icon_GradeType3')) > 0:
            df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
        elif len(driver.find_elements(By.CLASS_NAME, 'Icon_GradeType2')) > 0:
            df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
        elif len(driver.find_elements(By.CLASS_NAME, 'Icon_GradeType1')) > 0:
            df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

        # 障害レースの場合
        if hurdle_race_flg:
            df["around"] = [Master.AROUND_LIST[3]] * len(df)
            df["race_class"] = [Master.RACE_CLASS_LIST[9]] * len(df)

        df['date'] = [date] * len(df)
    except Exception as e:
        print(e)
    finally:
        driver.close()
        driver.quit()

    # 取消された出走馬を削除
    df = df[df[Cols.WEIGHT_AND_DIFF] != '--']
    df.to_pickle(file_path)

def scrape_horse_id_list(race_id_list: list) -> list:
    """
    当日出走するhorse_id一覧を取得
    """
    print('sraping horse_id_list')
    horse_id_list = []
    for race_id in tqdm(race_id_list):
        query = '?race_id=' + race_id
        url = UrlPaths.SHUTUBA_TABLE + query
        agent = random.choice(USER_AGENTS)
        req = Request(url, headers={'User-Agent': agent})
        html = urlopen(req)
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        horse_td_list = soup.find_all("td", attrs={'class': 'HorseInfo'})
        for td in horse_td_list:
            horse_id = re.findall(r'\d+', td.find('a')['href'])[0]
            horse_id_list.append(horse_id)
    return horse_id_list
