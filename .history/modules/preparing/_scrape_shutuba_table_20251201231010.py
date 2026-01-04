import time
import re
import random
import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    # 暗黙待機よりも明示待機で主要要素のレンダリングを待つ
    wait = WebDriverWait(driver, 15)
    query = '?race_id=' + race_id
    url = UrlPaths.SHUTUBA_TABLE + query
    df = pd.DataFrame()
    try:
        driver.get(url)
        # JSレンダリング完了のシグナル: 出馬表のリスト要素が現れるのを待つ
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'HorseList')))

        # メインのテーブルの取得
        for tr in driver.find_elements(By.CLASS_NAME, 'HorseList'):
            row = []
            for td in tr.find_elements(By.TAG_NAME, 'td'):
                if td.get_attribute('class') in ['HorseInfo']:
                    href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    row.append(re.findall(r'horse/(\d*)', href)[0])
                elif td.get_attribute('class') in ['Jockey']:
                    href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    row.append(re.findall(r'jockey/result/recent/(\w*)', href)[0])
                elif td.get_attribute('class') in ['Trainer']:
                    href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    row.append(re.findall(r'trainer/result/recent/(\w*)', href)[0])
                row.append(td.text)
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

        # デバッグ: スクレイピング直後の生データを確認
        print(f"スクレイピング完了 - レース{race_id}: {len(df)}頭立て")
        if len(df) > 0:
            print(f"生データの列数: {len(df.columns)}")
            print(f"馬番列（index=1）の値: {df[1].tolist()}")

        # レース結果テーブルと列を揃える
        df = df[[0, 1, 5, 6, 12, 13, 11, 3, 7, 9]]
        df.columns = [Cols.WAKUBAN, Cols.UMABAN, Cols.SEX_AGE, Cols.KINRYO, Cols.TANSHO_ODDS, Cols.POPULARITY, Cols.WEIGHT_AND_DIFF, 'horse_id', 'jockey_id', 'trainer_id']
        df.index = [race_id] * len(df)

        # レース情報の取得
        # レース情報の要素が現れるまで待機
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'RaceList_Item02')))
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
    
    # 馬番クリーンアップ（無効な馬番のレコードを除去）
    def is_valid_umaban(umaban):
        """
        馬番の有効性を検証する拡張関数
        """
        try:
            if pd.isna(umaban):
                return False
            
            # 文字列に変換して前後の空白を除去
            str_umaban = str(umaban).strip()
            
            # 空文字や'None'文字列をチェック
            if str_umaban == '' or str_umaban.lower() == 'none':
                return False
                
            # 取消を示すキーワードをチェック
            cancel_keywords = ['取消', '除外', '--', 'キャンセル', 'cancel']
            if any(keyword in str_umaban for keyword in cancel_keywords):
                return False
            
            # 数値に変換
            num = int(str_umaban)
            
            # 1-18の範囲チェック
            return 1 <= num <= 18
            
        except (ValueError, TypeError):
            return False
    
    # 馬番クリーンアップを適用
    if len(df) > 0:
        print(f"クリーンアップ前の馬番: {df[Cols.UMABAN].tolist()}")
        
        valid_mask = df[Cols.UMABAN].apply(is_valid_umaban)
        invalid_count = (~valid_mask).sum()
        
        if invalid_count > 0:
            print(f"scrape_shutuba_table: {invalid_count}件の不正な馬番レコードを除去しました")
            invalid_umaban = df[~valid_mask][Cols.UMABAN].tolist()
            print(f"除去された馬番: {invalid_umaban}")
            
            # 不正なレコードの詳細をログ出力
            invalid_records = df[~valid_mask]
    for race_id in tqdm(race_id_list):
        query = '?race_id=' + race_id
        url = UrlPaths.SHUTUBA_TABLE + query
        req = Request(url, headers={'User-Agent': random.choice(USER_AGENTS)})
        html = urlopen(req)
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        horse_td_list = soup.find_all("td", attrs={'class': 'HorseInfo'})
            print("すべての馬番が有効です")
    
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
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(req)
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        horse_td_list = soup.find_all("td", attrs={'class': 'HorseInfo'})
        for td in horse_td_list:
            horse_id = re.findall(r'\d+', td.find('a')['href'])[0]
            horse_id_list.append(horse_id)
    return horse_id_list
