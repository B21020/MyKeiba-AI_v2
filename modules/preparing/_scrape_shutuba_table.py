import time
import re
from tkinter.tix import NoteBook
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from tqdm.notebook import tqdm

from modules.constants import UrlPaths
from modules.constants import ResultsCols as Cols

def scrape_shutuba_table(race_id: str, date: str, file_path: str):
    """
    当日の出馬表をスクレイピング。
    dateはyyyy/mm/ddの形式。
    """
    options = Options()
    options.add_argument('--headless') #ヘッドレスモード（ブラウザが立ち上がらない）
    driver = webdriver.Chrome(options=options)
    #画面サイズをなるべく小さくし、余計な画像などを読み込まないようにする
    driver.set_window_size(8, 8)
    query = '?race_id=' + race_id
    url = UrlPaths.SHUTUBA_TABLE + query
    df = pd.DataFrame()
    try:
        driver.get(url)
        time.sleep(1)
        
        #メインのテーブルの取得
        for tr in driver.find_elements(By.CLASS_NAME, 'HorseList'):
            row = []
            for td in tr.find_elements(By.TAG_NAME, 'td'):
                if td.get_attribute('class') in ['HorseInfo', 'Jockey']:
                    href = td.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    row.append(re.findall(r'\d+', href)[0])
                row.append(td.text)
            df = df.append(pd.Series(row), ignore_index=True)
            
        #レース結果テーブルと列を揃える
        df = df[[0, 1, 5, 6, 11, 12, 10, 3, 7]]
        df.columns = [Cols.WAKUBAN, Cols.UMABAN, Cols.SEX_AGE, Cols.KINRYO, Cols.TANSHO_ODDS, Cols.POPULARITY, Cols.WEIGHT_AND_DIFF, 'horse_id', 'jockey_id']
        df.index = [race_id] * len(df)
        
        #レース情報の取得
        texts = driver.find_element(By.CLASS_NAME, 'RaceList_Item02').text
        texts = re.findall(r'\w+', texts)
        for text in texts:
            if 'm' in text:
                df['course_len'] = [int(re.findall(r'\d+', text)[-1])] * len(df) #20211212：[0]→[-1]に修正
            if text in ["曇", "晴", "雨", "小雨", "小雪", "雪"]:
                df["weather"] = [text] * len(df)
            if text in ["良", "稍重", "重"]:
                df["ground_state"] = [text] * len(df)
            if '不' in text:
                df["ground_state"] = ['不良'] * len(df)
            # 2020/12/13追加
            if '稍' in text:
                df["ground_state"] = ['稍重'] * len(df)
            if '芝' in text:
                df['race_type'] = ['芝'] * len(df)
            if '障' in text:
                df['race_type'] = ['障害'] * len(df)
            if 'ダ' in text:
                df['race_type'] = ['ダート'] * len(df)
        df['date'] = [date] * len(df)
    except Exception as e:
        print(e)
        driver.close()
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
        html = urlopen(url)
        soup = BeautifulSoup(html, 'lxml', from_encoding='utf-8')
        horse_td_list = soup.find_all("td", attrs={'class': 'HorseInfo'})
        for td in horse_td_list:
            horse_id = re.findall(r'\d+', td.find('a')['href'])[0]
            horse_id_list.append(horse_id)
    return horse_id_list