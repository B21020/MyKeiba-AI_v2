import datetime
import re
import pandas as pd
import time
import os
from tqdm.notebook import tqdm
from urllib.request import urlopen

from modules.constants import UrlPaths, LocalDirs

def scrape_html_race(race_id_list: list, skip: bool = True):
    """
    netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for race_id in tqdm(race_id_list):
        filename = os.path.join(LocalDirs.HTML_RACE_DIR, race_id+'.bin') #保存するファイル名
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
            print('race_id {} skipped'.format(race_id))
        else:
            url = UrlPaths.RACE_URL + race_id #race_idからurlを作る
            html = urlopen(url).read() #スクレイピング実行
            time.sleep(1) #相手サーバーに負担をかけないように1秒待機する
            with open(filename, 'wb') as f: #保存するファイルパスを指定
                f.write(html) #保存
            updated_html_path_list.append(filename)
    return updated_html_path_list

def scrape_html_horse(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for horse_id in tqdm(horse_id_list):
        filename = os.path.join(LocalDirs.HTML_HORSE_DIR, horse_id+'.bin') #保存するファイル名
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
            print('horse_id {} skipped'.format(horse_id))
        else:
            url = UrlPaths.HORSE_URL + horse_id #horse_idからurlを作る
            time.sleep(1) #相手サーバーに負担をかけないように1秒待機する
            html = urlopen(url).read() #スクレイピング実行
            with open(filename, 'wb') as f: #保存するファイルパスを指定
                f.write(html) #保存
            updated_html_path_list.append(filename)
    return updated_html_path_list

def scrape_html_ped(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    updated_html_path_list = []
    for horse_id in tqdm(horse_id_list):
        filename = os.path.join(LocalDirs.HTML_PED_DIR, horse_id+'.bin') #保存するファイル名
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルがすでに存在する場合は飛ばす
            print('horse_id {} skipped'.format(horse_id))
        else:
            url = UrlPaths.PED_URL + horse_id #horse_idからurlを作る
            time.sleep(1) #相手サーバーに負担をかけないように1秒待機する
            html = urlopen(url).read() #スクレイピング実行
            with open(filename, 'wb') as f: #保存するファイルパスを指定
                f.write(html) #保存
            updated_html_path_list.append(filename)
    return updated_html_path_list

def scrape_html_horse_with_updated_at(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    また、horse_idごとに、最後にスクレイピングした日付を記録し、data/master/horse_results_updated_at.csvに保存する。
    """
    ### スクレイピング実行 ###
    updated_html_path_list = scrape_html_horse(horse_id_list, skip)
    # パスから正規表現でhorse_id_listを取得
    horse_id_list = [
        re.findall('(?<=horse/)\d+', html_path)[0] for html_path in updated_html_path_list
        ]
    # DataFrameにしておく
    horse_id_df = pd.DataFrame({'horse_id': horse_id_list})
    
    ### 取得日マスタの更新 ###
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') # 現在日時を取得
    # マスタのパス
    filename_master = os.path.join(LocalDirs.MASTER_DIR, 'horse_results_updated_at.csv')
    # ファイルが存在しない場合は、作成する
    if not os.path.isfile(filename_master):
        pd.DataFrame(columns=['horse_id', 'updated_at']).to_csv(filename_master, index=None)
    # 旧マスタを読み込み、新たなhorse_idを追加
    master = pd.read_csv(filename_master).merge(horse_id_df, how='outer')
    # マスタ更新
    master.loc[master['horse_id'].isin(horse_id_list), :] = now
    master.to_csv(filename_master, index=None)
    
#TODO: scrape_html_horse_with_updated_atのテスト