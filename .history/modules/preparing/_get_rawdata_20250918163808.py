import os
import pandas as pd
import numpy as np
NaN = np.nan
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
import re
from modules.constants import Master

def get_rawdata_results(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース結果テーブルに変換する関数。
    """
    print('preparing raw results table')
    race_results = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()
                # メインとなるレース結果テーブルデータを取得
                df = pd.read_html(html)[0]
                # htmlをsoupオブジェクトに変換
                soup = BeautifulSoup(html, "lxml")

                # 馬IDをスクレイピング
                horse_id_list = []
                horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/horse")}
                )
                for a in horse_a_list:
                    horse_id = re.findall(r"\d+", a["href"])
                    horse_id_list.append(horse_id[0])
                df["horse_id"] = horse_id_list

                # 騎手IDをスクレイピング
                jockey_id_list = []
                jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/jockey")}
                )
                for a in jockey_a_list:
                    #'jockey/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                    jockey_id = re.findall(r"jockey/result/recent/(\w*)", a["href"])
                    jockey_id_list.append(jockey_id[0])
                df["jockey_id"] = jockey_id_list

                # 調教師IDをスクレイピング
                trainer_id_list = []
                trainer_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/trainer")}
                )
                for a in trainer_a_list:
                    #'trainer/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                    trainer_id = re.findall(r"trainer/result/recent/(\w*)", a["href"])
                    trainer_id_list.append(trainer_id[0])
                df["trainer_id"] = trainer_id_list

                # 馬主IDをスクレイピング
                owner_id_list = []
                owner_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/owner")}
                )
                for a in owner_a_list:
                    #'owner/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                    owner_id = re.findall(r"owner/result/recent/(\w*)", a["href"])
                    owner_id_list.append(owner_id[0])
                df["owner_id"] = owner_id_list

                # インデックスをrace_idにする
                # use raw string for regex to avoid invalid escape sequence warning
                race_id = re.findall(r'race\W(\d+)\.bin', html_path)[0]
                df.index = [race_id] * len(df)

                race_results[race_id] = df
            except Exception as e:
                print('error at {}'.format(html_path))
                print(e)
    # pd.DataFrame型にして一つのデータにまとめる
    if not race_results:
        raise ValueError(
            f"No race result tables were parsed. html_path_list size={len(html_path_list)}. "
            "Possible causes: (1) html_path_list is empty, (2) target table structure changed, "
            "(3) earlier exceptions during parsing (they should have been printed above)."
        )
    race_results_df = pd.concat([race_results[key] for key in race_results])

    # 列名に半角スペースがあれば除去する
    race_results_df = race_results_df.rename(columns=lambda x: x.replace(' ', ''))

    return race_results_df

def get_rawdata_info(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
    """
    print('preparing raw race_info table')
    race_infos = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                # htmlをsoupオブジェクトに変換
                soup = BeautifulSoup(html, "lxml")

                # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
                texts = (
                    soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
                    + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
                )
                info = re.findall(r'\w+', texts)
                df = pd.DataFrame()
                # 障害レースフラグを初期化
                hurdle_race_flg = False
                for text in info:
                    if text in ["芝", "ダート"]:
                        df["race_type"] = [text]
                    if "障" in text:
                        df["race_type"] = ["障害"]
                        hurdle_race_flg = True
                    if "0m" in text:
                        # 20211212：[0]→[-1]に修正
                        df["course_len"] = [int(re.findall(r"\d+", text)[-1])]
                    if text in Master.GROUND_STATE_LIST:
                        df["ground_state"] = [text]
                    if text in Master.WEATHER_LIST:
                        df["weather"] = [text]
                    if "年" in text:
                        df["date"] = [text]
                    if "右" in text:
                        df["around"] = [Master.AROUND_LIST[0]]
                    if "左" in text:
                        df["around"] = [Master.AROUND_LIST[1]]
                    if "直線" in text:
                        df["around"] = [Master.AROUND_LIST[2]]
                    if "新馬" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[0]]
                    if "未勝利" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[1]]
                    if ("1勝クラス" in text) or ("500万下" in text):
                        df["race_class"] = [Master.RACE_CLASS_LIST[2]]
                    if ("2勝クラス" in text) or ("1000万下" in text):
                        df["race_class"] = [Master.RACE_CLASS_LIST[3]]
                    if ("3勝クラス" in text) or ("1600万下" in text):
                        df["race_class"] = [Master.RACE_CLASS_LIST[4]]
                    if "オープン" in text:
                        df["race_class"] = [Master.RACE_CLASS_LIST[5]]

                # グレードレース情報の取得
                grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
                if "G3" in grade_text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
                elif "G2" in grade_text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
                elif "G1" in grade_text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

                # 障害レースの場合
                if hurdle_race_flg:
                    df["around"] = [Master.AROUND_LIST[3]]
                    df["race_class"] = [Master.RACE_CLASS_LIST[9]]

                # インデックスをrace_idにする
                race_id = re.findall(r'race\W(\d+)\.bin', html_path)[0]
                df.index = [race_id] * len(df)

                race_infos[race_id] = df
            except Exception as e:
                print('error at {}'.format(html_path))
                print(e)
    # pd.DataFrame型にして一つのデータにまとめる
    race_infos_df = pd.concat([race_infos[key] for key in race_infos])

    return race_infos_df

def get_rawdata_return(html_path_list: list):
    """
    raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
    """
    print('preparing raw return table')
    race_return = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                html = html.replace(b'<br />', b'br')
                dfs = pd.read_html(html)

                # dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
                df = pd.concat([dfs[1], dfs[2]])

                race_id = re.findall(r'race\W(\d+)\.bin', html_path)[0]
                df.index = [race_id] * len(df)
                race_return[race_id] = df
            except Exception as e:
                print('error at {}'.format(html_path))
                print(e)
    # pd.DataFrame型にして一つのデータにまとめる
    race_return_df = pd.concat([race_return[key] for key in race_return])
    return race_return_df

import re
import pandas as pd
from bs4 import BeautifulSoup
from numpy import nan as NaN

def get_rawdata_horse_info(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数（堅牢版）。
    - EUC-JPでデコード
    - プロフィール表は位置ではなく summary/caption/クラス名で特定
    - 調教師/馬主/生産者IDは a[href^="/trainer"] 等から抽出
    """
    out_rows = []

    for html_path in html_path_list:
        raw = open(html_path, 'rb').read()

        # 1) EUC-JPでテキスト化（失敗時はUTF-8へフォールバック）
        for enc in ('euc-jp', 'cp932', 'utf-8'):
            try:
                text = raw.decode(enc)
                break
            except Exception:
                text = raw.decode(enc, errors='ignore')
                break

        soup = BeautifulSoup(text, 'lxml')

        # 2) プロフィールテーブルを位置でなく「意味」で特定
        prof_table = (
            soup.select_one('table.db_prof_table[summary*="プロフィール"]')
            or soup.find('table', attrs={'summary': re.compile('プロフィール')})
        )
        if prof_table is None:
            # テーブルがない → ログを出してスキップ
            # （※ログ集め推奨：ファイル名、title、先頭500文字など）
            # print(f'プロフィール表なし: {html_path}')
            continue

        # 3) テーブルを読み込む（2列：項目/値）
        df = pd.read_html(str(prof_table), flavor='lxml')[0]
        # 左列を項目名、右列を値として転置（1行化）
        if df.shape[1] >= 2:
            df = df.iloc[:, :2]                 # 念のため2列に限定
            df.columns = ['項目', '値']
            df_info = df.set_index('項目').T
        else:
            # 想定外の形はスキップ
            # print(f'プロフィール表の列数が想定外: {html_path}')
            continue

        # 4) 各IDは a要素から直接拾う（親テーブル探索に失敗しても拾える）
        def extract_id(selector, pattern):
            a = soup.select_one(selector)
            if a and a.has_attr('href'):
                m = re.search(pattern, a['href'])
                if m:
                    return m.group(1)
            return NaN

        trainer_id = extract_id('a[href^="/trainer/"]', r'/trainer/([^/]+)/')
        owner_id   = extract_id('a[href^="/owner/"]',   r'/owner/([^/]+)/')
        breeder_id = extract_id('a[href^="/breeder/"]', r'/breeder/([^/]+)/')

        df_info['trainer_id'] = trainer_id
        df_info['owner_id']   = owner_id
        df_info['breeder_id'] = breeder_id

        # 5) インデックスを horse_id に
        horse_id_m = re.search(r'horse\W(\d+)\.bin', html_path)
        horse_id = horse_id_m.group(1) if horse_id_m else None
        df_info.index = [horse_id]

        out_rows.append(df_info)

    if not out_rows:
        return pd.DataFrame()

    horse_info_df = pd.concat(out_rows, axis=0)
    return horse_info_df


def get_rawdata_horse_results(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
    """
    print('preparing raw horse_results table')
    horse_results = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                df = pd.read_html(html)[3]
                # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
                if df.columns[0]=='受賞歴':
                    df = pd.read_html(html)[4]

                # 新馬の競走馬レビューが付いた場合、
                # 列名に0が付与されるため、次のhtmlへ飛ばす
                if df.columns[0] == 0:
                    print('horse_results empty case1 {}'.format(html_path))
                    continue

                horse_id = re.findall(r'horse\W(\d+)\.bin', html_path)[0]

                df.index = [horse_id] * len(df)
                horse_results[horse_id] = df

            # 競走データが無い場合（新馬）を飛ばす
            except IndexError:
                print('horse_results empty case2 {}'.format(html_path))
                continue

    # pd.DataFrame型にして一つのデータにまとめる
    horse_results_df = pd.concat([horse_results[key] for key in horse_results])

    # 列名に半角スペースがあれば除去する
    horse_results_df = horse_results_df.rename(columns=lambda x: x.replace(' ', ''))

    return horse_results_df

def get_rawdata_peds(html_path_list: list):
    """
    horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
    """
    print('preparing raw peds table')
    peds = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            # 保存してあるbinファイルを読み込む
            html = f.read()

            # horse_idを取得
            horse_id = re.findall(r'ped\W(\d+)\.bin', html_path)[0]

            # htmlをsoupオブジェクトに変換
            soup = BeautifulSoup(html, "lxml")

            peds_id_list = []

            # 血統データからhorse_idを取得する
            horse_a_list = (
                soup.find("table", attrs={"summary": "5代血統表"})
                .find_all("a", attrs={"href": re.compile(r"^/horse/\w{10}")})
            )

            for a in horse_a_list:
                # 血統データのhorse_idを抜き出す
                # Use explicit slash in pattern for clarity instead of \W
                work_peds_id = re.findall(r'/horse/(\w{10})', a["href"])[0]
                peds_id_list.append(work_peds_id)

            peds[horse_id] = peds_id_list

    # pd.DataFrame型にして一つのデータにまとめて、列と行の入れ替えして、列名をpeds_0, ..., peds_61にする
    peds_df = pd.DataFrame.from_dict(peds, orient='index').add_prefix('peds_')

    return peds_df

def update_rawdata(filepath: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    filepathにrawテーブルのpickleファイルパスを指定し、new_dfに追加したいDataFrameを指定。
    元々のテーブルにnew_dfが追加されてpickleファイルが更新される。
    pickleファイルが存在しない場合は、filepathに新たに作成される。
    """
    # pickleファイルが存在する場合の更新処理
    if os.path.isfile(filepath):
        backupfilepath = filepath + '.bak'
        # 結合データがない場合
        if new_df.empty:
            print('preparing update raw data empty')
        else:
            # 元々のテーブルを読み込み
            filedf = pd.read_pickle(filepath)
            # new_dfに存在しないindexのみ、旧データを使う
            filtered_old = filedf[~filedf.index.isin(new_df.index)]
            # bakファイルが存在する場合
            if os.path.isfile(backupfilepath):
                os.remove(backupfilepath)
            # バックアップ
            os.rename(filepath, backupfilepath)
            # 結合
            updated = pd.concat([filtered_old, new_df])
            # 保存
            updated.to_pickle(filepath)
    else:
        # pickleファイルが存在しない場合、新たに作成
        new_df.to_pickle(filepath)