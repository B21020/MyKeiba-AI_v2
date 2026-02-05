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
    failed: dict[str, str] = {}

    # NOTE: html_path_list の要素が Path の場合でも動くように os.fspath で文字列化する
    for html_path in tqdm(html_path_list):
        html_path_str = os.fspath(html_path)
        try:
            with open(html_path, 'rb') as f:
                # 保存してあるbinファイルを読み込む
                html = f.read()

            if html is None or len(html) == 0:
                raise ValueError('empty html file')

            # pandas.read_html は bytes のままだと環境依存になりやすいので、先にdecodeしてから読む
            from io import StringIO
            lower = html.lower()
            if (b'euc-jp' in lower) or (b'euc_jp' in lower):
                decoded = html.decode('euc-jp', errors='ignore')
            else:
                decoded = html.decode('utf-8', errors='ignore')
            decoded = decoded.replace('<br />', 'br')

            dfs = pd.read_html(StringIO(decoded))
            if dfs is None or len(dfs) == 0:
                raise ValueError('no tables parsed by read_html')

            # メインとなるレース結果テーブルデータを取得（固定[0]だとページ差分に弱い）
            df = None
            for dfi in dfs:
                if not isinstance(dfi, pd.DataFrame) or dfi.empty:
                    continue
                cols = [str(c).replace(' ', '') for c in dfi.columns]
                if ('着順' in cols) and ('馬名' in cols):
                    df = dfi.copy()
                    df.columns = cols
                    break
            if df is None:
                df = dfs[0].copy()
                df.columns = [str(c).replace(' ', '') for c in df.columns]

            # htmlをsoupオブジェクトに変換
            soup = BeautifulSoup(decoded, "lxml")

            # レース結果テーブルを特定（summary='レース結果' が無いケースもある）
            result_table = soup.find("table", attrs={"summary": "レース結果"})
            horse_href_pattern = re.compile(r"/horse/")
            if result_table is None:
                # まず「馬リンク数 == df行数」で一致するtableを優先
                for table in soup.find_all('table'):
                    n_horse_links = len(table.find_all('a', href=horse_href_pattern))
                    if n_horse_links == len(df):
                        result_table = table
                        break

            if result_table is None:
                # 次点: 馬リンクが最も多いtableを採用
                best_table = None
                best_n = 0
                for table in soup.find_all('table'):
                    n_horse_links = len(table.find_all('a', href=horse_href_pattern))
                    if n_horse_links > best_n:
                        best_table = table
                        best_n = n_horse_links
                result_table = best_table
            if result_table is None:
                raise ValueError('race result table not found in html')

            # 馬IDをスクレイピング
            horse_id_list = []
            horse_a_list = result_table.find_all("a", href=horse_href_pattern)
            for a in horse_a_list:
                horse_id = re.findall(r"\d+", a.get("href", ""))
                if horse_id:
                    horse_id_list.append(horse_id[0])
            if len(horse_id_list) != len(df):
                # fallback: 行単位で拾う（table内に馬リンク以外のリンクが混ざる/構造差分に対応）
                horse_id_list = []
                for tr in result_table.find_all('tr'):
                    a = tr.find('a', href=horse_href_pattern)
                    if a is None:
                        continue
                    horse_id = re.findall(r"\d+", a.get("href", ""))
                    if horse_id:
                        horse_id_list.append(horse_id[0])
                if len(horse_id_list) != len(df):
                    raise ValueError(f'horse_id_list length mismatch: {len(horse_id_list)} != {len(df)}')
            df["horse_id"] = horse_id_list

            # 騎手IDをスクレイピング
            jockey_id_list = []
            jockey_a_list = result_table.find_all("a", href=re.compile(r"/jockey"))
            for a in jockey_a_list:
                # 'jockey/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                jockey_id = re.findall(r"jockey/result/recent/(\w*)", a.get("href", ""))
                if jockey_id:
                    jockey_id_list.append(jockey_id[0])
            if len(jockey_id_list) == len(df):
                df["jockey_id"] = jockey_id_list
            else:
                df["jockey_id"] = NaN

            # 調教師IDをスクレイピング
            trainer_id_list = []
            trainer_a_list = result_table.find_all("a", href=re.compile(r"/trainer"))
            for a in trainer_a_list:
                # 'trainer/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                trainer_id = re.findall(r"trainer/result/recent/(\w*)", a.get("href", ""))
                if trainer_id:
                    trainer_id_list.append(trainer_id[0])
            if len(trainer_id_list) == len(df):
                df["trainer_id"] = trainer_id_list
            else:
                df["trainer_id"] = NaN

            # 馬主IDをスクレイピング
            owner_id_list = []
            owner_a_list = result_table.find_all("a", href=re.compile(r"/owner"))
            for a in owner_a_list:
                # 'owner/result/recent/'より後ろの英数字(及びアンダーバー)を抽出
                owner_id = re.findall(r"owner/result/recent/(\w*)", a.get("href", ""))
                if owner_id:
                    owner_id_list.append(owner_id[0])
            # 新しいページでは owner リンクが掲載されないことがあるため欠損許容
            if len(owner_id_list) == len(df):
                df["owner_id"] = owner_id_list
            else:
                df["owner_id"] = NaN

            # インデックスをrace_idにする
            race_id_match = re.findall(r'race\W(\d+)\.bin', html_path_str)
            if not race_id_match:
                raise ValueError(f'cannot extract race_id from path: {html_path_str}')
            race_id = race_id_match[0]
            df.index = [race_id] * len(df)
            race_results[race_id] = df

        except Exception as e:
            failed[html_path_str] = f'{type(e).__name__}: {e}'
            print('error at {}'.format(html_path_str))
            print(e)
    # pd.DataFrame型にして一つのデータにまとめる
    if not race_results:
        sample_errors = " | ".join(list(failed.values())[:3])
        sample_paths = " | ".join(list(failed.keys())[:3])
        raise ValueError(
            f"No race result tables were parsed. html_path_list size={len(html_path_list)}. "
            f"Sample errors: {sample_errors}. Sample paths: {sample_paths}. "
            "Possible causes: (1) html_path_list is empty, (2) you passed Path objects / wrong paths, "
            "(3) you passed non-result pages (e.g. shutuba), (4) target table structure changed."
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
    failed: dict[str, str] = {}
    for html_path in tqdm(html_path_list):
        html_path_str = os.fspath(html_path)
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                if html is None or len(html) == 0:
                    raise ValueError('empty html file')

                # bytes のままだと BeautifulSoup/pandas で文字化けや拾い漏れが起きやすいのでdecodeして扱う
                lower = html.lower()
                if (b'euc-jp' in lower) or (b'euc_jp' in lower):
                    decoded = html.decode('euc-jp', errors='ignore')
                else:
                    decoded = html.decode('utf-8', errors='ignore')
                decoded = decoded.replace('<br />', 'br')

                # htmlをsoupオブジェクトに変換
                soup = BeautifulSoup(decoded, "lxml")

                # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
                texts = None

                # 旧レイアウト（data_intro）
                data_intro = soup.find("div", attrs={"class": "data_intro"})
                if data_intro is not None:
                    ps = data_intro.find_all('p')
                    if len(ps) >= 2:
                        texts = ps[0].get_text(' ', strip=True) + ps[1].get_text(' ', strip=True)

                # 新レイアウト（RaceData01/02 等）
                if texts is None:
                    parts: list[str] = []
                    for sel in ('.RaceName', '.RaceData01', '.RaceData02', '.RaceData03', '.RaceData04'):
                        el = soup.select_one(sel)
                        if el is not None:
                            parts.append(el.get_text(' ', strip=True))
                    # title は冗長だが最低限の補助として足す
                    title = soup.find('title')
                    if title is not None:
                        parts.append(title.get_text(' ', strip=True))
                    texts = ' '.join([p for p in parts if p])

                if not texts:
                    raise ValueError('race info text not found in html')

                # 旧実装は \w+ で分割していたが、新レイアウトでは「天候:晴」などの記号が重要なので
                # テキスト全文を使って正規表現/含有で判定する。
                info = re.findall(r'\w+|[左右直線]+|G\d', texts)
                df = pd.DataFrame()
                # 障害レースフラグを初期化
                hurdle_race_flg = False

                # レース種別/距離（新旧共通で拾えるよう正規表現）
                m_course = re.search(r'(芝|ダート|ダ|障害|障)\s*([左右直線内外]*?)\s*(\d{3,4})m', texts)
                if m_course:
                    rt = m_course.group(1)
                    if rt == 'ダ':
                        rt = 'ダート'
                    if rt in ('障', '障害'):
                        rt = '障害'
                        hurdle_race_flg = True
                    df['race_type'] = [rt]
                    df['course_len'] = [int(m_course.group(3))]

                # 天候/馬場
                for w in Master.WEATHER_LIST:
                    if w in texts:
                        df['weather'] = [w]
                        break
                for gs in Master.GROUND_STATE_LIST:
                    if gs in texts:
                        df['ground_state'] = [gs]
                        break

                # 日付
                m_date = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', texts)
                if m_date:
                    df['date'] = [m_date.group(1)]

                # 回り
                if '直線' in texts:
                    df['around'] = [Master.AROUND_LIST[2]]
                elif '右' in texts:
                    df['around'] = [Master.AROUND_LIST[0]]
                elif '左' in texts:
                    df['around'] = [Master.AROUND_LIST[1]]

                # クラス（グレード/条件戦）
                if 'G3' in texts:
                    df['race_class'] = [Master.RACE_CLASS_LIST[6]]
                elif 'G2' in texts:
                    df['race_class'] = [Master.RACE_CLASS_LIST[7]]
                elif 'G1' in texts:
                    df['race_class'] = [Master.RACE_CLASS_LIST[8]]
                else:
                    if '新馬' in texts:
                        df['race_class'] = [Master.RACE_CLASS_LIST[0]]
                    elif '未勝利' in texts:
                        df['race_class'] = [Master.RACE_CLASS_LIST[1]]
                    elif ('1勝クラス' in texts) or ('500万下' in texts):
                        df['race_class'] = [Master.RACE_CLASS_LIST[2]]
                    elif ('2勝クラス' in texts) or ('1000万下' in texts):
                        df['race_class'] = [Master.RACE_CLASS_LIST[3]]
                    elif ('3勝クラス' in texts) or ('1600万下' in texts):
                        df['race_class'] = [Master.RACE_CLASS_LIST[4]]
                    elif 'オープン' in texts:
                        df['race_class'] = [Master.RACE_CLASS_LIST[5]]

                # 障害レースの場合
                if hurdle_race_flg:
                    df["around"] = [Master.AROUND_LIST[3]]
                    df["race_class"] = [Master.RACE_CLASS_LIST[9]]

                # インデックスをrace_idにする
                race_id = re.findall(r'race\W(\d+)\.bin', html_path_str)[0]
                df.index = [race_id] * len(df)

                race_infos[race_id] = df
            except Exception as e:
                print('error at {}'.format(html_path_str))
                print(e)
                failed[html_path_str] = f'{type(e).__name__}: {e}'
    # pd.DataFrame型にして一つのデータにまとめる
    if not race_infos:
        sample_errors = " | ".join(list(failed.values())[:3])
        sample_paths = " | ".join(list(failed.keys())[:3])
        raise ValueError(
            f"No race info tables were parsed (No objects to concatenate). html_path_list size={len(html_path_list)}. "
            f"Sample errors: {sample_errors}. Sample paths: {sample_paths}."
        )

    race_infos_df = pd.concat([race_infos[key] for key in race_infos])

    return race_infos_df

def get_rawdata_return(html_path_list: list):
    """
    raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
    """
    print('preparing raw return table')
    race_return = {}
    failed: dict[str, str] = {}
    for html_path in tqdm(html_path_list):
        race_id = None
        try:
            html_path_str = os.fspath(html_path)
            m = re.findall(r'race\W(\d+)\.bin', html_path_str)
            race_id = m[0] if len(m) > 0 else None

            with open(html_path, 'rb') as f:
                # 保存してあるbinファイルを読み込む
                html = f.read()

            if html is None or len(html) == 0:
                raise ValueError('empty html file')

            # NOTE:
            # - netkeiba は meta charset=EUC-JP が多い
            # - bytes のまま read_html すると環境依存で拾える表が減ることがあるため、先にdecodeしてから読む
            from io import StringIO

            lower = html.lower()
            # result.html は meta charset="EUC-JP" 形式が多いので、"charset=euc-jp" だけだと検出漏れする。
            if (b'euc-jp' in lower) or (b'euc_jp' in lower):
                decoded = html.decode('euc-jp', errors='ignore')
            else:
                decoded = html.decode('utf-8', errors='ignore')
            decoded = decoded.replace('<br />', 'br')

            # まずは result.html の払戻テーブル（Payout_Detail_Table）を直接拾う。
            # bs4のパーサ相性問題もあり得るので、regex抽出→read_html を優先する。
            payout_dfs: list[pd.DataFrame] = []
            try:
                pattern = (
                    r'(<table[^>]*class=(?:"[^"]*Payout_Detail_Table[^"]*"|\'[^\']*Payout_Detail_Table[^\']*\')[^>]*>.*?</table>)'
                )
                table_htmls = re.findall(pattern, decoded, flags=re.IGNORECASE | re.DOTALL)
                for table_html in table_htmls:
                    try:
                        payout_dfs.append(pd.read_html(StringIO(table_html))[0])
                    except Exception:
                        continue
            except Exception:
                payout_dfs = []

            if payout_dfs:
                df = pd.concat(payout_dfs, axis=0)
            else:
                # NOTE:
                # - ページによっては「壊れた表」(列見出しが空など)が混ざり、
                #   pandas.read_html が IndexError(list index out of range) を投げることがある。
                # - その場合は BeautifulSoup で <table> を切り出して、読めるものだけを集める。
                try:
                    dfs = pd.read_html(StringIO(decoded))
                except Exception as e_read_html:
                    from bs4 import BeautifulSoup

                    try:
                        soup = BeautifulSoup(decoded, 'lxml')
                        dfs = []
                        for table in soup.find_all('table'):
                            try:
                                dfs.extend(pd.read_html(StringIO(str(table))))
                            except Exception:
                                continue
                    except Exception:
                        # fallback自体が失敗した場合は、元の例外を優先して投げる
                        raise e_read_html

                if dfs is None or len(dfs) == 0:
                    raise ValueError('no tables parsed by read_html')

                payout_keywords = (
                    '単勝', '複勝', '枠連', '馬連', 'ワイド', '馬単', '三連複', '三連単', 'WIN5'
                )

                payout_dfs: list[pd.DataFrame] = []
                for dfi in dfs:
                    if not isinstance(dfi, pd.DataFrame) or dfi.empty or dfi.shape[1] < 2:
                        continue
                    try:
                        col0 = dfi.columns[0]
                        s0 = dfi[col0].astype(str)
                    except Exception:
                        continue
                    if s0.str.contains('|'.join(payout_keywords), regex=True).any():
                        payout_dfs.append(dfi)

                if not payout_dfs:
                    # 期待する払戻テーブルが無いケースを明示
                    raise ValueError(
                        f'payout tables not found in parsed tables: n_tables={len(dfs)}'
                    )

                df = pd.concat(payout_dfs, axis=0)

            # result.html では払戻金が「190円」のように単位付きで入ることがあるため、後段のReturnProcessorが
            # 数値変換できるように「円」を除去（NaNを文字列化しないように注意）。
            for c in (1, 2):
                if c in df.columns:
                    ser = df[c]
                    df[c] = ser.where(ser.isna(), ser.astype(str).str.replace('円', '', regex=False))

            # result.html の複勝/ワイドは <br> 区切りが pandas.read_html で空白区切りになることがある。
            # 既存ReturnProcessorは 'br' 区切りを前提にしているため、空白→'br' に正規化する。
            if 0 in df.columns:
                # 券種名の揺れを正規化（ReturnProcessorが期待する表記に寄せる）
                df[0] = df[0].replace({'3連複': '三連複', '3連単': '三連単'})
                multi_mask = df[0].astype(str).isin(['複勝', 'ワイド'])
                for c in (1, 2):
                    if c in df.columns:
                        ser = df.loc[multi_mask, c]
                        df.loc[multi_mask, c] = ser.where(
                            ser.isna(),
                            ser.astype(str).str.replace(r'\s+', 'br', regex=True)
                        )

            if race_id is None:
                raise ValueError('race_id not found from html path')
            df.index = [race_id] * len(df)
            race_return[race_id] = df

        except Exception as e:
            key = race_id if race_id is not None else html_path_str
            failed[key] = f'{type(e).__name__}: {e}'
            print('error at {}'.format(html_path_str))
            print(e)

    # pd.DataFrame型にして一つのデータにまとめる
    if not race_return:
        n_total = len(html_path_list)
        failed_ids = sorted(failed.keys())
        # 例外メッセージが長くなりすぎないよう先頭だけ
        sample_ids = failed_ids[:20]
        sample_errors = list(failed.items())[:5]
        details = "\n".join([f"- {rid}: {msg}" for rid, msg in sample_errors])
        raise ValueError(
            "No return tables were parsed (No objects to concatenate). "
            f"html_path_list size={n_total}, parsed=0, failed={len(failed_ids)}. "
            f"failed race_id sample={sample_ids}.\n"
            "Representative errors:\n"
            f"{details}\n"
            "Hints: (1) HTMLがレース結果ページではない/取得失敗ページ, (2) 払戻テーブル未掲載, "
            "(3) netkeiba側のHTML構造変更, (4) 保存ファイルが空。"
        )

    race_return_df = pd.concat([race_return[key] for key in race_return])
    return race_return_df

import re
import pandas as pd
from bs4 import BeautifulSoup
from numpy import nan as NaN
from io import StringIO
from tqdm.auto import tqdm

def get_rawdata_horse_info(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数（修正版）。
    - UTF-8優先でデコード
    - プロフィールテーブルを確実に特定
    - 調教師/馬主/生産者IDを確実に抽出
    """
    print('preparing raw horse_info table')
    out_rows = []

    for html_path in tqdm(html_path_list):
        try:
            with open(html_path, 'rb') as f:
                raw = f.read()

            # 1) エンコーディング優先順位: UTF-8 → EUC-JP → CP932
            text = None
            for encoding in ['utf-8', 'euc-jp', 'cp932']:
                try:
                    text = raw.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if text is None:
                # print(f'エンコーディング失敗: {html_path}')
                continue

            soup = BeautifulSoup(text, 'lxml')

            # 2) プロフィールテーブルの確実な特定
            prof_table = (
                soup.find('table', class_='db_prof_table') or
                soup.find('table', attrs={'summary': re.compile('プロフィール')}) or
                soup.select_one('table[summary*="プロフィール"]')
            )
            
            if prof_table is None:
                # print(f'プロフィールテーブル見つからず: {html_path}')
                continue

            # 3) テーブルを読み込む（StringIOを使用して警告を回避）
            df = pd.read_html(StringIO(str(prof_table)))[0]
            
            # 左列を項目名、右列を値として転置（1行化）
            if df.shape[1] >= 2:
                df = df.iloc[:, :2]
                df.columns = ['項目', '値']
                df_info = df.set_index('項目').T
            else:
                # print(f'プロフィールテーブルの列数が想定外: {html_path}')
                continue

            # 4) 各IDをより確実に抽出
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
            if horse_id_m:
                horse_id = horse_id_m.group(1)
                df_info.index = [horse_id]
                out_rows.append(df_info)
            # else:
            #     print(f'horse_id抽出失敗: {html_path}')
                
        except Exception as e:
            # print(f'処理エラー {html_path}: {e}')
            continue

    if not out_rows:
        # print('処理できたhorse_infoデータがありません')
        return pd.DataFrame()

    horse_info_df = pd.concat(out_rows, axis=0)
    return horse_info_df


def get_rawdata_horse_results(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
    AJAX実装対応版: 過去成績テーブルはインデックス1（2番目）にある
    """
    print('preparing raw horse_results table')
    horse_results = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, 'rb') as f:
            try:
                # 保存してあるbinファイルを読み込む
                html = f.read()

                # AJAX実装では、過去成績テーブルは2番目（インデックス1）
                dfs = pd.read_html(html)
                
                # テーブル数の確認
                if len(dfs) < 2:
                    print(f'horse_results insufficient tables: {len(dfs)} tables in {html_path}')
                    continue
                
                # 過去成績テーブルは2番目（インデックス1）
                df = dfs[1]
                
                # 受賞歴がある馬の場合の処理（必要に応じて）
                if df.columns[0]=='受賞歴':
                    # 受賞歴テーブルがある場合は次のテーブルを試す
                    if len(dfs) > 2:
                        df = dfs[2]
                    else:
                        print(f'horse_results no race results after awards table: {html_path}')
                        continue

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
            except Exception as e:
                print(f'horse_results error in {html_path}: {e}')
                continue

    if not horse_results:
        print("警告: 処理できた過去成績データがありません")
        return pd.DataFrame()

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
        try:
            with open(html_path, 'rb') as f:
                # 保存してあるbinファイルを読み込む
                raw = f.read()

            # horse_idを取得
            horse_id = re.findall(r'ped\W(\d+)\.bin', html_path)[0]

            # エンコーディングを試行（UTF-8 → EUC-JP → CP932）
            for encoding in ['utf-8', 'euc-jp', 'cp932']:
                try:
                    html = raw.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print(f"デコードに失敗しました: {horse_id}")
                peds[horse_id] = []
                continue

            # htmlをsoupオブジェクトに変換
            soup = BeautifulSoup(html, "lxml")

            # 血統テーブルを検索
            blood_table = soup.find("table", attrs={"summary": "5代血統表"})
            
            if blood_table is None:
                print(f"血統テーブルが見つかりません: {horse_id}")
                peds[horse_id] = []
                continue

            peds_id_list = []

            # 修正された正規表現パターンで血統データからhorse_idを取得する
            pattern = r'https://db\.netkeiba\.com/horse/(\w{10})/$'
            horse_a_list = blood_table.find_all("a", attrs={"href": re.compile(pattern)})

            for a in horse_a_list:
                # 血統データのhorse_idを抜き出す
                href = a.get('href')
                match = re.search(pattern, href)
                if match:
                    work_peds_id = match.group(1)
                    peds_id_list.append(work_peds_id)

            peds[horse_id] = peds_id_list

        except Exception as e:
            print(f"エラーが発生しました {html_path}: {e}")
            peds[horse_id] = []
            continue

    # pd.DataFrame型にして一つのデータにまとめて、列と行の入れ替えして、列名をpeds_0, ..., peds_61にする
    peds_df = pd.DataFrame.from_dict(peds, orient='index').add_prefix('peds_')

    return peds_df

def update_rawdata(filepath: str, new_df: pd.DataFrame, mode: str = 'update') -> pd.DataFrame:
    """
    filepathにrawテーブルのpickleファイルパスを指定し、new_dfに追加したいDataFrameを指定。
    
    Parameters:
    -----------
    filepath : str
        保存先のpickleファイルパス
    new_df : pd.DataFrame
        追加・更新したいDataFrame
    mode : str, default 'update'
        - 'update': 既存データは保持、新規データのみ追加/更新（推奨）
        - 'replace': 同一インデックスのデータを完全置換（従来の動作）
        - 'append': 完全追加（重複も許可、統計精度最優先）
    
    Returns:
    --------
    pd.DataFrame : 更新後のDataFrame（統計情報出力用）
    """
    # pickleファイルが存在する場合の更新処理
    if os.path.isfile(filepath):
        backupfilepath = filepath + '.bak'
        # 結合データがない場合
        if new_df.empty:
            print('preparing update raw data empty')
            return pd.read_pickle(filepath)
        else:
            # 元々のテーブルを読み込み
            filedf = pd.read_pickle(filepath)
            
            if mode == 'append':
                # 完全追加モード：重複を許可して全データを保持
                print(f'追加モード: 既存 {len(filedf)} + 新規 {len(new_df)} = 合計 {len(filedf) + len(new_df)} レコード')
                updated = pd.concat([filedf, new_df])
                
            elif mode == 'update':
                # 更新モード：新規データのみ追加、既存は保持
                new_indices = new_df.index[~new_df.index.isin(filedf.index)]
                new_data_only = new_df.loc[new_indices]
                print(f'更新モード: 既存 {len(filedf)} + 新規 {len(new_data_only)} = 合計 {len(filedf) + len(new_data_only)} レコード')
                updated = pd.concat([filedf, new_data_only])
                
            elif mode == 'replace':
                # 置換モード：従来の動作（既存データの上書き）
                filtered_old = filedf[~filedf.index.isin(new_df.index)]
                print(f'置換モード: 保持 {len(filtered_old)} + 置換 {len(new_df)} = 合計 {len(filtered_old) + len(new_df)} レコード')
                updated = pd.concat([filtered_old, new_df])
                
            else:
                raise ValueError(f"無効なmode: {mode}. 'update', 'replace', 'append'のいずれかを指定してください。")
            
            # bakファイルが存在する場合は削除
            if os.path.isfile(backupfilepath):
                os.remove(backupfilepath)
            # バックアップ作成
            os.rename(filepath, backupfilepath)
            # 更新されたデータを保存
            updated.to_pickle(filepath)
            
            print(f'データ更新完了: {filepath}')
            return updated
    else:
        # pickleファイルが存在しない場合、新たに作成
        print(f'新規作成: {len(new_df)} レコードを保存')
        new_df.to_pickle(filepath)
        return new_df