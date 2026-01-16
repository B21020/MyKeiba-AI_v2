import dataclasses


@dataclasses.dataclass(frozen=True)
class UrlPaths:
    DB_DOMAIN: str = 'https://db.netkeiba.com/'
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RACE_URL: str = DB_DOMAIN + 'race/'
    # 馬の過去成績テーブルが含まれるページ
    HORSE_URL: str = DB_DOMAIN + 'horse/'
    # 血統テーブルが含まれるページ
    PED_URL: str = HORSE_URL + 'ped/'
    
    RACE_DOMAIN: str = 'https://race.netkeiba.com/'
    TOP_URL: str = RACE_DOMAIN + 'top/'
    # 開催日程ページ
    CALENDAR_URL: str = TOP_URL + 'calendar.html'
    # レース一覧ページ
    RACE_LIST_URL: str = TOP_URL + 'race_list.html'

    # レース結果（払戻テーブルが含まれる）
    # 例: https://race.netkeiba.com/race/result.html?race_id=202606010501
    RACE_RESULT_URL: str = RACE_DOMAIN + 'race/result.html?race_id='
    
    # 出馬表ページ
    SHUTUBA_TABLE: str = 'https://race.netkeiba.com/race/shutuba.html'