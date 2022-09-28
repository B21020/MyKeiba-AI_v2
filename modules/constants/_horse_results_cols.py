import dataclasses


@dataclasses.dataclass(frozen=True)
class HorseResultsCols:
    """
    サイト上のテーブル列名を、定数として持っておく。
    """
    DATE: str = '日付'
    PLACE: str = '開催'
    WEATHER: str = '天 気'
    R: str = 'R'
    RACE_NAME: str = 'レース名'
    N_HORSES: str = '頭 数'
    WAKUBAN: str = '枠 番'
    UMABAN: str = '馬 番'
    TANSHO_ODDS: str = 'オ ッ ズ'
    POPULARITY: str = '人 気'
    RANK: str = '着 順'
    JOCKEY: str = '騎手'
    KINRYO: str = '斤 量'
    RACE_TYPE_COURSE_LEN: str = '距離'
    GROUND_STATE: str = '馬 場'
    TIME: str = 'タイム'
    RANK_DIFF: str = '着差'
    CORNER: str = '通過'
    PACE: str = 'ペース'
    NOBORI: str = '上り'
    WEIGHT_AND_DIFF: str = '馬体重'
    PRIZE: str = '賞金'
