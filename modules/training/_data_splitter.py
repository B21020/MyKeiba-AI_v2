import lightgbm as lgb
import pandas as pd
import numpy as np

try:
    import optuna.integration.lightgbm as lgb_o
except ModuleNotFoundError:
    # 推論（モデルロード/スコア計算）ではoptuna連携は不要。
    # チューニング実行時のみ optuna-integration[lightgbm] を要求する。
    lgb_o = None

from modules.constants import ResultsCols


SPLIT_DATE_KEYS = ('train_start', 'valid_start', 'test_start', 'test_end')


def split_race_ids_by_dates(date_per_race: pd.Series, split_dates: dict) -> tuple[pd.Index, pd.Index, pd.Index]:
    """race_id -> 日付 から、日付の境界で学習・検証・テストの race_id を作る。

    - 学習: train_start <= date < valid_start
    - 検証（early stopping 用）: valid_start <= date < test_start
    - テスト: test_start <= date <= test_end
    同じレースの出走馬は必ず同じ区分に入る。
    """
    missing = [k for k in SPLIT_DATE_KEYS if k not in split_dates]
    if missing:
        raise ValueError(f'split_dates is missing keys: {missing}')
    b = {k: pd.Timestamp(split_dates[k]) for k in SPLIT_DATE_KEYS}
    if not (b['train_start'] < b['valid_start'] < b['test_start'] <= b['test_end']):
        raise ValueError(f'split_dates must satisfy train_start < valid_start < test_start <= test_end: {split_dates}')
    d = pd.to_datetime(date_per_race)
    train = d[(d >= b['train_start']) & (d < b['valid_start'])].sort_values().index
    valid = d[(d >= b['valid_start']) & (d < b['test_start'])].sort_values().index
    test = d[(d >= b['test_start']) & (d <= b['test_end'])].sort_values().index
    if len(train) == 0 or len(valid) == 0 or len(test) == 0:
        raise ValueError(f'empty split: train={len(train)} valid={len(valid)} test={len(test)}')
    return train, valid, test


class DataSplitter:
    def __init__(
        self,
        featured_data,
        test_size=None,
        valid_size=None,
        use_race_normalized_weight: bool = False,
        time_decay_half_life_days: float | None = None,
        split_dates: dict | None = None,
    ) -> None:
        """
        split_dates を渡すと、比率の代わりに日付の境界で分割する（SPLIT_DATE_KEYS を参照）。
        どちらの場合も、学習（X_train）と early stopping 用の検証（X_valid）は重ならない。
        """
        if split_dates is None and (test_size is None or valid_size is None):
            raise ValueError('Either split_dates or both test_size and valid_size must be given')
        self.__split_dates = dict(split_dates) if split_dates is not None else None
        self.__featured_data = featured_data
        self.__use_race_normalized_weight = bool(use_race_normalized_weight)
        self.__time_decay_half_life_days = time_decay_half_life_days
        self.__time_decay_ref_date = None
        self.__lgb_train_optuna = None
        self.__lgb_valid_optuna = None
        self.__X_test = None
        self.__y_test = None
        self.__test_data = None
        self.__tansho_odds_test = None

        self.__sample_weight_train = None
        self.__sample_weight_train_optuna = None
        self.__sample_weight_valid_optuna = None
        self.train_valid_test_split(test_size, valid_size)

    def train_valid_test_split(self, test_size, valid_size):
        """
        訓練データとテストデータに分ける。さらに訓練データをoptuna用の訓練データと検証データに分ける。
        """
        df = self.__featured_data

        # 学習・評価に用いる特徴量からは、目的変数・日付・オッズ・馬番を除外する
        # NOTE: 目的変数候補（例: win/longshot）が混入するとリークするため除外する
        drop_cols = ['rank', 'win', 'longshot', 'date', ResultsCols.TANSHO_ODDS, ResultsCols.UMABAN, 'race_id']
        self.__feature_cols = [c for c in df.columns if c not in drop_cols]

        # race_id単位の代表日付（小さいSeries）を作ってからID分割する
        # NOTE: featured_data の index が race_id とは限らないため、race_id 列があればそれを優先する
        date_series = df['date']
        if 'race_id' in df.columns:
            race_keys = df['race_id']
        else:
            race_keys = self.__get_race_keys(df.index)
        date_per_race = date_series.groupby(race_keys).first()

        # id分割（巨大DataFrameの実体コピーを作らない）
        if getattr(self, '_DataSplitter__split_dates', None) is not None:
            train_optuna_ids, valid_optuna_ids, test_id_list = split_race_ids_by_dates(date_per_race, self.__split_dates)
            train_id_list = train_optuna_ids.append(valid_optuna_ids)
        else:
            train_id_list, test_id_list = self.__split_id_by_date(date_per_race, test_size=test_size)
            # train/valid（optuna用）も id で分割（必要時にDatasetへ変換）
            train_optuna_ids, valid_optuna_ids = self.__split_id_by_date(date_per_race.loc[train_id_list], test_size=valid_size)
        # train_id_list = 学習＋検証（train_data 互換）。モデルの fit には train_optuna_ids だけを使う
        self.__train_id_list = train_id_list
        self.__test_id_list = test_id_list
        self.__train_optuna_id_list = train_optuna_ids
        self.__valid_optuna_id_list = valid_optuna_ids

        # 時系列重み付け用の基準日（fit に使う期間内の最大日付）
        try:
            self.__time_decay_ref_date = pd.to_datetime(date_per_race.loc[train_optuna_ids]).max()
        except Exception:
            self.__time_decay_ref_date = None

        # 学習に必要な最小限だけ先に作る（test側は後段で必要なら作る）
        # NOTE: X_train は early stopping 用の検証期間を含まない（検証で止める反復数が楽観にならないように）
        df_train = self.__select_by_race_ids(df, train_optuna_ids)
        self.__X_train = df_train.loc[:, self.__feature_cols]
        self.__y_train = df_train.loc[:, 'rank']

        # optuna用も遅延（Dataset作成時に生成）
        self.__X_train_optuna = None
        self.__y_train_optuna = None
        self.__X_valid_optuna = None
        self.__y_valid_optuna = None

        # split が変わったのでキャッシュをリセット
        self.__sample_weight_train = None
        self.__sample_weight_train_optuna = None
        self.__sample_weight_valid_optuna = None

        self.__X_test = None
        self.__y_test = None
        self.__test_data = None
        self.__tansho_odds_test = None

    def __ensure_compat_attrs(self) -> None:
        """旧pickle互換: 後から追加したprivate属性が欠けている場合に補完する。"""
        defaults = {
            '_DataSplitter__X_test': None,
            '_DataSplitter__y_test': None,
            '_DataSplitter__test_data': None,
            '_DataSplitter__tansho_odds_test': None,
            '_DataSplitter__sample_weight_train': None,
            '_DataSplitter__sample_weight_train_optuna': None,
            '_DataSplitter__sample_weight_valid_optuna': None,
        }
        for attr, default in defaults.items():
            if not hasattr(self, attr):
                setattr(self, attr, default)

    @property
    def test_data(self) -> pd.DataFrame:
        """テスト期間の元データ（特徴量 + メタ列）。

        単勝EVなどで `race_id` / `馬番` / `単勝オッズ` が必要になるため、
        X_test（学習特徴量のみ）とは別に保持する。
        """
        self.__ensure_compat_attrs()
        test_data = getattr(self, '_DataSplitter__test_data', None)
        if test_data is None:
            test_data = self.__select_by_race_ids(self.__featured_data, self.__test_id_list)
            setattr(self, '_DataSplitter__test_data', test_data)
        return test_data

    @staticmethod
    def __get_race_keys(index) -> pd.Index:
        if isinstance(index, pd.MultiIndex):
            return index.get_level_values(0)
        return index

    @staticmethod
    def __select_by_race_ids(df: pd.DataFrame, race_id_list) -> pd.DataFrame:
        """race_id で行抽出する。

        - MultiIndex の level0 が race_id の場合: df.loc[race_id_list]
        - race_id が列として存在する場合: isin で抽出
        - それ以外（後方互換）: df.loc[race_id_list]
        """
        if 'race_id' in df.columns:
            sliced = df[df['race_id'].isin(race_id_list)]
            if not sliced.empty:
                return sliced
            # 互換: 旧pickleでは race_id_list が index のキー（行ID）になっている場合がある
            try:
                return df.loc[race_id_list]
            except Exception:
                return sliced
        return df.loc[race_id_list]

    def __compute_sample_weight(self, df_slice: pd.DataFrame, *, apply_time_decay: bool) -> pd.Series:
        """LightGBM の sample_weight。

        - レース正規化: 各レースの寄与をほぼ等しくする（1/n_horses）
        - 時系列減衰: 学習期間内で最近データをやや重くする（半減期指定）
        """
        w = pd.Series(1.0, index=df_slice.index, dtype='float32')

        if self.__use_race_normalized_weight:
            if 'race_id' in df_slice.columns:
                race_keys = df_slice['race_id']
            else:
                race_keys = self.__get_race_keys(df_slice.index)
            counts = pd.Series(race_keys).value_counts()
            inv = pd.Series(race_keys, index=df_slice.index).map(lambda k: 1.0 / float(counts[k]))
            w *= inv.astype('float32')

        if apply_time_decay and self.__time_decay_half_life_days is not None:
            try:
                half_life = float(self.__time_decay_half_life_days)
                if half_life > 0:
                    ref = self.__time_decay_ref_date
                    if ref is None:
                        ref = pd.to_datetime(df_slice['date']).max()
                    dates = pd.to_datetime(df_slice['date'])
                    delta_days = (pd.to_datetime(ref) - dates).dt.total_seconds() / (60 * 60 * 24)
                    delta_days = delta_days.clip(lower=0.0)
                    decay = np.exp(-np.log(2.0) * (delta_days / half_life)).astype('float32')
                    w *= pd.Series(decay, index=df_slice.index)
            except Exception:
                pass

        return w

    def __build_lgb_dataset(self, X, y, sample_weight: pd.Series | None = None):
        dataset_cls = lgb_o.Dataset if lgb_o is not None else lgb.Dataset

        # category列はLightGBMに明示しておく（pandas category を保持する前提）
        try:
            categorical_cols = X.select_dtypes(include='category').columns.tolist()
        except Exception:
            categorical_cols = []

        return dataset_cls(
            X,
            y,
            weight=sample_weight,
            feature_name=list(X.columns),
            categorical_feature=categorical_cols if categorical_cols else 'auto',
            free_raw_data=True,
        )

    def __split_id_by_date(self, df_or_series, test_size):
        """
        時系列に沿って訓練IDとテストIDに分ける関数。test_sizeは0~1。
        """
        # df_or_series は race_id -> date のSeries（推奨）または date列を含むDataFrame
        if isinstance(df_or_series, pd.Series):
            date_per_race = df_or_series
        else:
            date_series = df_or_series['date']
            if 'race_id' in df_or_series.columns:
                race_keys = df_or_series['race_id']
            else:
                race_keys = self.__get_race_keys(df_or_series.index)
            date_per_race = date_series.groupby(race_keys).first()

        sorted_id_list = date_per_race.sort_values().index
        train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1 - test_size))]
        test_id_list = sorted_id_list[round(len(sorted_id_list) * (1 - test_size)) :]
        return train_id_list, test_id_list

    @property
    def featured_data(self):
        return self.__featured_data

    @property
    def fit_race_ids(self) -> pd.Index:
        """モデルの fit に使う race_id（検証期間を含まない）。"""
        return pd.Index(self.__train_optuna_id_list)

    @property
    def valid_race_ids(self) -> pd.Index:
        """early stopping 用の検証 race_id。"""
        return pd.Index(self.__valid_optuna_id_list)

    @property
    def test_race_ids(self) -> pd.Index:
        return pd.Index(self.__test_id_list)

    @property
    def train_data(self):
        return self.__select_by_race_ids(self.__featured_data, self.__train_id_list)

    # NOTE: test_data は上でキャッシュ付きpropertyとして定義済み。
    # ここで再定義すると上書きされてしまうため、定義しない。

    @property
    def train_data_optuna(self):
        return self.__select_by_race_ids(self.__featured_data, self.__train_optuna_id_list)

    @property
    def valid_data_optuna(self):
        return self.__select_by_race_ids(self.__featured_data, self.__valid_optuna_id_list)

    @property
    def lgb_train_optuna(self):
        if self.__lgb_train_optuna is None:
            if self.__X_train_optuna is None:
                df = self.__select_by_race_ids(self.__featured_data, self.__train_optuna_id_list)
                self.__X_train_optuna = df.loc[:, self.__feature_cols]
                self.__y_train_optuna = df.loc[:, 'rank']
            if self.__sample_weight_train_optuna is None:
                df = self.__select_by_race_ids(self.__featured_data, self.__train_optuna_id_list)
                self.__sample_weight_train_optuna = self.__compute_sample_weight(df, apply_time_decay=True)
            self.__lgb_train_optuna = self.__build_lgb_dataset(
                self.__X_train_optuna,
                self.__y_train_optuna,
                sample_weight=self.__sample_weight_train_optuna,
            )
        return self.__lgb_train_optuna

    @property
    def lgb_valid_optuna(self):
        if self.__lgb_valid_optuna is None:
            if self.__X_valid_optuna is None:
                df = self.__select_by_race_ids(self.__featured_data, self.__valid_optuna_id_list)
                self.__X_valid_optuna = df.loc[:, self.__feature_cols]
                self.__y_valid_optuna = df.loc[:, 'rank']
            if self.__sample_weight_valid_optuna is None:
                df = self.__select_by_race_ids(self.__featured_data, self.__valid_optuna_id_list)
                # valid は early stopping 用。基本は等重み（安定優先）
                self.__sample_weight_valid_optuna = self.__compute_sample_weight(df, apply_time_decay=False)
            self.__lgb_valid_optuna = self.__build_lgb_dataset(
                self.__X_valid_optuna,
                self.__y_valid_optuna,
                sample_weight=self.__sample_weight_valid_optuna,
            )
        return self.__lgb_valid_optuna

    @property
    def sample_weight_train(self) -> pd.Series | None:
        self.__ensure_compat_attrs()
        if not self.__use_race_normalized_weight and self.__time_decay_half_life_days is None:
            return None
        if self.__sample_weight_train is None:
            # X_train と同じ行（検証期間を含まない）
            df = self.__select_by_race_ids(self.__featured_data, self.__train_optuna_id_list)
            self.__sample_weight_train = self.__compute_sample_weight(df, apply_time_decay=True)
        return self.__sample_weight_train

    @property
    def sample_weight_valid(self) -> pd.Series | None:
        self.__ensure_compat_attrs()
        if not self.__use_race_normalized_weight and self.__time_decay_half_life_days is None:
            return None
        # valid は optuna valid と同一ID
        _ = self.lgb_valid_optuna
        return self.__sample_weight_valid_optuna

    @property
    def X_train(self):
        return self.__X_train

    @property
    def y_train(self):
        return self.__y_train

    @property
    def X_valid(self):
        """early stopping 用の検証データ（train内のvalid split）。"""
        if self.__X_valid_optuna is None:
            df = self.__select_by_race_ids(self.__featured_data, self.__valid_optuna_id_list)
            self.__X_valid_optuna = df.loc[:, self.__feature_cols]
            self.__y_valid_optuna = df.loc[:, 'rank']
        return self.__X_valid_optuna

    @property
    def y_valid(self):
        if self.__y_valid_optuna is None:
            _ = self.X_valid
        return self.__y_valid_optuna

    @property
    def X_test(self):
        self.__ensure_compat_attrs()
        if self.__X_test is None:
            df = self.test_data
            self.__X_test = df.loc[:, self.__feature_cols]
        return self.__X_test

    @property
    def y_test(self):
        self.__ensure_compat_attrs()
        if self.__y_test is None:
            df = self.test_data
            self.__y_test = df.loc[:, 'rank']
        return self.__y_test

    @property
    def tansho_odds_test(self):
        self.__ensure_compat_attrs()
        # 互換: 旧pickleで空Seriesがキャッシュされている場合があるため、整合性を検証する
        needs_rebuild = False
        if self.__tansho_odds_test is None:
            needs_rebuild = True
        else:
            try:
                if len(self.__tansho_odds_test) == 0:
                    needs_rebuild = True
                else:
                    x_test = self.X_test
                    if len(self.__tansho_odds_test) != len(x_test) or not self.__tansho_odds_test.index.equals(x_test.index):
                        needs_rebuild = True
            except Exception:
                needs_rebuild = True

        if needs_rebuild:
            df = self.test_data
            odds = df.loc[:, ResultsCols.TANSHO_ODDS]
            # 念のため X_test index に合わせる
            try:
                odds = odds.reindex(self.X_test.index)
            except Exception:
                pass
            self.__tansho_odds_test = odds
        return self.__tansho_odds_test
