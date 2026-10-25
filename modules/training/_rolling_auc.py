import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import roc_auc_score

from modules.constants import ResultsCols


DEFAULT_DROP_COLS = [
    'rank',
    'win',
    'longshot',
    'date',
    ResultsCols.TANSHO_ODDS,
    ResultsCols.UMABAN,
    'race_id',
]


def _race_id_series(df: pd.DataFrame) -> pd.Series:
    """各行の race_id を返す（列があれば列優先）。"""
    if 'race_id' in df.columns:
        return df['race_id']
    if isinstance(df.index, pd.MultiIndex):
        return pd.Series(df.index.get_level_values(0), index=df.index)
    return pd.Series(df.index, index=df.index)


def _date_per_race(df: pd.DataFrame) -> pd.Series:
    """race_id -> date の代表値（最初のdate）を作る。"""
    date_series = df['date']
    race_ids = _race_id_series(df)
    # groupbyにSeriesを渡すと同一indexでグルーピングできる
    return date_series.groupby(race_ids).first()


def _subset_by_race_ids(df: pd.DataFrame, race_ids) -> pd.DataFrame:
    """race_id の集合で DataFrame を抽出（index/列どちらでも対応）。"""
    # 最後の手段（最も安全）: race_id列/疑似race_id系列で boolean mask
    # NOTE: indexベースの .loc は、indexの意味が race_id でない場合に
    #       意図せず大量抽出になることがあるため、こちらを正とする。
    race_id_s = _race_id_series(df)

    # 型不一致（int/str）で isin が外れるのを避ける
    try:
        race_ids_set = set(map(str, list(race_ids)))
        mask = race_id_s.astype(str).isin(race_ids_set)
    except Exception:
        mask = race_id_s.isin(set(race_ids))

    return df.loc[mask]


def _nunique_races_in_df(df: pd.DataFrame) -> int:
    try:
        return int(_race_id_series(df).nunique())
    except Exception:
        return int(pd.Series(_race_id_series(df)).nunique())


def rolling_time_series_auc(
    featured_data: pd.DataFrame,
    *,
    params: dict | None = None,
    n_splits: int = 5,
    test_size: float = 0.15,
    valid_size: float = 0.2,
    gap_races: int = 0,
    seed: int = 42,
    target_col: str = 'rank',
    drop_cols: list | None = None,
    stopping_rounds: int = 50,
) -> pd.DataFrame:
    """時系列の複数分割（ローリング）で AUC のブレを評価する。

    分割の考え方:
    - race_id を代表日付で昇順ソート
    - 末尾から `test_size` 分の race_id をテスト期間として取り、
      その直前（必要なら gap_races 分空けた）までを学習期間とする
    - テスト期間を過去方向にスライドして `n_splits` 回繰り返す

    戻り値:
    - fold ごとの train/test 期間・件数・AUC を持つ DataFrame
    """
    if not isinstance(featured_data, pd.DataFrame):
        raise TypeError('featured_data must be a pandas DataFrame')

    if drop_cols is None:
        drop_cols = list(DEFAULT_DROP_COLS)

    df = featured_data

    feature_cols = [c for c in df.columns if c not in drop_cols]
    if len(feature_cols) == 0:
        raise ValueError('No feature columns left after drop_cols')

    # race_id 単位で分割したいので、ここは必ずユニークレースで作る
    date_per_race = _date_per_race(df).sort_values()
    race_ids = date_per_race.index.to_list()
    n_total = len(race_ids)
    if n_total < 10:
        raise ValueError(f'Not enough races for rolling split: {n_total}')

    if not (0 < test_size < 1):
        raise ValueError('test_size must be a fraction between 0 and 1')
    if not (0 < valid_size < 1):
        raise ValueError('valid_size must be a fraction between 0 and 1')

    test_n = max(1, int(round(n_total * test_size)))

    results: list[dict] = []
    end = n_total

    for fold in range(1, n_splits + 1):
        test_start = end - test_n
        if test_start <= 0:
            break

        train_end = max(0, test_start - int(gap_races))
        train_ids_all = race_ids[:train_end]
        test_ids = race_ids[test_start:end]

        if len(train_ids_all) < max(20, test_n * 2):
            break

        # train内で valid を末尾から分割（時系列）
        valid_n = max(1, int(round(len(train_ids_all) * valid_size)))
        if len(train_ids_all) - valid_n <= 0:
            break

        train_ids = train_ids_all[:-valid_n]
        valid_ids = train_ids_all[-valid_n:]

        train_df = _subset_by_race_ids(df, train_ids)
        valid_df = _subset_by_race_ids(df, valid_ids)
        test_df = _subset_by_race_ids(df, test_ids)

        # 抽出後の実ユニークrace数（抽出が正しく行われているかの監視）
        train_races_selected = _nunique_races_in_df(train_df)
        valid_races_selected = _nunique_races_in_df(valid_df)
        test_races_selected = _nunique_races_in_df(test_df)

        X_train = train_df[feature_cols]
        y_train = train_df[target_col]
        X_valid = valid_df[feature_cols]
        y_valid = valid_df[target_col]
        X_test = test_df[feature_cols]
        y_test = test_df[target_col]

        # 行数（馬単位）と陽性率（ラベル比率）も記録して、期間差/分布差を把握する
        try:
            train_rows = int(len(y_train))
            valid_rows = int(len(y_valid))
            test_rows = int(len(y_test))
        except Exception:
            train_rows = valid_rows = test_rows = None

        try:
            rows_per_race_test = float(test_rows / test_races_selected) if test_races_selected > 0 else None
        except Exception:
            rows_per_race_test = None

        # test内の「1レースあたり行数」分布（重複混入の検知）
        max_rows_per_race_test = None
        p95_rows_per_race_test = None
        race_id_max_rows_test = None
        try:
            test_race_ids = _race_id_series(test_df).astype(str)
            per_race_counts = test_race_ids.value_counts()
            if len(per_race_counts) > 0:
                max_rows_per_race_test = int(per_race_counts.max())
                p95_rows_per_race_test = float(np.percentile(per_race_counts.to_numpy(), 95))
                race_id_max_rows_test = str(per_race_counts.idxmax())
        except Exception:
            pass

        def _mean_safe(s):
            try:
                return float(pd.Series(s).mean())
            except Exception:
                return None

        pos_rate_train = _mean_safe(y_train)
        pos_rate_valid = _mean_safe(y_valid)
        pos_rate_test = _mean_safe(y_test)

        model = lgb.LGBMClassifier(objective='binary', random_state=seed)
        if params:
            model.set_params(**params)

        best_iteration = None
        # early stopping が使える場合は有効化
        try:
            model.fit(
                X_train,
                y_train.values,
                eval_set=[(X_valid, y_valid.values)],
                eval_metric='auc',
                callbacks=[lgb.early_stopping(stopping_rounds=stopping_rounds, verbose=False)],
            )
            best_iteration = getattr(model, 'best_iteration_', None)
        except Exception:
            model.fit(X_train, y_train.values)

        proba_train = model.predict_proba(X_train)[:, 1]
        proba_test = model.predict_proba(X_test.reindex(columns=X_train.columns, fill_value=0))[:, 1]

        auc_train = roc_auc_score(y_train, proba_train)
        auc_test = roc_auc_score(y_test, proba_test)

        train_start_date = pd.to_datetime(date_per_race.loc[train_ids].min())
        train_end_date = pd.to_datetime(date_per_race.loc[train_ids].max())
        test_start_date = pd.to_datetime(date_per_race.loc[test_ids].min())
        test_end_date = pd.to_datetime(date_per_race.loc[test_ids].max())

        try:
            test_days = int((test_end_date - test_start_date).days) + 1
        except Exception:
            test_days = None

        try:
            if test_days is not None and test_days > 0:
                test_races_per_year = float(len(test_ids) / test_days * 365.25)
            else:
                test_races_per_year = None
        except Exception:
            test_races_per_year = None

        results.append(
            {
                'fold': fold,
                'train_races': len(train_ids),
                'valid_races': len(valid_ids),
                'test_races': len(test_ids),
                'train_races_selected': train_races_selected,
                'valid_races_selected': valid_races_selected,
                'test_races_selected': test_races_selected,
                'train_rows': train_rows,
                'valid_rows': valid_rows,
                'test_rows': test_rows,
                'rows_per_race_test': rows_per_race_test,
                'p95_rows_per_race_test': p95_rows_per_race_test,
                'max_rows_per_race_test': max_rows_per_race_test,
                'race_id_max_rows_test': race_id_max_rows_test,
                'pos_rate_train': pos_rate_train,
                'pos_rate_valid': pos_rate_valid,
                'pos_rate_test': pos_rate_test,
                'test_days': test_days,
                'test_races_per_year': test_races_per_year,
                'train_start': train_start_date,
                'train_end': train_end_date,
                'test_start': test_start_date,
                'test_end': test_end_date,
                'auc_train': float(auc_train),
                'auc_test': float(auc_test),
                'gap': float(auc_train - auc_test),
                'best_iteration': None if best_iteration is None else int(best_iteration),
            }
        )

        end = test_start

    if len(results) == 0:
        raise ValueError('No splits were generated. Try reducing n_splits/test_size/gap_races.')

    out = pd.DataFrame(results)
    out['auc_test_mean'] = out['auc_test'].mean()
    out['auc_test_std'] = out['auc_test'].std(ddof=0)
    out['auc_test_min'] = out['auc_test'].min()
    out['auc_test_max'] = out['auc_test'].max()
    return out
