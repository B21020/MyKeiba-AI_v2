"""学習時の特徴量定義（列名・順序・型・カテゴリ）を保存し、予測時に照合する。

再開計画 段階1：存在しない列を黙ってゼロ埋めしない。要求列が欠けていれば処理を止める。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

SCHEMA_VERSION = 1


class FeatureSchemaError(ValueError):
    """予測データの列が学習時の定義と一致しない。"""


def build_schema(X: pd.DataFrame) -> dict:
    """学習に使った X から特徴量定義を作る。"""
    columns = [str(c) for c in X.columns]
    if len(set(columns)) != len(columns):
        dup = sorted({c for c in columns if columns.count(c) > 1})
        raise FeatureSchemaError(f'duplicated feature columns: {dup[:10]}')
    categories = {
        str(c): [str(v) for v in X[c].cat.categories]
        for c in X.columns if isinstance(X[c].dtype, pd.CategoricalDtype)
    }
    schema = {
        'schema_version': SCHEMA_VERSION,
        'n_features': len(columns),
        'columns': columns,
        'dtypes': {str(c): str(t) for c, t in X.dtypes.items()},
        'categories': categories,
    }
    schema['fingerprint'] = fingerprint(schema)
    return schema


def fingerprint(schema: dict) -> str:
    """列名・順序・型から作る短いハッシュ。比較するモデル同士で入力定義が同じかを確かめる。"""
    payload = json.dumps({'columns': schema['columns'], 'dtypes': schema['dtypes']}, ensure_ascii=False)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def save_schema(schema: dict, path: str | Path) -> None:
    Path(path).write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding='utf-8')


def load_schema(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding='utf-8'))


def align_to_schema(X: pd.DataFrame, schema: dict, *, allow_extra: bool = True) -> pd.DataFrame:
    """X を学習時の列順に並べる。要求列が欠けていれば FeatureSchemaError。

    - allow_extra=True: 余分な列（race_id 等のメタ列）は落とす
    - カテゴリ列は学習時のカテゴリ順にそろえる（未知の値は欠損になる）
    """
    columns = schema['columns']
    missing = [c for c in columns if c not in X.columns]
    if missing:
        raise FeatureSchemaError(
            f'{len(missing)} required feature columns are missing (e.g. {missing[:10]}); '
            'rebuild features with the same pipeline as the model'
        )
    extra = [c for c in X.columns if c not in set(columns)]
    if extra and not allow_extra:
        raise FeatureSchemaError(f'unexpected columns: {extra[:10]}')
    out = X.loc[:, columns].copy()
    for c, cats in schema.get('categories', {}).items():
        out[c] = pd.Categorical(out[c].astype(str).where(out[c].notna()), categories=cats)
    return out
