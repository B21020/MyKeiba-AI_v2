"""古いパイプライン（オリジナル・2月）に渡す共通の入力を作り、git worktree の data/ に置く。

各パイプラインの処理ロジックは変えず、今日のデータを各パイプラインが期待する形式にそろえる:
- 結果：全列一致の重複を除き、旧名・新名の列を統合（coalesce_result_columns）。index は名前なし（オリジナルの _sort が 'index' 列を使うため）
- 馬戦績：data/interim/horse_results_merged.pickle（HTML から作り直したもの）
- race_info・return_tables・horse_info・peds：data/raw/ のもの
- 最終確認期間（FINAL_PERIOD_START 以降）のレース・戦績は入れない
- data/master/*.csv は複製（ID 符号化が読み書きするため。本体の master は触らない）

使い方:
  .venv/Scripts/python.exe tools/pipeline_compare/prepare_inputs.py --dest <worktree>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START  # noqa: E402
from modules.preprocessing._results_processor import coalesce_result_columns  # noqa: E402


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while block := f.read(16 * 1024**2):
            h.update(block)
    return h.hexdigest()


def prepare_results(raw: pd.DataFrame, keep_ids: set[str]) -> pd.DataFrame:
    """重複除去・列統合・対象レースへの絞り込み。index は名前なしの race_id。"""
    df = raw[~raw.reset_index().duplicated().to_numpy()]
    df = coalesce_result_columns(df)
    # 取消・除外の馬の単勝は '---'（旧形式の行にもある）。オリジナルは astype(float) で止まるので欠損にそろえる。
    # 2月・現在の処理は pd.to_numeric(errors='coerce') で同じ結果になる
    df['単勝'] = pd.to_numeric(df['単勝'], errors='coerce')
    df = df[df.index.astype(str).isin(keep_ids)]
    df.index.name = None
    return df


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dest', required=True, help='worktree のパス')
    args = ap.parse_args()
    dest = Path(args.dest)
    raw_dir, master_dir = dest / 'data' / 'raw', dest / 'data' / 'master'
    for d in (raw_dir, master_dir, dest / 'data' / 'tmp'):
        d.mkdir(parents=True, exist_ok=True)

    ri = pd.read_pickle(ROOT / 'data/raw/race_info.pickle')
    dates = pd.to_datetime(ri['date'], format='%Y年%m月%d日')
    ri = ri[dates < FINAL_PERIOD_START]
    keep_ids = set(ri.index.astype(str))
    manifest = {'final_period_start': str(FINAL_PERIOD_START.date()), 'files': {}}

    res = prepare_results(pd.read_pickle(ROOT / 'data/raw/results.pickle'), keep_ids)
    manifest['results_rows'] = len(res)
    manifest['results_races'] = int(res.index.nunique())
    manifest['results_dup_race_umaban'] = int(res.reset_index().duplicated(['index', '馬番']).sum())
    res.to_pickle(raw_dir / 'results.pickle')
    ri.to_pickle(raw_dir / 'race_info.pickle')
    rt = pd.read_pickle(ROOT / 'data/raw/return_tables.pickle')
    rt[rt.index.astype(str).isin(keep_ids)].to_pickle(raw_dir / 'return_tables.pickle')
    hr = pd.read_pickle(ROOT / 'data/interim/horse_results_merged.pickle')
    hr = hr[pd.to_datetime(hr['日付'], errors='coerce') < FINAL_PERIOD_START]
    manifest['horse_results_rows'] = len(hr)
    hr.to_pickle(raw_dir / 'horse_results.pickle')
    for name in ('horse_info.pickle', 'peds.pickle'):
        shutil.copy2(ROOT / 'data/raw' / name, raw_dir / name)
    for f in (ROOT / 'data/master').glob('*.csv'):
        shutil.copy2(f, master_dir / f.name)
    for f in sorted(raw_dir.glob('*.pickle')):
        manifest['files'][f.name] = {'bytes': f.stat().st_size, 'sha256': sha256(f)}
    (dest / 'pipeline_inputs_manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in manifest.items() if k != 'files'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
