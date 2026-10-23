# 実験開始時点の作業状態（2026-10-23）

再開計画（`prompts/recovery_plan_20261022.md`）4.1「再現できる作業状態を保存する」の記録。
作成: `tools/record_repro_state.py`（コード・環境・データ台帳）、`tools/convert_horse_results_parquet.py`（horse_results の中間形式化）。

## コード

| ファイル | 内容 |
|---|---|
| `git_state.txt` | ブランチ `20260208`、HEAD `691e0bb`（2026-02-08 "Fix bet policy syntax"）と `git status` |
| `tracked_changes.patch` | HEAD に対する未コミット差分（追跡済み12ファイル、1.6MB） |
| `untracked_sources.tar.gz` / `.csv` | 未追跡かつ .gitignore 対象外の373ファイル（tools 30本、`modules/training/_calibration.py`、prompts・reports、`models/2026020x` の結果CSV/JSON 317件、images）と SHA-256 |

復元手順（2026-10-23 に一時 worktree で確認済み。代表ファイルが作業ツリーと一致）:

```bash
git worktree add --detach ../restore 691e0bba2cef4f43317f4e91619b787dc27542f8
cd ../restore
git apply --whitespace=nowarn <このフォルダ>/tracked_changes.patch
tar -xzf <このフォルダ>/untracked_sources.tar.gz
```

## 環境

`environment.json`・`pip_freeze.txt`。プロジェクトの `.venv`（Python 3.12.10）で取得。

- 主要ライブラリ: numpy 2.3.3、pandas 2.3.2、lightgbm 4.6.0、scikit-learn 1.7.2、optuna 4.5.0、scipy 1.16.2、pyarrow 22.0.0、dill 0.4.0
- マシン: Windows 11 Home 10.0.26200、i7-14700F（論理28）、RAM 31.7GiB、C: 空き 459GiB
- `requirements.txt` はバージョン未固定。比較実験中はこの `pip_freeze.txt` を基準にし、ライブラリ更新とモデル比較を同時に行わない

## データ台帳（`data_manifest.csv`）

626件。各ファイルのパス・役割（raw / backup / feature_cache / master / model / derived / html_cache）・サイズ・更新日時・SHA-256。2GiB 以下の pickle は行数・列数・インデックスのユニーク数・日付範囲も記録。

| 役割 | 件数 | 容量 |
|---|---:|---:|
| raw（`data/raw`） | 6 | 9.06 GiB |
| backup（`.bak` 等） | 18 | 9.11 GiB |
| feature_cache（`data/tmp/featured_data_*`） | 7 | 2.91 GiB |
| html_cache（`data/html` 3フォルダ、計157,060ファイル） | 3 | 13.50 GiB |
| model（`models/**/*.pickle`） | 429 | 61.17 GiB |

主な記録:

- `results.pickle`: 5,930,515行・38,338レース。`return_tables.pickle`: 304,986行・38,338レース。`race_info.pickle`: 38,452行（日付列は台帳の自動判定では日付として読めず、範囲は未記録）
- `featured_data_20260219.pickle`: 536,209行・282列、2015-01-04〜2026-02-15
- raw の原本と `.bak` はすべてハッシュが異なる（原本は 2026-02-19〜21 更新、`.bak` は 2026-02-04〜19）。どちらが監査前の原本かは更新日時と合わせて判断する
- `models/` は 61GiB で、`models/20260206` の1ファイルだけで約30GiB。容量整理の候補

## horse_results の中間形式化（未完了）

`tools/convert_horse_results_parquet.py` で `data/raw/horse_results.pickle`（8.1GiB）を `data/interim/horse_results.parquet`（zstd）に変換し、行数・馬数・日付範囲・重複件数・ピークメモリを `horse_results_conversion.json` に書く設計。

2026-10-23 の実行は、pickle の読み込み中に空き物理メモリが 1.2GiB まで下がり、安全装置（下限 1.5GiB）で中断した。出力ファイルは作られていない。開始時の空きは約 12.3GiB。

- 必要メモリは「空き 12GiB 超」。全件 pickle は分割して読めないため、ほかのアプリを閉じて空きを 20GiB 程度にしてから再実行する
- 再実行コマンド:

```bash
.venv/Scripts/python.exe tools/convert_horse_results_parquet.py --src data/raw/horse_results.pickle --out data/interim/horse_results.parquet --report reproducibility/20261023_baseline/horse_results_conversion.json
```

- 一度 Parquet にできれば、以降は必要な列だけ読み込める。`*.parquet` は `.gitignore` に追加済み

## ログ

`_manifest_run.log`（台帳作成）、`_horse_results_run.log`（変換の中断記録）。
