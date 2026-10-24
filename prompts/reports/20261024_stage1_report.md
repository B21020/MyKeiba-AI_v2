# 段階1：前処理・分割・列定義の修正と特徴量の作り直し

対象計画：[再開計画 3・7節 段階1](../recovery_plan_20261022.md)。根拠は [段階0監査](20261024_stage0_audit.md)。実施日：2026年10月24日。

## 状態

**完了（2026-10-24 10:38）**。特徴量を作り直し、すべての確認項目を満たした。

| 項目 | 結果 |
|---|---|
| 出力 | `data/tmp/featured_data_20261024_stage1.pickle`（550,493行・342列、うち特徴量336列） |
| 列定義 | `reproducibility/20261024_stage1/feature_schema.json`（fingerprint `f50b4a8cddb0b8aa`） |
| 入力ハッシュ・検証結果 | `reproducibility/20261024_stage1/manifest.json`（`problems` は空） |
| 対象 | 段階0の学習対象39,322レース、2015-01-04〜2026-10-18 |
| 「race_id×馬番」の重複 | 0 |
| 単勝・枠番・体重の欠損 | 全年0%（体重変化は0.06%以下） |
| P11の145列 | すべて存在 |
| ラベル | 3着以内 118,065、1着 39,382（1着同着60レース分を含む） |

### 馬戦績は原本 pickle ではなく HTML から作り直した

`data/raw/horse_results.pickle`（8.1GiB）は、空きメモリ18GiBでも読み込み中に空きが1.2GiBまで下がり、安全装置で中断した（`_prepare_run.log`）。そこで、その元になった馬ページのHTML（`data/html/horse`）を20並列で読み直した（`tools/rebuild_horse_results_from_html.py`、約25分）。

- 4.2差分にある10,371頭は差分（列ずれ修正後）を使い、残り52,332頭はHTMLから読んだ。HTMLの読み込み失敗は0件。
- 結果：1,426,821行・62,703頭。**結果データに出てくる馬62,703頭すべてに戦績がある**。中間ファイルは355MB（`data/interim/horse_results_merged.pickle`、記録は `horse_results_prepare.json`）。
- 原本 pickle との行単位の突き合わせはしていない（読めないため）。

### 旧キャッシュとの比較（`featured_data_20260219.pickle`）

| 項目 | 旧 | 新 |
|---|---|---|
| 体重・枠番・単勝（2015〜2019、2026） | 欠損100% | 欠損0% |
| 過去成績がある行の割合（2015年） | 60% | 91% |
| 騎手成績がある行の割合（2015年） | 66% | 100% |
| 2020〜2025年で値が一致した割合：`賞金_allR`（全戦の平均） | — | 99.8% |
| 同：`着順_5R`（直近5走の平均） | — | 74.0% |

全戦の平均はほぼ一致し、直近5走の平均だけが4分の1で食い違う。旧キャッシュの直近5走の平均着順は新より高く（例：2020年 7.2 対 6.8）、走破時計の平均も長い。旧の馬戦績に同じレースの行が繰り返し含まれていて、「直近5走」が同じレースの複製で埋まっていた場合にこの形になる。数値からの推定で、原本を読めないため行単位では確かめていない。いずれにしても、**過去の実験は直近N走の特徴量がこの新しいキャッシュと異なる条件で行われた**。

## 修正したこと

| 項目 | 変更 | ファイル |
|---|---|---|
| 結果の重複 | 全列一致の重複行を最初に集約 | `modules/preprocessing/_results_processor.py` |
| 列名の新旧混在 | `単勝`・`枠番`・`馬体重`・`調教師` が欠損なら `単勝オッズ`・`枠`・`馬体重(増減)`・`厩舎` で埋める | 同上 `coalesce_result_columns` |
| 着順コード | `取消`・`取`・`除外`・`除` は除去、`N(降)` は降着後の着順、`中止`・`中`・`失格`・`失` は着外 | 同上 `parse_finish_code` |
| 学習と検証の重なり | `DataSplitter`・`WinDataSplitter` の `X_train` から early stopping 用の検証期間を除いた。`sample_weight_train` も同じ行に合わせた | `modules/training/_data_splitter.py`、`_win_data_splitter.py` |
| 日付での分割 | `split_dates={'train_start','valid_start','test_start','test_end'}` で日付の境界を指定できる。レース単位で分割し、空の区分や順序違いは停止 | 同上 `split_race_ids_by_dates` |
| 列定義の保存 | 学習時に列名・順序・型・カテゴリをモデルに保存（`mykeiba_feature_schema_`）。指紋（fingerprint）で比較するモデル同士の入力定義の一致を確かめられる | `modules/training/_feature_schema.py`、`_model_wrapper.py` |
| 列定義の照合 | 列定義を持つモデルの予測は、欠けた列があれば `FeatureSchemaError` で停止。ゼロ埋めしない。欠損値もゼロにしない（学習時と同じ扱い） | `modules/policies/_score_policy.py` |
| 4.2差分の列ずれ（新たに判明） | 公開版の戦績表は伏せ字の有料列のため「通過」以降が4列右にずれていた（差分117,712行すべて）。本来の列に戻す関数を追加し、4.2の取得処理にも適用 | `modules/preparing/_horse_history_columns.py`、`tools/update_race_results.py` |
| 馬戦績の中間ファイル | 原本を一度だけ読み、全列一致の重複を除き、4.2差分（列ずれ修正後）で対象馬を置換、10月21日より後を除いて保存 | `tools/prepare_horse_results.py` |
| 特徴量の作成手順の統一 | main.ipynb のセル38〜44と同じ処理に、レース内相対特徴量（`_zR`・`_rnkR`）を追加。対象は段階0の学習対象レース。既存の `data/tmp` のファイルは上書きしない | `tools/build_features.py` |

### 4.2差分の列ずれについて

旧スクレイパーが保存したHTML（`data/html/horse`）を同じ方法で読むと、列はずれていない（29列で「通過」に通過順が入る）。ずれているのは4.2で公開の戦績断片から取得した差分だけ。修正後の検証で、通過順・ペース・馬体重の形式はすべて一致した。この差分を修正前のまま特徴量に使うと、通過順・上り・馬体重・賞金が誤った値になっていた。

### 既存の実験スクリプトへの影響

- `X_train` が学習期間だけになったため、同じ比率指定でも学習に使う行が減る。過去の P5〜P15 の数値は、この修正前の条件の結果として扱う。
- 列定義を持つモデル（今後学習するもの）は予測時に厳密照合される。過去に保存したモデルは従来の処理のまま（列のゼロ埋めあり）。

## テスト

`tests/test_stage1_pipeline.py`（9件）：着順コード、列名の統合、重複除去と取消・除外の除去、日付分割の境界、`X_train` と検証の非重複（比率指定・日付指定の両方、`WinDataSplitter` も）、列定義の照合（欠落で停止・順序・NaN維持・カテゴリ）、列ずれの修正と未変更の行。

既存のテストと合わせて38件すべて通過。

**動作確認（一部期間）**：馬戦績に4.2差分（列ずれ修正後）だけを使い、2026-09-05〜10-18の416レースで `build_features.py` を最後まで実行した（`--date-from/--date-to`）。5,568行・342列（`rel_ext` 版と同じ列数）、「race_id×馬番」の重複0、P11の145列すべて存在、`単勝`・`枠番`・`体重` の欠損0%・`体重変化` 0.04%。過去成績の特徴量（`着順_5R` 等）は83.7%の行にあり（残りは初出走など）、通過順・上り・賞金の値の範囲も妥当だった。全期間の作り直しではない。

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_stage1_pipeline tests.test_odds_freshness_probe tests.test_public_odds tests.test_odds_snapshots tests.test_update_race_results
```

## 再実行の手順

```powershell
# 馬戦績（HTML から。約25分。原本 pickle を読める環境なら tools\prepare_horse_results.py でもよい）
.\.venv\Scripts\python.exe tools\rebuild_horse_results_from_html.py --out data\interim\horse_results_merged.pickle --report reproducibility\20261024_stage1\horse_results_prepare.json --workers 20
# 特徴量（約5分）。鮮度計測と重ねないため、取得時間帯は一時停止するラッパーで実行した
.\.venv\Scripts\python.exe tools\run_paused_during.py --pause 12:33-13:05 --pause 14:18-15:45 -- .venv\Scripts\python.exe tools\build_features.py --tag 20261024_stage1
```

完了条件は `manifest.json` の `problems` が空であること（重複0、主要列の欠損率が各年2%以下、P11の145列が存在）。

## 残ること

- 馬戦績の重複監査（中間ファイル作成時に件数を `horse_results_prepare.json` に記録する）。
- 2026年に初出走の馬は、`horse_info`・`peds`（2月時点の取得）が無く欠損になる。必要なら追加取得を検討する。
- 確率補正・購入条件選択の区分を含む共通評価runner（計画5.1）は次の段階で作る。今回の `split_dates` は学習・検証・テストの3区分まで。
