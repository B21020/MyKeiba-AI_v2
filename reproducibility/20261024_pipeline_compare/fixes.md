# 古いパイプラインを動かすための最小限の対処

比較の結果：`prompts/reports/20261024_pipeline_compare.md`。各パイプラインの処理の論理は変えず、動かすために必要なものだけを記録する。

| 対象 | 対処 | 理由 |
|---|---|---|
| 共通の入力（結果） | 全列一致の重複を除き、旧名・新名の列を統合（`coalesce_result_columns`） | 今日の raw は同じ行が最大約10回連結され、2015〜2019年と2026年の一部は新名の列だけを持つ。オリジナルは新名を読めず、重複で頭数（n_horses）が約10倍になる |
| 共通の入力（結果） | `単勝` の `'---'`（取消・除外の馬、旧形式の行にもある）を欠損に | オリジナルの `astype(float)` が止まる。2月・現在は `pd.to_numeric(errors='coerce')` で同じ結果 |
| 共通の入力（馬戦績） | `data/interim/horse_results_merged.pickle`（HTML から作り直し、重複なし）を使用 | 原本 pickle は 8.1GiB で読めず、同じ走の重複で直近N走の集計が壊れる |
| 共通の入力 | 2026-07-01 以降のレース・戦績を除外 | 最終確認期間を読まない |
| オリジナルの実行環境 | 別の仮想環境に `optuna-integration[lightgbm]` 5.0.0（他は本体と同じ版） | `optuna.integration.lightgbm` が必要。本体の `.venv` は変えない |
| オリジナル | `modules/preparing` を import しない | numpy 2 で `numpy.NaN` が無い。学習には不要 |
| オリジナルの評価 | 予測を学習時と同じ `.values` で行う | オリジナルは `.values`（カテゴリはエンコード値）で学習し、DataFrame（カテゴリはコード）で予測する食い違いがある。学習自体は変えない |
| 2月 | 投票用モデルの手順（flow c）で再現：P11 の145列のうち存在する列＋システム列、`models/20260212/p11_ext/optuna_best_params.json`、`train_without_tuning()` | 2月に実際に使った手順 |
| 両方 | `data/master/*.csv` は worktree に複製 | ID 符号化が読み書きする。本体の master は変えない |
