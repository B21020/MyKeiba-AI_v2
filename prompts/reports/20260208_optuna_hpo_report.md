# P4: Optuna ハイパーパラメータ最適化レポート

**実行日**: 2026-02-08  
**スクリプト**: `tools/run_optuna_tuning.py`  
**所要時間**: 約17分 (100 trials)

---

## 1. 実験設定

| 項目 | 値 |
|------|-----|
| Sampler | TPESampler (seed=42) |
| Trials | 100 |
| 目的関数 | AUC(validation) 最大化 |
| 探索パラメータ数 | 11 |
| 訓練データ | 2015-01-04 ~ 2024-06-30 (全期間) |
| 検証データ | DataSplitter内蔵 validation split |
| テストデータ | 2024-07-01以降 (5,312レース) |

### 探索パラメータ範囲

| Parameter | Range | Best | Default |
|-----------|-------|------|---------|
| num_leaves | 15–300 | **222** | 31 |
| max_depth | 3–12 | **4** | -1 (無制限) |
| learning_rate | 0.005–0.3 (log) | **0.0322** | 0.1 |
| n_estimators | 100–1500 | **700** | 100 |
| min_child_samples | 5–200 | **91** | 20 |
| subsample | 0.5–1.0 | **0.989** | 1.0 |
| subsample_freq | 0–7 | **4** | 0 |
| colsample_bytree | 0.2–1.0 | **0.321** | 1.0 |
| reg_alpha | 1e-8–10.0 (log) | **0.0011** | 0.0 |
| reg_lambda | 1e-8–10.0 (log) | **~0** | 0.0 |
| min_split_gain | 0.0–1.0 | **0.734** | 0.0 |

---

## 2. 最適化結果

- **Best Trial**: #82 / 100
- **Best AUC(valid)**: 0.747011
- **Default AUC(valid)**: 0.741165
- **改善幅**: +0.0058

### AUC推移
Trial 15で0.7459に到達後、Trial 55で0.7467、Trial 82で最終的に0.7470まで微増。  
収束傾向が見られ、100 trialsでほぼ飽和。

---

## 3. テストセット比較（核心結果）

| 指標 | Default | Optuna Best | 差分 |
|------|---------|-------------|------|
| **AUC(test)** | 0.7327 | **0.7391** | **+0.0065** |
| **単勝 回収率** | **0.7362** | 0.7019 | **-0.0344** |
| 単勝 賭け数 | 9,108 | 9,062 | -46 |
| 単勝 stderr | 0.0468 | 0.0410 | -0.0058 |
| **複勝 回収率** | **0.8609** | 0.8580 | **-0.0029** |
| 複勝 賭け数 | 777 | 557 | -220 |
| 複勝 stderr | 0.0350 | 0.0358 | +0.0007 |
| Train AUC | 0.838 | 0.807 | -0.031 |

### 重要な発見

1. **AUCは改善したが、回収率は悪化した**  
   - テストAUC: 0.7327 → 0.7391 (+0.65%)
   - 単勝回収率: 0.7362 → 0.7019 (-4.7%)
   - 複勝回収率: 0.8609 → 0.8580 (-0.3%)

2. **AUCと回収率の非単調関係**  
   AUCの改善が直接的に回収率改善に結びつかないことが実証された。Optuna最適化モデルはランキング精度は高いが、オッズとの相対関係（＝期待値）では劣る。

3. **Optunaモデルの特性**  
   - `max_depth=4` + `min_split_gain=0.73` + `colsample_bytree=0.32` → 強い正則化
   - Train AUC 0.807 vs Default 0.838 → 過学習は抑制
   - しかし、予測値の分布が保守的になり、高確率馬の確信度が下がった可能性

---

## 4. ハイパーパラメータ重要度 (fANOVA)

Optuna内蔵のfANOVA分析で算出。最もAUCに影響したHP:

1. **colsample_bytree** — 特徴量サブサンプリング率
2. **learning_rate** — 学習率
3. **n_estimators** — ブースティング回数数
4. **min_split_gain** — 分割ゲインの最小値

→ 正則化系パラメータが上位を占める。

---

## 5. 特徴量重要度 Top10 (Optuna Best Model)

| 順位 | 特徴量 | Importance (split) |
|------|--------|-------------------|
| 1 | horse_id | 713 |
| 2 | peds_31 | 461 |
| 3 | peds_47 | 238 |
| 4 | jockey_id | 154 |
| 5 | peds_55 | 141 |
| 6 | peds_61 | 135 |
| 7 | peds_59 | 119 |
| 8 | trainer_id | 107 |
| 9 | n_horses | 67 |
| 10 | owner_id | 56 |

→ ID系（horse_id, jockey_id, trainer_id, owner_id）と血統（peds_*）が支配的。  
→ 着順・賞金等のパフォーマンス特徴量は下位。

---

## 6. 結論と次ステップ

### 結論
- **Optunaによるハイパーパラメータ最適化は AUC を微改善（+0.65%）するが、回収率改善には寄与しない**
- 回収率のボトルネックはハイパーパラメータではなく、特徴量 or 予測→賭け戦略の変換ロジックにある
- 100 trials で十分に収束しており、trials数増加による追加改善は期待薄

### 次に進むべきP4候補

| 優先度 | 施策 | 期待効果 |
|--------|------|----------|
| ★★★ | **特徴量エンジニアリング改善** | horse_id依存を減らし汎化性向上 |
| ★★★ | **少数精鋭戦略 (n_bets=100-500)** | 高確信度ベットのみの回収率検証 |
| ★★☆ | **オッズフィルタ/レースフィルタ** | 低オッズ・少頭数の除外 |
| ★★☆ | **目的関数変更** | AUCではなく回収率直接最適化 |
| ★☆☆ | **アンサンブル** | 複数モデルの予測平均 |

---

## 7. 生成物

| ファイル | 内容 |
|---------|------|
| `models/20260208/optuna_study.pickle` | Optuna Study オブジェクト |
| `models/20260208/optuna_best_params.json` | 最適パラメータ |
| `models/20260208/optuna_best_feature_importance.csv` | 特徴量重要度 |
| `models/20260208/optuna_comparison_summary.pickle` | 比較結果 |
| `images/optuna_trial_history.png` | Trial 推移グラフ |
| `images/optuna_hp_importance.png` | HP重要度グラフ |
| `images/optuna_comparison_bar.png` | Default vs Optuna 比較 |
| `images/optuna_feature_importance_top20.png` | 特徴量重要度 Top20 |
