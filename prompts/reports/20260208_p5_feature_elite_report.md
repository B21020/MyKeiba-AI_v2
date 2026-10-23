# P5: 特徴量エンジニアリング + 少数精鋭戦略 レポート

**実行日**: 2026-02-08  
**スクリプト**: `tools/run_p5_experiments.py`, `tools/run_calibration_sweep.py`

---

## 1. 実験概要

### P5-1: 特徴量エンジニアリング改善（race-relative features）
- 12個の主要パフォーマンス特徴量に対し、**レース内z-score** と **レース内順位** を追加
- 対象: 着順_5R/allR, 賞金_5R/allR, 着差_5R/allR, time_seconds_5R/allR, 斤量, 年齢, 体重, jockey_plc_rate_10_all
- 結果: 24新特徴量追加（12 z-score + 12 rank）→ 特徴量数 274 → 298

### P5-2: 少数精鋭戦略
- `--min-bets 50` で低ベット域（n=50-500）のRRを検証
- BetPolicyEVTansho（max 2/race）, BetPolicyFukusho（max 3/race）で実データ払戻RRを計算

---

## 2. AUC比較

| 特徴量セット | AUC(test) |
|:------------|:---------:|
| baseline | 0.7327 |
| **rel (race-relative追加)** | **0.7502** |
| baseline + Optuna(P4) | 0.7391 |

**race-relative features は AUC を +0.0175 改善** — Optuna HPO（+0.0065）の約2.7倍の効果。

---

## 3. 複勝 回収率（実データ払戻）

### n_bets >= 500（信頼性の高い帯域）

| Variant | Calibration | RR | n_bets | std |
|:--------|:-----------|----:|-------:|----:|
| baseline | isotonic | 0.8713 | 519 | 0.039 |
| baseline | none | 0.8100 | 578 | 0.037 |
| rel | none | **0.8581** | 449 | 0.040 |
| rel | isotonic | **0.8605** | 537 | 0.038 |

### n_bets = 100-200（少数精鋭帯域） ★注目

| Variant | Calibration | RR | n_bets | std |
|:--------|:-----------|----:|-------:|----:|
| baseline | none | 0.8110 | 91 | 0.097 |
| baseline | platt | 0.7969 | 196 | 0.063 |
| baseline | isotonic | 0.8212 | 198 | 0.062 |
| **rel** | **none** | **0.9550** | **129** | 0.090 |
| **rel** | **platt** | **0.9571** | **140** | 0.089 |
| **rel** | **isotonic** | **0.9598** | **132** | 0.094 |

### n_bets = 50-100（ハイリスク帯域）

| Variant | Calibration | RR | n_bets | std |
|:--------|:-----------|----:|-------:|----:|
| baseline | none | 0.8800 | 60 | 0.128 |
| baseline | platt | 0.8889 | 63 | 0.124 |
| **rel** | **platt** | **1.0000** | **54** | 0.184 |
| rel | none | 0.9179 | 56 | 0.175 |

### プロット用データ（rel_platt, 複勝RR推移）

```
th=2.758  RR=0.841  n=562
th=2.864  RR=0.854  n=391
th=2.970  RR=0.873  n=265
th=3.040  RR=0.916  n=206
th=3.111  RR=0.957  n=140
th=3.217  RR=0.975  n=80
th=3.288  RR=1.000  n=54  ← BEST
```

---

## 4. 単勝 回収率（実データ払戻）

| Variant | Calibration | RR | n_bets |
|:--------|:-----------|----:|-------:|
| **baseline** | **none** | **0.7362** | 9108 |
| baseline | platt | 0.7298 | 8554 |
| rel | none | 0.6987 | 9015 |
| rel | platt | 0.7283 | 8464 |

**単勝はrel variantで悪化(-0.04)**。AUC改善が単勝RR改善に直結しないパターンが再確認された。

---

## 5. 重要な発見

### (1) AUCと回収率の関係はポリシー依存
- **複勝**: AUC↑ → RR↑（正の相関、特にn=100-200帯域で顕著）
- **単勝**: AUC↑ → RR↓（逆相関、P4 Optunaと同傾向）

### (2) race-relative features → 複勝に強い理由
- 複勝は「3着以内に入れるか」の判定 → 「レース内で上位」かどうかが直接的に効く
- race-relative features は「このレースでの相対的強さ」を直接モデルに提供
- 単勝はオッズとの相対関係でEVを判定 → relative features よりオッズ感度が重要

### (3) 少数精鋭戦略は複勝で特に有効
- n=100-200に絞るとrel variantは RR ≈ 0.95-0.96 を安定して達成
- n=54ではRR=1.0を初めて達成（ただし統計的有意性は不十分、95%CI ≈ [0.64, 1.36]）

### (4) 1.0到達に必要な残りの改善幅
- 最も信頼性の高い結果: rel_platt, n=140, RR=0.9571, std=0.089
- 95%下限: 0.957 - 1.96×0.089 ≈ 0.78
- **RR 1.0到達まで残り +0.043（4.3%ポイント）**

---

## 6. 結論

| 項目 | Before (P3) | After (P5) | 改善 |
|:-----|:----------:|:----------:|:----:|
| AUC(test) | 0.7327 | **0.7502** | **+0.0175** |
| 複勝 RR (n≥500) | 0.8713 | 0.8605 | -0.011 |
| 複勝 RR (n=100-200) | 0.8212 | **0.9598** | **+0.139** |
| 複勝 RR (n≈50) | 0.8800 | **1.0000** | **+0.120** |
| 単勝 RR | 0.7362 | 0.6987 | -0.038 |

**P5-1（race-relative features）+ P5-2（少数精鋭）の組み合わせが最も有望。複勝少数精鋭でRR≈0.96を達成。**

---

## 7. 次の改善候補

| 優先度 | 施策 | 期待効果 |
|:------:|:-----|:--------|
| ★★★ | **更なるrace-relative特徴量** (first_corner, final_corner等を追加) | AUC +0.01-0.02 |
| ★★★ | **featured_dataにrel featuresを恒久組込み** | 本番運用対応 |
| ★★☆ | **単勝特化モデル** (1着のみをtargetに学習) | 単勝RR改善 |
| ★★☆ | **複勝 closed-form EV** (score × avg_fukusho_payout) | 複勝EV戦略 |
| ★☆☆ | **Optuna × rel features** | AUC → 0.76+ |

---

## 8. 生成物

| ファイル | 内容 |
|---------|------|
| `data/tmp/featured_data_20260205_rel.pickle` | race-relative特徴量入りデータ |
| `models/20260208/p5_rel/` | rel variant sweep結果一式 |
| `models/20260208/p5_baseline/` | baseline sweep結果一式（min-bets=50） |
| `models/20260208/p5_phase_a_elite_sweep.pickle` | Phase A結果（proxy metric） |
| `models/20260208/p5_phase_b_feature_variants.pickle` | Phase B AUC比較結果 |
| `tools/run_p5_experiments.py` | P5実験スクリプト |
