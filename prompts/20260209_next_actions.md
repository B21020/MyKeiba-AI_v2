# 次のアクションToDo — P6完了、P7計画

## P6 完了サマリ ✅

### 実施内容
1. ✅ race-relative features恒久組込み (`FeatureEngineering.add_race_relative_features()`)
2. ✅ 単勝特化モデル (`WinDataSplitter`, `--target win`)
3. ✅ 複勝EV戦略 (`BetPolicyEVFukusho`: score × odds ≥ threshold)
4. ✅ rel features拡張 (12→30カラム, 60新特徴量)
5. ✅ データバグ修正 (`_ensure_win_column` merge重複 534K→5.9M膨張を修正)

### P6 結果（修正版、テスト73,208レコード）

| Model × Features | AUC | 単勝edge RR (n) | 複勝 RR (n) |
|---|---|---|---|
| baseline_rank | 0.7327 | 0.686 (9,560) | 0.889 (63) |
| **rel_ext_rank** | **0.7479** | 0.700 (9,802) | **0.991 (188)** |
| baseline_win | 0.7462 | **0.916 (927)** | 0.902 (123) |
| **rel_ext_win** | **0.7556** | 0.706 (1,275) | **1.000 (66)** |

### 重要知見
- **win model の edge 戦略で単勝RR 0.69→0.92** (大幅改善、P5比+0.22)
- **rel_ext + isotonic で複勝RR 0.991 (n=188)** — 最も安定した高RR
- **最適はモデル分離**: 単勝→win model / 複勝→rank model (rel_ext+isotonic)
- 複勝EV戦略(score×odds)は threshold-only に劣後 → 複勝オッズ直接取得が必要
- テスト期間2025-02月～の約1年間、win=38,187件, rank(3着以内)=98,969件

### 現状サマリ

| 指標 | P3校正 | P5 rel | **P6** |
|---|---|---|---|
| AUC(test) rank | 0.733 | 0.750 | **0.748** (rel_ext) |
| AUC(test) win | — | — | **0.756** (rel_ext) |
| 単勝RR best | 0.736 | 0.699 | **0.916** (win/edge) |
| 複勝RR (n≥100) | 0.871 | 0.955 | **0.970** (n=260) |
| 複勝RR (n≥50) | — | 1.000 (n=54) | **0.991** (n=188) |

---

## P7 計画（優先順）

### ★★★ P7-1: Optuna HPO 再実行（win model + rel_ext）
- P4では rank model のみ HPO し効果薄だったが、win model + rel_ext は状況が異なる
- 特に edge 閾値域（0.14-0.15）での RR 感度が高く、微小なAUC改善が RR>1.0 に直結する可能性
- 実行: `run_calibration_sweep.py --target win` で HPO 後モデルの edge sweep

### ★★★ P7-2: Optuna HPO（rel_ext_rank + isotonic, 複勝特化）
- 現状 RR=0.991 (n=188) → 1.0超えまであと0.009
- HPO 目的関数を AUC ではなく「複勝RRの最大化」にカスタムすることも検討
- isotonic calibration との相互最適化

### ★★☆ P7-3: Dual-model 本番組込み
- main.ipynb で win model (単勝用) + rank model (複勝用) を並行運用
- 予想出力: 単勝=edge閾値, 複勝=score閾値 で各独立判定
- 要: score_app.py の2モデル対応

### ★★☆ P7-4: 複勝オッズスクレイピング
- 現在の fukusho EV = score × 単勝odds は代理変数として不十分
- 出馬表から直前複勝オッズを取得できれば、真の EV 計算が可能
- 期待: fukusho_ev 方式が threshold 方式を上回る可能性

### ★☆☆ P7-5: Fine-grain sweep + Bootstrap CI
- edge: 0.13~0.16 を 500点で精密探索
- fukusho: 3.0~3.2 を 500点で精密探索
- Bootstrap (1000回 race-level resampling) で 95% CI 算出
- RR=0.991 が統計的に有意に >0.9 かを検証

### ★☆☆ P7-6: 特徴量選択（SHAP / Boruta）
- 289特徴量 → 重要度上位を精選
- Noise features 除去で汎化性能向上の可能性

---

## 詳細レポート
- [P6レポート](reports/20260209_p6_report.md)
- [P5レポート](reports/20260208_p5_feature_elite_report.md)
- [P4 HPOレポート](reports/20260208_optuna_hpo_report.md)
- [P3校正レポート](reports/20260208_calibration_sweep_report.md)
