# 次のアクションToDo（窓長比較回収率>1.0）

## 前提（固定）
- test期間: 2024-07-01 〜 2026-02-01（固定）
- 評価指標: return_rate / n_bets / n_races / std / AUC(test)
- 購入制約: 単勝 max2、複勝 cap3（固定して比較）
- 校正: none / platt / isotonic を横並び

## P0 再現性固定 
- [x] test対象race_id一覧を保存  data/tmp/test_race_ids_20240701.csv (5,312レース)
- [x] 窓長比較の出力命名規則を統一  models/YYYYMMDD/window_{label}_{policy}.pickle

## P1 可視化（Phase3） 
- [x] window_comparison_summary.pickle から比較プロット生成
  - images/window_auc_comparison.png
  - images/window_tansho_sweep.png
  - images/window_fukusho_sweep.png
  - images/window_reliability_scatter.png
- [x] 校正sweep結果プロット生成
  - images/calibration_heatmap.png
  - images/calibration_grouped_bar.png
  - images/calibration_sweep_curves.png
- [x] レポート  prompts/reports/20260208_calibration_sweep_report.md

## P2 データ起点（2014以前の有無確認） 
- [x] data/raw の最古日年別件数を監査
  - results / race_info / return_tables: 2015〜2026のみ
  - horse_results: 2007〜2026（pre-2015は168万行）
  - 結論: レースデータは2015以降のみ。拡張にはHTML追加取得が必要  保留
- [x] 10y = all = 同一データ範囲（2015-01 〜）を確認

## P3 回収率>1.0に向けた意思決定改善 
- [x] 校正別(none/platt/isotonic)窓長(5y/7y/all)ポリシー(EV/fukusho/edge) 計27sweep
  - 結果: models/20260208/calibration_sweep_summary.pickle
  - best単勝: all_none RR=0.7362 (margin=0.177, n=9108)
  - best複勝: all_isotonic RR=0.8713 (th=2.864, n=519)
- [x] Edge(calibrated_p - 1/odds)ベースの購入条件を実装比較
  - best edge: 5y_none RR=0.7107 (edge_th=0.112, n=9948)  EV方式と同水準
- [x] n_bets >= 500 を満たす範囲でbest採用

## 結論
**現状の回収率は単勝0.74/複勝0.87でいずれも1.0未達。**
校正の効果は限定的。モデルの判別力(AUC0.73)自体の向上が必要。

## P4 Optunaハイパーパラメータ最適化 ✅
- [x] 100 trials (TPESampler, 11パラメータ探索)  約17分
- [x] Best AUC(valid): 0.747011 (trial #82)  Default 0.741165比 +0.0058
- [x] テスト結果:
  - AUC(test): 0.7327 → 0.7391 (+0.0065) ✅微改善
  - 単勝RR: 0.7362 → 0.7019 (-0.0344) ❌悪化
  - 複勝RR: 0.8609 → 0.8580 (-0.0029) ❌微悪化
- [x] 結論: **AUC改善は回収率改善に直結しない。HPOの効果は限定的。**
- [x] 詳細レポート: prompts/reports/20260208_optuna_hpo_report.md
- [x] 可視化: images/optuna_*.png (4枚)

## P5 特徴量エンジニアリング + 少数精鋭戦略 ✅
- [x] P5-1: race-relative features (12特徴量 × z-score + rank = 24新特徴量)
  - 対象: 着順/賞金/着差/time_seconds の5R/allR, 斤量, 年齢, 体重, jockey_plc_rate
  - data/tmp/featured_data_20260205_rel.pickle (305 cols)
- [x] P5-2: 少数精鋭戦略 (--min-bets 50 で低ベット域検証)
- [x] 結果:
  - **AUC(test): 0.7327 → 0.7502 (+0.0175)** ← Optuna (+0.0065) の2.7倍
  - **複勝 RR (n=129, rel+none): 0.9550** ← P3比 +0.084
  - **複勝 RR (n=54, rel+platt): 1.0000** ← 初の1.0! (ただしstd=0.184)
  - 複勝 RR (n=140, rel+platt): 0.9571 ← 最も信頼できる高RR
  - 単勝 RR: 0.7362 → 0.6987 (悪化) ← AUC↑でも単勝RR↓ パターン再確認
- [x] 発見:
  - race-relative features は「レース内相対強さ」を直接表現 → 複勝（3着以内判定）に特効
  - 少数精鋭(n=100-200)でrel variant RR ≈ 0.95-0.96 を安定達成
  - 単勝はEV×オッズ方式のため、AUC改善が効きにくい構造
- [x] 詳細レポート: prompts/reports/20260208_p5_feature_elite_report.md

## 現状サマリ（P5完了時点）

| 指標 | P0初期 | P3校正 | P4 HPO | **P5 rel+elite** |
|:----|:------:|:------:|:------:|:------:|
| AUC(test) | 0.731 | 0.733 | 0.739 | **0.750** |
| 単勝RR | - | 0.736 | 0.702 | 0.699 |
| 複勝RR(n≥500) | - | 0.871 | 0.858 | 0.861 |
| **複勝RR(n≈130)** | - | - | - | **0.955** |
| **複勝RR(n≈54)** | - | - | - | **1.000** |

## Next Steps（P6候補）
- [ ] **★★★ featured_dataにrel features恒久組込み** — 本番運用対応
- [ ] **★★★ rel features拡張** — first_corner/final_corner/枠番等を追加
- [ ] **★★☆ 単勝特化モデル** — 1着のみをtargetとして再学習
- [ ] **★★☆ 複勝EV戦略導入** — score × avg_fukusho_payout >= threshold
- [ ] ★★☆ Optuna × rel features — AUC 0.76+ を狙う
- [ ] ★☆☆ オッズ/レースフィルタ（min_odds、頭数、クラス等）
- [ ] ★☆☆ アンサンブル（複数モデルの予測平均）
