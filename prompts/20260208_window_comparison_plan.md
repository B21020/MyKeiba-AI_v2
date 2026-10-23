# 学習窓長比較 実験計画 (2026/02/08)

## ゴール
「同一テスト期間を固定して、学習データの窓長（5年/7年/10年/全期間）で回収率・AUC・購入点数・stdの安定性を比較する」

---

## 前提条件
- **テスト期間**: 直近の一定期間に固定（例: 2024-07-01 〜 2026-02-08）
  - DataSplitter の test_size で制御するのではなく、**日付カットオフ**で固定する
  - これにより窓長が変わってもテスト対象レースが同一になる
- **学習データ窓長（train_start）**:
  - 5年: 2019-01-01 〜
  - 7年: 2017-01-01 〜
  - 10年: 2014-01-01 〜（≒現行の全期間）
  - 全期間: データの最古日〜（ベースライン）
- **Validation**: train の末尾 20% を valid（early stopping用）
- **featured_data**: 既存の `data/tmp/featured_data_20260205.pickle` を共有（date列でフィルタ）
- **評価**: 単勝EV sweep + 複勝 threshold sweep を各窓長で実行し、pickle化して比較

---

## ToDo（実行順）

### Phase 1: データ準備・窓長切り出しの仕組み
- [x] 1-1. 環境把握（モジュール構造・DataSplitter・FeatureEngineering の仕組みを確認）
- [x] 1-2. featured_data の日付Range確認（最古〜最新、年別レース数分布）
  - 結果: 2015-01-04 〜 2026-02-01、534,240行、38,200レース、281列
  - 年別: 2015(49,895)〜2025(46,537)、2026(4,479)
- [x] 1-3. **方法B（外部で date フィルタ→結合して DataSplitter へ）** で実装
  - `tools/run_window_comparison.py` として実装完了

### Phase 2: 窓長比較スクリプト
- [x] 2-1. `tools/run_window_comparison.py` を作成・動作確認
- [x] 2-2. 本番実行（5/7/10/all, test_start=2024-07-01, base_params流用）

### Phase 2 結果（test_start=2024-07-01, test_end=2026-02-01）

| 窓長  | 学習期間      | 学習行数  | AUC(train) | AUC(test) | 単勝best RR | 単勝n_bets | 単勝std   | 複勝best RR | 複勝n_bets | 複勝std   |
|:-----:|:-------------|:--------:|:----------:|:---------:|:-----------:|:----------:|:---------:|:-----------:|:----------:|:---------:|
| 5y    | 2019-2024    | 238,915  | 0.8504     | 0.7238    | 0.7472      | 9,200      | 0.0499    | 0.9643      | 14         | 0.2109    |
| 7y    | 2017-2024    | 336,683  | 0.8290     | 0.7303    | 0.7248      | 9,216      | 0.0422    | 0.9379      | 87         | 0.1393    |
| 10y   | 2015-2024    | 461,032  | 0.8102     | 0.7312    | 0.7404      | 9,093      | 0.0466    | 0.8670      | 518        | 0.0373    |
| all   | 2015-2024    | 461,032  | 0.8102     | 0.7312    | 0.7404      | 9,093      | 0.0466    | 0.8670      | 518        | 0.0373    |

**所見（Phase 2 時点）:**
- **AUC(test)**: 窓が長いほどわずかに改善（5y: 0.724 → 10y: 0.731）、ただし差は小さい
- **AUC(train)**: 窓が長いほど下がる（過学習しにくくなる＝汎化の方向に寄る）
- **単勝 return_rate**: 10y(0.740) > 5y(0.747) ≒ 7y(0.725)。5y が微妙に高いが std も最大
- **複勝**: best の n_bets が窓長が短いほど極小（5y: 14点）→ 信頼性が低い。10y(518点, std=0.037) が最も安定
- **10y と all が同一**: featured_data が2015年始まりのため、10年窓＝全期間。差別化には2014年以前のデータが必要
- **結論（暫定）**: AUC と安定性（std の小ささ）を重視すると **7〜10年が無難**。ただしreturn_rate の最良値自体はどの窓でも 0.72〜0.75 で大差なく、**窓長よりもモデル・校正・購入ルールの方が支配的**

### Phase 3: 可視化・レポート
- [ ] 3-1. 窓長×return_rate のプロット（± std 帯付き）
- [ ] 3-2. 窓長×AUC のプロット
- [ ] 3-3. 結論を `prompts/reports/` にレポートとして記録

---

## 設計メモ

### テスト期間の固定方法
現行の `DataSplitter.__split_id_by_date()` は `test_size`（割合）で末尾を切る方式。
→ 窓長比較では **日付で直接カット** する必要がある。

方法A: DataSplitter に `train_start_date` / `test_start_date` を渡せるように拡張
方法B: 外部で featured_data を date フィルタしてから DataSplitter に渡す
→ **方法B が最小変更**（既存コードを壊さない）。ただし test 期間のレースIDがずれないよう注意。

### 具体的な流れ（方法B）
```python
# 1. featured_data から test 期間を切り出し
test_mask = (df['date'] >= test_start) & (df['date'] <= test_end)
test_df = df[test_mask]
test_race_ids = test_df['race_id'].unique()

# 2. 各窓長で train を切り出し
train_mask = (df['date'] >= train_start) & (df['date'] < test_start)
train_df = df[train_mask]

# 3. train + test を結合して DataSplitter に渡す（test_size で test 比率を指定）
combined = pd.concat([train_df, test_df])
test_ratio = len(test_df) / len(combined)
ds = DataSplitter(combined, test_size=test_ratio, valid_size=0.2)
# → 時系列ソートで切られるため、test_df が末尾に来る
```

### パラメータ固定
- LightGBM パラメータは既存の best params（models/20260208/basemodel_2020_2026.pickle から取得）を全窓長で共通利用
- チューニングは行わない（train_without_tuning）→ パラメータの差による影響を排除

### 注意点
- 馬の過去成績集約特徴量は featured_data 生成時に「対象日より過去」で作られている
  → featured_data の date フィルタだけで安全に窓長を変えられる（特徴量リークなし）
- ただし featured_data 自体が2015年〜のデータしか含まない場合、5/7/10年の比較範囲が制限される
  → Phase 1-2 で確認

---

## リスク・確認事項
1. featured_data の最古日が2015年以前かどうか → 10年窓が取れるか
2. test期間の長さ（短すぎると分散が大きい、長すぎると窓長比較の効果が見えにくい）
3. 馬の過去成績集約が date filter だけで正しく動くか（DataMerger の `_separate_by_date` で対象日より過去に絞っているので問題ないはず）
