# MyKeiba-AI v2 - AI Coding Agent Instructions

## Project Overview
競馬予想AIシステム v2 - 競馬データの取得、前処理、特徴量エンジニアリング、機械学習モデルによる予想を行う包括的なシステム。

## Core Architecture

### Execution Flow
1. **メインエントリーポイント**: `main.ipynb` - 全ての処理をNotebook形式で実行
2. **モジュラー設計**: `modules/` 配下に機能別パッケージを配置
3. **データフロー**: 生データ取得 → 前処理 → 特徴量エンジニアリング → 学習・予測 → シミュレーション

### Module Structure
```
modules/
├── constants/     # マスターデータ、カラム定義、パス定義
├── preparing/     # Webスクレイピング、HTMLデータ取得
├── preprocessing/ # データ前処理、抽象基底クラスパターン
├── training/      # 機械学習モデル（LightGBM）
├── simulation/    # 予想結果シミュレーション
└── policies/      # 賭け戦略・スコア算出ロジック
```

## Key Development Patterns

### Data Processing Pattern
- **抽象基底クラス**: `AbstractDataProcessor` をベースにした統一処理パターン
- **プロセッサー継承**: 各データタイプ（horse_results, race_info等）に専用プロセッサー
- **Pickle形式**: `data/raw/` にpickle形式でデータ保存、処理後は `data/tmp/` へ
- **マスターデータ**: `data/master/` にCSV形式でID管理データ

### Web Scraping Guidelines
⚠️ **CRITICAL**: 必ずサーバー負荷軽減のため `time.sleep(1)` を各リクエスト間に挿入
```python
for race_id in race_id_list:
    time.sleep(1)  # 必須！
    url = "https://db.netkeiba.com/race/" + race_id
    # スクレイピング処理
```

### Column Naming Convention
- `modules/constants/_*_cols.py`: 各データソースのカラム名を定数化
- `HorseResultsCols`, `ResultsCols` 等のクラスで定数管理
- スクレイピング元サイトのテーブル構造に対応

## Development Workflow

### Weekly Operation Schedule (Production)
- **水曜16:30過ぎ**: 過去成績データ更新・再学習 (main.ipynb 2-5章)
- **金曜10:05過ぎ**: 土曜出馬表取得・予想 (main.ipynb 6.1-6.2章)  
- **土日レース日**: リアルタイム予想実行 (main.ipynb 6.3章)

### File Processing Priority
1. **HTML取得**: `modules/preparing/_scrape_html.py` で netkeiba.com からデータ取得
2. **Raw生成**: `modules/preparing/_get_rawdata.py` でPickle形式変換
3. **前処理**: `modules/preprocessing/` でクリーニング・特徴量生成
4. **学習**: `modules/training/` でLightGBMモデル構築

## Technical Specifications

### Dependencies & Environment
- **Python 3.8.5+** 推奨
- **Key Libraries**: pandas, lightgbm, selenium>=4.0.0, beautifulsoup4, optuna
- **実行環境**: Jupyter Notebook (VS Code推奨)
- **Chrome Driver**: `prepare_chrome_driver()` でセットアップ

### Data Structure
- **Index Convention**: `horse_id`, `race_id` をDataFrameインデックスとして利用
- **Date Handling**: `datetime` 型で統一、`date` カラム名で管理
- **Master Files**: IDの整合性管理用、重複チェック・クリーニング対象

### Feature Engineering
- **時系列処理**: 馬の過去成績から集約特徴量を生成
- **レース展開**: コーナー位置から順位変動特徴量を算出
- **外部要因**: 天候・馬場状態・騎手・調教師データの結合

## Common Operations

### Module Reloading
```python
%load_ext autoreload
%autoreload
```
Notebookでのモジュール変更を即座に反映。

### Master Data Cleaning
IDの重複・不整合をチェックし、バックアップ付きでクリーニング実行。`data/master/` の各ファイルが対象。

### Model Training
LightGBMによる多クラス分類、Optunaでハイパーパラメータ最適化、モデルは `models/YYYYMMDD/` に保存。

## Testing & Validation
- **Data Integrity**: マスターデータの整合性チェック
- **Model Performance**: 過去データでのバックテスト実行
- **Scraping Test**: 小規模データでスクレイピング動作確認

## Important Notes
- **Notebook中心開発**: main.ipynb が実行環境の中心、スクリプト化は補助的
- **データ依存性**: 各処理ステップは前段の成果物に依存、順次実行必須
- **スクレイピング制限**: 1秒間隔必須、過度なアクセス避ける
- **Pickle管理**: 大容量データはPickle、設定・マスターはCSV