# 野菜市場分析プラットフォーム

野菜市場データと気象データを統合した価格変動分析プラットフォーム

## アーキテクチャ
- **データソース**: 東京都中央卸売市場データ + 気象庁データ
- **クラウド**: Google Cloud Platform (BigQuery + Cloud Storage)
- **変換**: dbtによる階層化データ変換 (RAW → STG → MART)
- **インフラ**: Terraform
- **実行環境**: Docker + Docker Compose

## データフロー

```
市場データ → Excel → Python Script → GCS → BigQuery RAW
気象データ → JMA API → Python Script → GCS → BigQuery RAW
                                            ↓ dbt
                                     STG → MART → 分析
```

## セットアップ手順

### 1. 環境設定
```bash
# 環境変数ファイルの作成
cp .env.example .env
# .envファイルを編集してGCP設定を記入

# GCP認証設定
# 1. GCPサービスアカウントキーをダウンロード
# 2. credentialsディレクトリに配置（service-account.json）
# 3. 環境変数設定
export GOOGLE_APPLICATION_CREDENTIALS="credentials/service-account.json"

# ⚠️ セキュリティ注意: 
# - 認証ファイルは.gitignoreで除外されています
# - 本物の認証ファイルはGitにコミットしないでください
```

### 2. インフラ構築
```bash
# Terraformでインフラ作成
docker-compose run terraform terraform init
docker-compose run terraform terraform apply
```

### 3. データパイプライン実行
```bash
# 野菜市場データ取得
docker-compose run data-ingestion

# 気象データ取得（農業優先14観測所）
docker-compose run weather-ingestion

# dbt変換実行
docker-compose run dbt dbt deps
docker-compose run dbt dbt run
docker-compose run dbt dbt test
```

## プロジェクト構造

```
vege_test/
├── Dockerfile                    # Python + dbt + Terraform
├── docker-compose.yml            # サービス定義
├── requirements.txt              # Python依存関係
├── scripts/
│   ├── get_vege_data.py         # 野菜市場データ取得
│   ├── get_weather_data.py      # 気象データ取得（統合版）
│   ├── ml_price.py              # 機械学習価格予測
│   ├── ml_time_series.py        # 時系列予測モデル
│   ├── ml_api.py                # 予測API（FastAPI）
│   └── generate_architecture_diagram.py # アーキテクチャ図生成
├── dbt/                         # dbtプロジェクト
│   ├── models/
│   │   ├── staging/            # STGレイヤー（データ正規化）
│   │   └── marts/              # MARTレイヤー（ビジネスロジック）
│   ├── dbt_project.yml         # dbt設定
│   └── profiles.yml            # BigQuery接続設定
└── terraform/                   # インフラ定義
    ├── main.tf                 # メイン設定
    ├── modules/                # モジュール
    └── terraform.tfvars.example
```

## データ構造

### BigQuery プロジェクト構成
- **プロジェクト**: ${GCP_PROJECT}
- **データセット**: vege_dev_raw (RAW層), vege_dev_stg (STG層), vege_dev_mart (MART層)

### BigQuery RAWレイヤー (vege_dev_raw)
- `tokyo_market`: 東京都中央卸売市場データ
- `weather_hourly`: 時別気象観測データ

### STGレイヤー (vege_dev_stg)
- `stg_market_raw`: 市場データの正規化
- `stg_weather_observation`: 気象観測データの正規化

### MARTレイヤー (vege_dev_mart) - 11テーブル

**ディメンションテーブル:**
- `dim_item_enhanced`: 強化品目マスター
- `dim_market`: 市場マスター
- `dim_weather_station`: 気象観測所マスター
- `dim_location`: 産地-観測所マッピング
- `dim_time`: 時系列ディメンション

**ファクトテーブル:**
- `fact_weather_daily`: 日次気象データ
- `fact_price_movement`: 価格変動分析ファクト

**マートテーブル:**
- `mart_price_weather`: 価格・気象統合マート
- `mart_seasonal_analysis`: 季節性分析マート

## 気象データ取得

### 観測所カバレッジ
- **農業優先版**: 14観測所（主要野菜産地）
- **全国版**: 47都道府県代表観測所
- **レガシー版**: 5大都市

### 実行例
```bash
# 農業優先版（デフォルト）
python scripts/get_weather_data.py --coverage agriculture

# 全国版
python scripts/get_weather_data.py --coverage all

# 5年分のバックフィル
python scripts/get_weather_data.py --mode backfill --start-year 2020 --end-year 2024

# ドライラン（見積もりのみ）
python scripts/get_weather_data.py --dry-run
```

## 🆓 無料運用機能

このプロジェクトは **GCP完全無料運用** に対応しています：

### 🛡️ 多重安全装置
- ✅ **Terraformコスト制御**: インフラレベルでの課金制御
- ✅ **自動コスト監視**: 無料枠使用量の自動チェック
- ✅ **多段階アラート**: 60%, 80%, 95%での段階的警告
- ✅ **緊急停止機能**: 95%超過時の自動リソース停止
- ✅ **実行前安全チェック**: 処理前の使用量確認
- ✅ **GitHub Actions統合**: CI/CDパイプラインでの自動監視

### 🚀 セットアップ手順

#### 1. 基本セットアップ
```bash
# 依存関係インストール
pip install -r requirements.txt

# 環境変数設定
cp .env.example .env
# .envファイルを編集してGCP設定を記入
```

#### 2. Terraformコスト制御デプロイ
```bash
# Terraform設定ファイル作成
cp terraform/terraform.tfvars.example terraform/terraform.tfvars

# terraform.tfvarsで以下を設定:
# - project_id: GCPプロジェクトID
# - enable_billing_alerts: true
# - billing_account_id: 課金アカウントID（任意）

# インフラ構築（コスト制御機能を含む）
cd terraform
terraform init
terraform plan
terraform apply
```

#### 3. 使用量監視テスト
```bash
# 現在の使用量をチェック
python scripts/cost_monitor.py --check-all

# 処理実行前の安全確認
python scripts/cost_monitor.py --safety-check 0.1

# JSON形式での詳細レポート
python scripts/cost_monitor.py --check-all --output json
```

### 📊 監視・アラート機能

#### 自動監視
- **BigQuery**: 月間1TB制限（無料枠）
- **Cloud Storage**: 月間5GB制限（無料枠）
- **アラート閾値**: WARNING(60%) → CRITICAL(80%) → EMERGENCY(95%)

#### 緊急時対応
```bash
# GitHub Actions無効化
gh workflow disable data-pipeline.yml
gh workflow disable deploy-prod.yml
```

**📖 詳細情報**: [無料運用ガイド](docs/free-tier-operation-guide.md) を参照

## 環境変数

| 変数名 | 説明 | 例 |
|--------|------|-----|
| `GCP_PROJECT` | GCPプロジェクトID | `my-project-123` |
| `GCS_BUCKET` | GCSバケット名 | `vege-dev-raw-data` |
| `BQ_DATASET` | BigQueryデータセット | `vege_dev_raw` |
| `BQ_TABLE` | BigQueryテーブル | `tokyo_market` |
| `ENVIRONMENT` | 環境 | `dev`, `prod` |

## テストと監視

```bash
# dbtテスト実行
docker-compose run dbt dbt test

# コンテナ内での作業
docker-compose run vege-pipeline bash

# ログ確認
docker-compose logs weather-ingestion
```

## 分析ユースケース

256,252件の取引データと気象データの統合分析（2021-2025年）
- 気象条件による価格変動分析
- 147品目、9市場、79産地の包括的分析
- 季節性パターンと価格トレンド分析
- Looker Studioダッシュボードでの可視化

## ダッシュボード

Looker Studioでの3種類のダッシュボード：
1. **市場概況ダッシュボード**: 価格トレンド・取引状況
2. **気象影響分析ダッシュボード**: 気象リスク・相関分析
3. **価格・季節性分析ダッシュボード**: 季節パターン・価格変動

## 機械学習予測システム

### 予測モデル
- **Prophet**: Facebook開発の時系列予測（推奨）
- **LSTM**: 深層学習による時系列予測
- **ARIMA**: 古典的統計手法

### 予測API
```bash
# API起動
docker-compose up ml-api

# 予測実行例
curl "http://localhost:8000/predict/キャベツ?months_ahead=3"
```

### バッチ予測
```bash
# 月次バッチ予測実行（GCSにモデル保存）
docker-compose run ml-batch

# 時系列予測モデル訓練
docker-compose run ml-training

# 利用可能な品目確認
curl "http://localhost:8000/items"
```

## 🔔 Slack通知システム

### 概要
BigQueryの予測結果とGCSの学習済みモデルを活用して、価格低下が見込まれる野菜をSlackに自動通知

### 機能
- **価格低下予測**: 10%以上の価格低下が見込まれる野菜を抽出
- **信頼度フィルタ**: 70%以上の信頼度を持つ予測のみを通知
- **季節洞察**: 月別の価格変動傾向を追加情報として提供
- **モデル情報**: GCSに保存された最新モデルの状態を表示

### セットアップ
```bash
# Slack Webhook URL設定
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# 通知閾値設定（オプション）
export PRICE_DECREASE_THRESHOLD=0.1  # 10%以上の価格低下
export MIN_CONFIDENCE=0.7             # 70%以上の信頼度
```

### 実行方法
```bash
# ドライラン（実際の送信なし・メッセージプレビュー）
python scripts/slack_notif.py --dry-run

# 特定月の通知
python scripts/slack_notif.py --month 3

# 閾値カスタマイズ
python scripts/slack_notif.py --threshold 0.15 --confidence 0.8

# Docker経由での実行
docker-compose run slack-notification
```

### 📊 実行フロー図
```
09:00 Data Pipeline開始（毎月1日）
  ├─ 09:00-09:20 データ収集（市場・気象データ）
  ├─ 09:20-09:40 dbt変換（RAW→STG→MART）
  ├─ 09:40-10:00 ML予測（Prophet/LSTM/ARIMA）
  │                └─ BigQuery保存 + GCS保存
  └─ 10:00-10:05 Slack通知（価格低下予測）
                    └─ 最新予測結果を確実に参照
```

### GitHub Actions統合実行
- **統合実行**: Data Pipeline完了後に自動実行（毎月1日 朝9時〜）
- **実行順序**: データ収集 → dbt変換 → ML予測 → **Slack通知**
- **依存関係**: ML予測成功時のみSlack通知を実行
- **手動実行**: GitHub Actionsから任意のタイミングで実行可能
- **コスト制御**: 実行前に無料枠使用量をチェック
- **エラーハンドリング**: 各ステップの成功/失敗を詳細に通知

### Slackメッセージ例
```
🥬 03月に安くなる野菜情報

AI予測により、03月に価格低下が見込まれる野菜をお知らせします！

🔥 キャベツ: ¥180 → ¥144 (-20.0%)
　信頼度: 85% | モデル: PROPHET

📉 だいこん: ¥120 → ¥102 (-15.0%)
　信頼度: 78% | モデル: RANDOM_FOREST

🤖 予測モデル情報
バージョン: 202501 | 対象品目: 20品目 | 更新: 2025-01-15
```

## カスタマイズ

新しい分析指標を追加する場合：
1. `dbt/models/marts/` にSQLファイルを追加
2. `dbt/models/marts/schema.yml` でテストとドキュメントを定義
3. `dbt run` で実行

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。