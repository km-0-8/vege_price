# GitHub Actions仕様設計書

## 2. ワークフロー構成

### 2.1 Data Pipeline Workflow (`data-pipeline.yml`)

#### **実行トリガー**
毎月1日 朝9時(JST)、手動実行

#### **ジョブ構成**
data-ingestion（データ取得）
↓
dbt-transformation（データ変換）
↓
ml-prediction（価格予測）
↓
slack-notification（slack通知）

### 2.2 ジョブ詳細仕様

#### **Job 1: data-ingestion**
実行条件:実行トリガー
処理内容:
  - コスト安全チェック (95%超過で中止)
  - 市場データ取得 (get_vege_data.py)
  - 気象データ取得 (get_weather_data.py)
  - BigQuery RAW層保存

#### **Job 2: dbt-transformation**
実行条件: data-ingestion成功
処理内容:
  - ELT実行
  - データ品質テスト

#### **Job 3: ml-prediction**
実行条件: dbt-transformation成功
処理内容:
  - 価格予測実行 (ml_batch.py)
  - 予測結果dbt処理
  - 予測データ品質テスト

#### **Job 4: slack-notification**
実行条件: ml-prediction成功
処理内容:
  - 価格低下予測取得
  - Slackメッセージ作成・送信

## 3. 環境変数・シークレット設計
GitHub Secrets
- `GCP_PROJECT_ID`
- `GCP_SERVICE_ACCOUNT_KEY`
- `GCS_BUCKET`
- `GCS_PREFIX`
- `WEATHER_GCS_PREFIX`
- `SLACK_WEBHOOK_URL`

## 4. 実行フロー詳細

### 4.1 月次定期実行フロー
09:00 JST - データパイプライン開始
09:00-09:20 データ収集
09:20-09:40 dbt変換
09:40-10:40 ML予測
10:40-10:45 Slack通知

