# データベース定義書
## 1. データベース概要

### 1.1 概要
```
BigQuery Project:
├── vege_dev_raw     (RAW層: 生データ - 3テーブル)
├── vege_dev_stg     (STG層: 正規化データ - 3テーブル)
└── vege_dev_mart    (MART層: 分析用データ - 13テーブル)
```

## 2. RAW層（vege_dev_raw）

### 2.1 tokyo_market
**概要**: 東京都中央卸売市場の野菜取引データ

| カラム名 | データ型 | 説明 | 例 |
|---------|---------|------|-----|
| year | INTEGER | 取引年 | 2024 |
| month | INTEGER | 取引月 | 1 |
| market | STRING | 市場名 | "築地市場" |
| major_category | STRING | 大分類 | "野菜" |
| sub_category | STRING | 中分類 | "葉茎菜類" |
| item_name | STRING | 品目名 | "キャベツ" |
| origin | STRING | 産地 | "群馬県" |
| quantity_kg | INTEGER | 数量(kg) | 12500 |
| amount_yen | INTEGER | 金額(円) | 2250000 |

### 2.2 weather_hourly
**概要**: 気象庁時別気象データ

| カラム名 | データ型 | 説明 | 例 |
|---------|---------|------|-----|
| station_id | STRING | 観測所ID | "47412" |
| station_name | STRING | 観測所名 | "札幌" |
| observation_datetime | TIMESTAMP | 観測日時 | 2024-01-01 09:00:00 |
| temperature | FLOAT | 気温(℃) | 15.5 |
| humidity | FLOAT | 湿度(%) | 65.0 |
| precipitation | FLOAT | 降水量(mm) | 2.5 |
| wind_speed | FLOAT | 風速(m/s) | 3.2 |
| wind_direction | FLOAT | 風向(度) | 180.0 |
| sunshine | FLOAT | 日照時間(時間) | 0.8 |

### 2.3 ml_price_pred
**概要**: 機械学習による価格予測結果

| カラム名 | データ型 | 説明 | 例 |
|---------|---------|------|-----|
| prediction_id | STRING | 予測ID | "pred_20240101_001" |
| prediction_date | DATE | 予測対象日 | 2024-06-01 |
| prediction_year | INTEGER | 予測年 | 2024 |
| prediction_month | INTEGER | 予測月 | 6 |
| prediction_generated_at | TIMESTAMP | 予測生成日時 | 2024-01-01 09:00:00 |
| item_name | STRING | 品目名 | "キャベツ" |
| market | STRING | 市場名 | "築地市場" |
| origin | STRING | 産地 | "群馬県" |
| model_type | STRING | モデル種別 | "LSTM", "PROPHET", "ARIMA" |
| model_version | STRING | モデルバージョン | "1.0" |
| predicted_price | FLOAT | 予測価格(円/kg) | 180.5 |
| lower_bound_price | FLOAT | 予測価格下限 | 150.0 |
| upper_bound_price | FLOAT | 予測価格上限 | 210.0 |
| confidence_interval | FLOAT | 信頼区間 | 0.8 |
| training_data_points | INTEGER | 訓練データ点数 | 36 |
| training_period_start | DATE | 訓練期間開始 | 2021-01-01 |
| training_period_end | DATE | 訓練期間終了 | 2023-12-31 |
| last_actual_price | FLOAT | 最終実価格 | 175.0 |
| last_actual_date | DATE | 最終実価格日 | 2023-12-31 |
| model_mae | FLOAT | モデル平均絶対誤差 | 15.2 |
| model_mse | FLOAT | モデル平均二乗誤差 | 280.5 |
| model_rmse | FLOAT | モデル平均平方根二乗誤差 | 16.7 |
| model_r2 | FLOAT | 決定係数 | 0.85 |
| data_quality_score | FLOAT | データ品質スコア | 0.9 |
| prediction_reliability | STRING | 予測信頼性 | "HIGH", "MEDIUM", "LOW" |
| created_at | TIMESTAMP | 作成日時 | 2024-01-01 09:00:00 |
| updated_at | TIMESTAMP | 更新日時 | 2024-01-01 09:00:00 |

## 3. STG層（vege_dev_stg）

### 3.1 stg_market_raw
主要機能:
- 「野菜」データのみフィルタリング
- データ型の安全な変換（SAFE_CAST）
- 表記ゆれの統一

### 3.2 stg_weather_observation
主要機能:
- 重複データの除去
- 数値の丸め処理

### 3.3 stg_price_pred
主要機能:
- 日付データの正規化・型変換（SAFE_CAST）
- 数値データの丸め処理と範囲チェック

## 4. MART層（vege_dev_mart）

### 4.1 ディメンションテーブル（6個）

#### dim_market - 市場マスター
- market_key: キー
- market: 市場名

#### dim_weather_station - 気象観測所マスター  
- station_key: キー
- station_id: 観測所ID
- station_name: 観測所名
- is_agriculture_priority: 農業優先観測所フラグ
- region: 地域分類

#### dim_location - 産地-観測所マッピング
- location_key: キー
- origin_name: 産地名
- prefecture: 都道府県
- station_id: 対応観測所ID
- region: 地域分類（8地域）
- has_weather_data: 気象データ利用可能フラグ

#### dim_time - 時系列ディメンション
- time_key: キー
- year: 年
- month: 月
- season_japanese: 季節（春夏秋冬）
- quarter: 四半期
- fiscal_year: 年度
- vegetable_demand_period: 野菜需要期

#### dim_item_enhanced - 品目マスター
- item_key: キー
- item_name: 品目名
- major_category: 大分類
- price_tier: 価格帯分類（高級品/中級品/一般品/低価格品）
- vegetable_category: 野菜分類（葉菜類/果菜類/根菜類等）
- seasonality: 季節性（春野菜/夏野菜等）
- storage_durability: 保存性
- avg_unit_price_yen: 平均単価
- is_frequent_item: 頻出品目フラグ

### 4.2 ファクトテーブル（7個）

#### fact_weather_daily - 日次気象データ
- observation_date: 観測日
- station_id: 観測所ID
- avg_temperature: 平均気温
- max_temperature: 最高気温
- min_temperature: 最低気温
- total_precipitation: 日降水量
- avg_wind_speed: 平均風速
- total_sunshine: 日照時間
- station_key: 観測所キー（外部キー）

#### mart_price_weather - 価格・気象統合マート
主要機能:
- 市場データと関東気象データの統合

主要カラム:
- year, month: 時系列キー
- item_name, market, origin: 基本ディメンション
- total_quantity_kg, total_amount_yen: 取引実績
- avg_unit_price_yen: 平均単価
- yoy_price_growth_rate: 前年同月比価格成長率
- mom_price_growth_rate: 前月比価格成長率
- price_3month_moving_avg: 3ヶ月移動平均
- price_trend_category: 価格トレンド（高値圏/安値圏/通常圏）
- avg_temperature, total_precipitation: 気象データ
- precipitation_category, temperature_category: 気象カテゴリ
- weather_data_quality: データ品質（COMPLETE/PARTIAL/INSUFFICIENT）

#### mart_seasonal_analysis - 季節性分析マート
主要機能:
- 品目別月次季節性パターン

主要カラム:
- item_name, month: 基本キー
- season, vegetable_demand_period: 季節分類
- avg_monthly_price: 月平均価格
- seasonal_price_index: 季節価格指数
- seasonal_quantity_index: 季節出荷量指数
- price_rank_high_to_low: 価格ランキング
- seasonality_level: 季節性レベル（高/中/低/なし）
- price_volatility_level: 価格変動性レベル

#### fact_price_movement - 価格変動分析ファクト

主要機能:
- 多期間価格変動率（前月比/3ヶ月前比/6ヶ月前比/前年同月比）

主要カラム:
- price_movement_key: サロゲートキー
- year, month: 時系列
- item_name, market, origin: ディメンション
- mom_price_change_pct: 前月比価格変動率
- yoy_price_change_pct: 前年同月比価格変動率
- price_3month_ma, price_6month_ma: 移動平均
- price_trend_direction: トレンド方向（上昇/下降/横這い/不安定）
- price_range_category: 価格レンジ（高値圏/やや高値/適正圏等）
- price_anomaly_status: 異常値ステータス

#### fact_price_predictions - 価格予測ファクト

主要機能:
- 複数モデル（LSTM/Prophet/ARIMA）による価格予測

主要カラム:
- prediction_key: サロゲートキー
- prediction_date, prediction_year, prediction_month: 予測対象時期
- prediction_season, prediction_quarter: 季節・四半期
- item_name, market, origin: 品目・市場・産地
- model_type, model_version: 使用モデル情報
- predicted_price: 予測価格（円/kg）
- lower_bound_price, upper_bound_price: 予測価格範囲
- prediction_range, confidence_interval: 予測幅・信頼区間
- forecast_horizon_days: 予測先行日数
- prediction_horizon_type: 予測期間分類（SHORT_TERM/MEDIUM_TERM/LONG_TERM）
- training_data_points, training_period_days: 訓練データ情報
- last_actual_price, prediction_lead_time_days: 実績価格・リードタイム
- model_mae, model_rmse, model_r2: 精度メトリクス
- data_quality_score, prediction_reliability: データ品質・予測信頼性
- predicted_price_trend: 予測トレンド（PRICE_INCREASE/DECREASE/STABLE）
- predicted_price_change_pct: 予測価格変動率

#### mart_price_predictions_dashboard - 価格予測ダッシュボードマート
**概要**: 価格予測結果の集約・可視化用マート

主要機能:
- 品目別・期間別予測結果集約
- 予測精度・信頼度分析
- 価格低下予測アラート生成
- モデル性能比較分析

## 5. データ品質管理

### 5.1 テスト項目
- **一意性**: キーの重複チェック
- **NULL値**: 必須項目のNULL許可チェック

### 5.2 データクレンジング
- **表記ゆれ統一**: 「野　菜」→「野菜」
- **重複除去**: 同一観測所・日時の最新データ採用

## 6. パフォーマンス最適化

### 6.1 クラスタリング設定
- **時系列データ**: (year, month) でクラスタリング
- **品目データ**: (item_name) でクラスタリング  
- **気象データ**: (observation_date, station_key) でクラスタリング

### 6.2 マテリアライゼーション
- **STG層**: VIEW
- **MART層**: TABLE
