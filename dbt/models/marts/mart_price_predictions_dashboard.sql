{{ config(
    materialized='table',
    cluster_by=['item_name', 'prediction_month', 'model_type'],
    description='ダッシュボード表示用の価格予測データマート'
) }}

-- 最新の価格予測データの抽出
WITH latest_predictions AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY item_name, prediction_date, model_type 
      ORDER BY prediction_generated_at DESC
    ) as rn
  FROM {{ ref('fact_price_predictions') }}
),

-- ダッシュボード表示用の予測サマリー
prediction_summary AS (
  SELECT 
    prediction_date,
    prediction_year,
    prediction_month,
    prediction_quarter,
    prediction_season,
    item_name,
    model_type,
    
    predicted_price,
    lower_bound_price,
    upper_bound_price,
    prediction_range,
    confidence_interval,
    
    
    last_actual_price,
    last_actual_date,
    predicted_price_change_pct,
    predicted_price_trend,
    
    
    prediction_horizon_type,
    forecast_horizon_days,
    prediction_lead_time_days,
    
    
    model_mae,
    model_rmse,
    model_r2,
    prediction_reliability,
    data_quality_score,
    
    
    training_data_points,
    prediction_generated_at
    
  FROM latest_predictions
  WHERE rn = 1
    AND prediction_date >= CURRENT_DATE()
    AND prediction_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 12 MONTH)
),

-- 実績価格データの抽出
actual_prices AS (
  SELECT 
    item_name,
    DATE(year, month, 1) as actual_date,
    year as actual_year,
    month as actual_month,
    AVG(avg_unit_price_yen) as actual_price,
    SUM(total_quantity_kg) as actual_quantity,
    COUNT(*) as market_count
  FROM {{ ref('mart_price_weather') }}
  WHERE year >= 2020
  GROUP BY item_name, year, month
),

-- モデル別精度指標の集計
model_accuracy AS (
  SELECT 
    item_name,
    model_type,
    COUNT(*) as prediction_count,
    AVG(model_mae) as avg_mae,
    AVG(model_rmse) as avg_rmse,
    AVG(model_r2) as avg_r2,
    AVG(CASE WHEN prediction_reliability = 'HIGH' THEN 1 ELSE 0 END) as high_reliability_ratio
  FROM prediction_summary
  GROUP BY item_name, model_type
),

-- 品目別予測トレンドの分析
item_trends AS (
  SELECT 
    item_name,
    prediction_month,
    COUNT(DISTINCT model_type) as model_count,
    AVG(predicted_price) as avg_predicted_price,
    MIN(predicted_price) as min_predicted_price,
    MAX(predicted_price) as max_predicted_price,
    STDDEV(predicted_price) as price_prediction_std,
    
    
    AVG(CASE WHEN predicted_price_trend = 'PRICE_INCREASE' THEN 1 ELSE 0 END) as increase_consensus,
    AVG(CASE WHEN predicted_price_trend = 'PRICE_DECREASE' THEN 1 ELSE 0 END) as decrease_consensus,
    AVG(CASE WHEN predicted_price_trend = 'PRICE_STABLE' THEN 1 ELSE 0 END) as stable_consensus
    
  FROM prediction_summary
  GROUP BY item_name, prediction_month
)

SELECT 
  ps.prediction_date,
  ps.prediction_year,
  ps.prediction_month,
  ps.prediction_quarter,
  ps.prediction_season,
  ps.item_name,
  ps.model_type,
  
  
  ROUND(ps.predicted_price, 2) as predicted_price,
  ROUND(ps.lower_bound_price, 2) as lower_bound_price,
  ROUND(ps.upper_bound_price, 2) as upper_bound_price,
  ROUND(ps.prediction_range, 2) as prediction_range,
  ps.confidence_interval,
  ps.predicted_price_trend,
  ps.predicted_price_change_pct,
  
  
  ROUND(ps.last_actual_price, 2) as last_actual_price,
  ps.last_actual_date,
  ROUND(ap.actual_price, 2) as comparable_actual_price,
  ap.actual_date as comparable_actual_date,
  
  
  CASE 
    WHEN ap.actual_price IS NOT NULL THEN
      ROUND((ps.predicted_price - ap.actual_price) / ap.actual_price * 100, 2)
    ELSE NULL
  END as vs_previous_year_pct,
  
  
  ps.prediction_horizon_type,
  ps.forecast_horizon_days,
  ps.prediction_lead_time_days,
  ps.prediction_reliability,
  ps.data_quality_score,
  
  
  ROUND(ps.model_mae, 2) as model_mae,
  ROUND(ps.model_rmse, 2) as model_rmse,
  ROUND(ps.model_r2, 3) as model_r2,
  ps.training_data_points,
  
  
  it.model_count as total_models_for_item,
  ROUND(it.avg_predicted_price, 2) as item_month_avg_prediction,
  ROUND(it.min_predicted_price, 2) as item_month_min_prediction,
  ROUND(it.max_predicted_price, 2) as item_month_max_prediction,
  ROUND(it.price_prediction_std, 2) as item_month_prediction_std,
  
  
  ROUND(it.increase_consensus * 100, 1) as increase_consensus_pct,
  ROUND(it.decrease_consensus * 100, 1) as decrease_consensus_pct,
  ROUND(it.stable_consensus * 100, 1) as stable_consensus_pct,
  
  
  RANK() OVER (
    PARTITION BY ps.item_name 
    ORDER BY ma.avg_mae ASC
  ) as model_accuracy_rank,
  
  
  RANK() OVER (
    PARTITION BY ps.prediction_month 
    ORDER BY ps.predicted_price DESC
  ) as monthly_price_rank,
  
  
  CASE 
    WHEN ps.predicted_price > ps.last_actual_price * 1.2 THEN '大幅上昇予測'
    WHEN ps.predicted_price > ps.last_actual_price * 1.1 THEN '上昇予測'
    WHEN ps.predicted_price < ps.last_actual_price * 0.8 THEN '大幅下落予測'
    WHEN ps.predicted_price < ps.last_actual_price * 0.9 THEN '下落予測'
    ELSE '安定予測'
  END as prediction_category,
  
  CASE 
    WHEN ps.prediction_reliability = 'HIGH' AND ps.data_quality_score >= 0.8 THEN '高信頼度'
    WHEN ps.prediction_reliability IN ('HIGH', 'MEDIUM') AND ps.data_quality_score >= 0.6 THEN '中信頼度'
    ELSE '低信頼度'
  END as overall_confidence,
  
  
  ps.prediction_generated_at,
  CURRENT_TIMESTAMP() as dashboard_updated_at

FROM prediction_summary ps

LEFT JOIN actual_prices ap ON (
  ps.item_name = ap.item_name 
  AND ps.prediction_month = ap.actual_month
  AND ap.actual_year = ps.prediction_year - 1
)

LEFT JOIN model_accuracy ma ON (
  ps.item_name = ma.item_name 
  AND ps.model_type = ma.model_type
)

LEFT JOIN item_trends it ON (
  ps.item_name = it.item_name 
  AND ps.prediction_month = it.prediction_month
)

WHERE ps.data_quality_score >= 0.5
  AND ps.training_data_points >= 12