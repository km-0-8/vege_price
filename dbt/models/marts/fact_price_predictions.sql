{{ config(
    materialized='table',
    cluster_by=['prediction_date', 'item_name', 'model_type'],
    description='野菜価格予測結果ファクトテーブル'
) }}

-- 価格予測データの基本フィルタリングと正規化
WITH base_predictions AS (
  SELECT 
    GENERATE_UUID() as prediction_key,
    
    
    prediction_date,
    prediction_year,
    prediction_month,
    prediction_generated_at,
    
    
    item_name,
    market,
    origin,
    
    
    model_type,
    model_version,
    
    
    predicted_price,
    lower_bound_price,
    upper_bound_price,
    confidence_interval,
    
    
    training_data_points,
    training_period_start,
    training_period_end,
    last_actual_price,
    last_actual_date,
    
    
    model_mae,
    model_mse,
    model_rmse,
    model_r2,
    
    
    data_quality_score,
    prediction_reliability,
    
    
    created_at,
    updated_at
    
  FROM {{ ref('stg_price_pred') }}
  WHERE prediction_date IS NOT NULL
    AND predicted_price IS NOT NULL
    AND predicted_price > 0
    AND predicted_price < 10000
)

SELECT 
  prediction_key,
  
  
  prediction_date,
  prediction_year,
  prediction_month,
  EXTRACT(QUARTER FROM prediction_date) as prediction_quarter,
  CASE 
    WHEN EXTRACT(MONTH FROM prediction_date) IN (3,4,5) THEN '春'
    WHEN EXTRACT(MONTH FROM prediction_date) IN (6,7,8) THEN '夏'
    WHEN EXTRACT(MONTH FROM prediction_date) IN (9,10,11) THEN '秋'
    ELSE '冬'
  END as prediction_season,
  
  
  prediction_generated_at,
  DATE_DIFF(prediction_date, DATE(prediction_generated_at), DAY) as forecast_horizon_days,
  
  
  item_name,
  COALESCE(market, 'ALL_MARKETS') as market,
  COALESCE(origin, 'ALL_ORIGINS') as origin,
  
  
  model_type,
  COALESCE(model_version, '1.0') as model_version,
  
  
  ROUND(predicted_price, 2) as predicted_price,
  ROUND(COALESCE(lower_bound_price, predicted_price * 0.8), 2) as lower_bound_price,
  ROUND(COALESCE(upper_bound_price, predicted_price * 1.2), 2) as upper_bound_price,
  ROUND(COALESCE(upper_bound_price, predicted_price * 1.2) - COALESCE(lower_bound_price, predicted_price * 0.8), 2) as prediction_range,
  COALESCE(confidence_interval, 0.8) as confidence_interval,
  
  
  training_data_points,
  training_period_start,
  training_period_end,
  DATE_DIFF(training_period_end, training_period_start, DAY) as training_period_days,
  
  
  ROUND(last_actual_price, 2) as last_actual_price,
  last_actual_date,
  DATE_DIFF(prediction_date, last_actual_date, DAY) as prediction_lead_time_days,
  
  
  ROUND(COALESCE(model_mae, 0), 2) as model_mae,
  ROUND(COALESCE(model_mse, 0), 2) as model_mse,
  ROUND(COALESCE(model_rmse, 0), 2) as model_rmse,
  ROUND(COALESCE(model_r2, 0), 3) as model_r2,
  
  
  COALESCE(data_quality_score, 0.7) as data_quality_score,
  CASE 
    WHEN COALESCE(data_quality_score, 0.7) >= 0.9 THEN 'HIGH'
    WHEN COALESCE(data_quality_score, 0.7) >= 0.7 THEN 'MEDIUM'
    WHEN COALESCE(data_quality_score, 0.7) >= 0.5 THEN 'LOW'
    ELSE 'VERY_LOW'
  END as prediction_reliability,
  
  
  CASE 
    WHEN DATE_DIFF(prediction_date, DATE(prediction_generated_at), DAY) <= 30 THEN 'SHORT_TERM'
    WHEN DATE_DIFF(prediction_date, DATE(prediction_generated_at), DAY) <= 90 THEN 'MEDIUM_TERM'
    ELSE 'LONG_TERM'
  END as prediction_horizon_type,
  
  
  CASE 
    WHEN predicted_price > last_actual_price * 1.1 THEN 'PRICE_INCREASE'
    WHEN predicted_price < last_actual_price * 0.9 THEN 'PRICE_DECREASE'
    ELSE 'PRICE_STABLE'
  END as predicted_price_trend,
  
  ROUND((predicted_price - last_actual_price) / last_actual_price * 100, 2) as predicted_price_change_pct,
  
  
  COALESCE(created_at, CURRENT_TIMESTAMP()) as created_at,
  COALESCE(updated_at, CURRENT_TIMESTAMP()) as updated_at

FROM base_predictions

WHERE training_data_points >= 12
  AND DATE_DIFF(prediction_date, DATE(prediction_generated_at), DAY) <= 1095