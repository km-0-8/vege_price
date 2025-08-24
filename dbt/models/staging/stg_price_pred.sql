{{ config(
    materialized='view',
    description='価格予測結果の正規化ビュー'
) }}

SELECT 
  prediction_id,
  CASE 
    WHEN prediction_date IS NULL THEN NULL
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(prediction_date AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(prediction_date AS STRING))
    ELSE CAST(prediction_date AS DATE)
  END as prediction_date,
  SAFE_CAST(prediction_year as INT64) as prediction_year,
  SAFE_CAST(prediction_month as INT64) as prediction_month,
  CASE 
    WHEN prediction_generated_at IS NULL THEN NULL
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(prediction_generated_at AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(prediction_generated_at AS STRING))
    ELSE CAST(prediction_generated_at AS TIMESTAMP)
  END as prediction_generated_at,
  TRIM(UPPER(item_name)) as item_name,
  TRIM(market) as market,
  TRIM(origin) as origin,
  UPPER(TRIM(model_type)) as model_type,
  COALESCE(model_version, '1.0') as model_version,
  ROUND(SAFE_CAST(predicted_price as FLOAT64), 2) as predicted_price,
  ROUND(SAFE_CAST(lower_bound_price as FLOAT64), 2) as lower_bound_price,
  ROUND(SAFE_CAST(upper_bound_price as FLOAT64), 2) as upper_bound_price,
  SAFE_CAST(confidence_interval as FLOAT64) as confidence_interval,
  SAFE_CAST(training_data_points as INT64) as training_data_points,
  CASE 
    WHEN training_period_start IS NULL THEN NULL
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(training_period_start AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(training_period_start AS STRING))
    ELSE CAST(training_period_start AS DATE)
  END as training_period_start,
  CASE 
    WHEN training_period_end IS NULL THEN NULL
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(training_period_end AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(training_period_end AS STRING))
    ELSE CAST(training_period_end AS DATE)
  END as training_period_end,
  ROUND(SAFE_CAST(last_actual_price as FLOAT64), 2) as last_actual_price,
  CASE 
    WHEN last_actual_date IS NULL THEN NULL
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(last_actual_date AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_DATE('%Y-%m-%d', CAST(last_actual_date AS STRING))
    ELSE CAST(last_actual_date AS DATE)
  END as last_actual_date,
  ROUND(SAFE_CAST(model_mae as FLOAT64), 2) as model_mae,
  ROUND(SAFE_CAST(model_mse as FLOAT64), 2) as model_mse,
  ROUND(SAFE_CAST(model_rmse as FLOAT64), 2) as model_rmse,
  CASE 
    WHEN SAFE_CAST(model_r2 as FLOAT64) IS NULL THEN NULL
    WHEN SAFE_CAST(model_r2 as FLOAT64) < -1 THEN -1.0
    WHEN SAFE_CAST(model_r2 as FLOAT64) > 1 THEN 1.0
    ELSE ROUND(SAFE_CAST(model_r2 as FLOAT64), 3)
  END as model_r2,
  SAFE_CAST(data_quality_score as FLOAT64) as data_quality_score,
  prediction_reliability,
  CASE 
    WHEN created_at IS NULL THEN NULL
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(created_at AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(created_at AS STRING))
    ELSE CAST(created_at AS TIMESTAMP)
  END as created_at,
  CASE 
    WHEN updated_at IS NULL THEN NULL
    WHEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(updated_at AS STRING)) IS NOT NULL 
    THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(updated_at AS STRING))
    ELSE CAST(updated_at AS TIMESTAMP)
  END as updated_at

FROM {{ source('vege_dev_raw', 'ml_price_pred') }}

WHERE 
  prediction_date IS NOT NULL
  AND item_name IS NOT NULL
  AND model_type IS NOT NULL
  AND predicted_price IS NOT NULL
  AND SAFE_CAST(predicted_price as FLOAT64) > 0
  AND SAFE_CAST(predicted_price as FLOAT64) < 10000
  AND prediction_date IS NOT NULL
  AND prediction_date >= '2020-01-01'
  AND prediction_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 3 YEAR)
  AND UPPER(TRIM(model_type)) IN ('PROPHET', 'LSTM', 'ARIMA', 'RANDOM_FOREST', 'GRADIENT_BOOSTING', 'LINEAR_REGRESSION')
  AND SAFE_CAST(training_data_points as INT64) >= 12
  AND (model_r2 IS NULL OR (SAFE_CAST(model_r2 as FLOAT64) >= -10 AND SAFE_CAST(model_r2 as FLOAT64) <= 2))