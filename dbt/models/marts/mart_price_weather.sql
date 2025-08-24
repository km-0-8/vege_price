
-- 市場データの月次集計
WITH base_data AS (
  SELECT
    r.year,
    r.month,
    r.item_name,
    r.market,
    r.origin,
    r.major_category,
    r.sub_category,
    
    SUM(r.quantity_kg) AS total_quantity_kg,
    SUM(r.amount_yen) AS total_amount_yen,
    ROUND(AVG(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS avg_unit_price_yen,
    COUNT(*) AS transaction_count
    
  FROM {{ ref('stg_market') }} r
  WHERE r.quantity_kg > 0 
    AND r.amount_yen > 0
    AND r.item_name IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- 天気データの月次平均値計算
weather_monthly AS (
  SELECT
    observation_year AS year,
    observation_month AS month,
    
    ROUND(AVG(avg_temperature), 1) AS avg_temperature,
    ROUND(AVG(max_temperature), 1) AS avg_max_temperature,
    ROUND(AVG(min_temperature), 1) AS avg_min_temperature,
    ROUND(AVG(avg_humidity), 1) AS avg_humidity,
    ROUND(SUM(total_precipitation), 1) AS total_precipitation,
    ROUND(AVG(avg_wind_speed), 1) AS avg_wind_speed,
    ROUND(AVG(total_sunshine), 1) AS total_sunshine,
    COUNT(DISTINCT observation_date) AS weather_observation_days
    
  FROM {{ ref('fact_weather_daily') }} w
  JOIN {{ ref('dim_weather_station') }} s ON w.station_key = s.station_key
  WHERE s.station_name IN ('東京', '熊谷', '千葉')
  GROUP BY 1, 2
),

-- 市場データと天気データの結合と天気カテゴリ分類
base_with_weather AS (
  SELECT
    bd.*,
    wm.avg_temperature,
    wm.avg_max_temperature,
    wm.avg_min_temperature,
    wm.avg_humidity,
    wm.total_precipitation,
    wm.avg_wind_speed,
    wm.total_sunshine,
    wm.weather_observation_days,
    
    CASE 
      WHEN wm.total_precipitation > 150 THEN '多雨'
      WHEN wm.total_precipitation > 75 THEN '普通'
      WHEN wm.total_precipitation IS NULL THEN '不明'
      ELSE '少雨'
    END AS precipitation_category,
    
    CASE
      WHEN wm.avg_temperature > 25 THEN '暑い'
      WHEN wm.avg_temperature > 15 THEN '普通'
      WHEN wm.avg_temperature IS NULL THEN '不明'
      ELSE '寒い'
    END AS temperature_category
    
  FROM base_data bd
  LEFT JOIN weather_monthly wm 
    ON bd.year = wm.year 
    AND bd.month = wm.month
),

-- 時系列分析（前年・前月比較、移動平均計算）
time_series_analysis AS (
  SELECT
    *,
    
    LAG(total_amount_yen, 12) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_year_amount_yen,
    
    LAG(avg_unit_price_yen, 12) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_year_unit_price_yen,
    
    LAG(total_quantity_kg, 12) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_year_quantity_kg,
    
    LAG(total_amount_yen, 1) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_month_amount_yen,
    
    LAG(avg_unit_price_yen, 1) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_month_unit_price_yen,
    
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS price_3month_moving_avg,
    
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS price_6month_moving_avg,
    
    ROW_NUMBER() OVER (
      PARTITION BY year, month 
      ORDER BY avg_unit_price_yen DESC
    ) AS monthly_price_rank
    
  FROM base_with_weather
),

-- 成長率計算と価格トレンド分析
final_calculations AS (
  SELECT
    *,
    
    ROUND(
      SAFE_DIVIDE(
        total_amount_yen - prev_year_amount_yen,
        NULLIF(prev_year_amount_yen, 0)
      ) * 100, 2
    ) AS yoy_amount_growth_rate,
    
    ROUND(
      SAFE_DIVIDE(
        avg_unit_price_yen - prev_year_unit_price_yen,
        NULLIF(prev_year_unit_price_yen, 0)
      ) * 100, 2
    ) AS yoy_price_growth_rate,
    
    ROUND(
      SAFE_DIVIDE(
        total_quantity_kg - prev_year_quantity_kg,
        NULLIF(prev_year_quantity_kg, 0)
      ) * 100, 2
    ) AS yoy_quantity_growth_rate,
    
    ROUND(
      SAFE_DIVIDE(
        total_amount_yen - prev_month_amount_yen,
        NULLIF(prev_month_amount_yen, 0)
      ) * 100, 2
    ) AS mom_amount_growth_rate,
    
    ROUND(
      SAFE_DIVIDE(
        avg_unit_price_yen - prev_month_unit_price_yen,
        NULLIF(prev_month_unit_price_yen, 0)
      ) * 100, 2
    ) AS mom_price_growth_rate,
    
    ROUND(
      SAFE_DIVIDE(
        avg_unit_price_yen - price_3month_moving_avg,
        NULLIF(price_3month_moving_avg, 0)
      ) * 100, 2
    ) AS price_deviation_from_3m_avg,
    
    CASE 
      WHEN price_3month_moving_avg IS NULL THEN '通常圏'
      WHEN avg_unit_price_yen IS NULL THEN '通常圏'
      WHEN avg_unit_price_yen > price_3month_moving_avg * 1.1 THEN '高値圏'
      WHEN avg_unit_price_yen < price_3month_moving_avg * 0.9 THEN '安値圏'
      ELSE '通常圏'
    END AS price_trend_category,
    
    CASE 
      WHEN origin LIKE '%北海道%' THEN '北海道'
      WHEN origin LIKE '%青森%' THEN '青森'
      WHEN origin LIKE '%茨城%' THEN '茨城'
      WHEN origin LIKE '%群馬%' THEN '群馬'
      WHEN origin LIKE '%千葉%' THEN '千葉'
      WHEN origin LIKE '%埼玉%' THEN '埼玉'
      WHEN origin LIKE '%長野%' THEN '長野'
      WHEN origin LIKE '%静岡%' THEN '静岡'
      WHEN origin LIKE '%愛知%' THEN '愛知'
      WHEN origin LIKE '%宮崎%' THEN '宮崎'
      WHEN origin LIKE '%鹿児島%' THEN '鹿児島'
      ELSE '不明・その他'
    END AS origin_prefecture
    
  FROM time_series_analysis
)

SELECT
  year,
  month,
  item_name,
  market,
  origin,
  origin_prefecture,
  major_category,
  sub_category,
  
  total_quantity_kg,
  total_amount_yen,
  avg_unit_price_yen,
  
  prev_year_amount_yen,
  prev_year_unit_price_yen,
  prev_year_quantity_kg,
  yoy_amount_growth_rate,
  yoy_price_growth_rate,
  yoy_quantity_growth_rate,
  
  prev_month_amount_yen,
  prev_month_unit_price_yen,
  mom_amount_growth_rate,
  mom_price_growth_rate,
  
  ROUND(price_3month_moving_avg, 2) AS price_3month_moving_avg,
  ROUND(price_6month_moving_avg, 2) AS price_6month_moving_avg,
  price_deviation_from_3m_avg,
  price_trend_category,
  monthly_price_rank,
  
  avg_temperature,
  avg_max_temperature,
  avg_min_temperature,
  avg_humidity,
  total_precipitation,
  avg_wind_speed,
  total_sunshine,
  precipitation_category,
  temperature_category,
  weather_observation_days,
  
  CASE 
    WHEN weather_observation_days >= 25 THEN 'COMPLETE'
    WHEN weather_observation_days >= 15 THEN 'PARTIAL'
    ELSE 'INSUFFICIENT'
  END AS weather_data_quality

FROM final_calculations