
-- 品目別月次データの基礎集計
WITH base_seasonal_data AS (
  SELECT
    r.item_name,
    r.major_category,
    r.sub_category,
    r.year,
    r.month,
    r.origin,
    
    SUM(r.quantity_kg) AS total_quantity_kg,
    SUM(r.amount_yen) AS total_amount_yen,
    ROUND(AVG(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS avg_unit_price_yen,
    COUNT(*) AS transaction_count
    
  FROM {{ ref('stg_market') }} r
  WHERE r.quantity_kg > 0 
    AND r.amount_yen > 0
    AND r.item_name IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
),

-- 月別季節パターンの統計計算
seasonal_patterns AS (
  SELECT
    item_name,
    major_category,
    sub_category,
    month,
    
    ROUND(AVG(total_quantity_kg), 1) AS avg_monthly_quantity,
    ROUND(AVG(total_amount_yen), 1) AS avg_monthly_amount,
    ROUND(AVG(avg_unit_price_yen), 2) AS avg_monthly_price,
    ROUND(STDDEV(avg_unit_price_yen), 2) AS monthly_price_stddev,
    
    ROUND(MIN(avg_unit_price_yen), 2) AS historical_min_price,
    ROUND(MAX(avg_unit_price_yen), 2) AS historical_max_price,
    
    COUNT(DISTINCT year) AS years_with_data,
    SUM(transaction_count) AS total_transactions,
    
    ROUND(SAFE_DIVIDE(STDDEV(total_quantity_kg), AVG(total_quantity_kg)), 3) AS quantity_cv,
    ROUND(SAFE_DIVIDE(STDDEV(avg_unit_price_yen), AVG(avg_unit_price_yen)), 3) AS price_cv
    
  FROM base_seasonal_data
  GROUP BY 1, 2, 3, 4
),

-- 品目別年間統計の算出
item_annual_stats AS (
  SELECT
    item_name,
    major_category,
    sub_category,
    
    -- 年間統計
    ROUND(AVG(avg_unit_price_yen), 2) AS annual_avg_price,
    ROUND(STDDEV(avg_unit_price_yen), 2) AS annual_price_stddev,
    SUM(total_quantity_kg) AS annual_total_quantity,
    SUM(total_amount_yen) AS annual_total_amount,
    
    -- 取引パターン
    COUNT(DISTINCT CONCAT(year, '-', month)) AS months_with_data,
    COUNT(DISTINCT year) AS years_available
    
  FROM base_seasonal_data
  GROUP BY 1, 2, 3
),

-- 季節性指標とランキングの計算
seasonal_analysis AS (
  SELECT
    sp.*,
    ias.annual_avg_price,
    ias.annual_price_stddev,
    ias.months_with_data,
    ias.years_available,
    
    -- 季節性指標
    ROUND(
      SAFE_DIVIDE(sp.avg_monthly_price - ias.annual_avg_price, ias.annual_avg_price) * 100, 2
    ) AS seasonal_price_index,
    
    -- 出荷量季節指数
    ROUND(
      sp.avg_monthly_quantity / 
      (ias.annual_total_quantity / NULLIF(ias.months_with_data, 0)) * 100, 1
    ) AS seasonal_quantity_index,
    
    -- 月別ランキング（価格・数量）
    ROW_NUMBER() OVER (
      PARTITION BY sp.item_name 
      ORDER BY sp.avg_monthly_price DESC
    ) AS price_rank_high_to_low,
    
    ROW_NUMBER() OVER (
      PARTITION BY sp.item_name 
      ORDER BY sp.avg_monthly_quantity DESC
    ) AS quantity_rank_high_to_low,
    
    -- 季節カテゴリ
    CASE 
      WHEN sp.month IN (3, 4, 5) THEN '春'
      WHEN sp.month IN (6, 7, 8) THEN '夏'
      WHEN sp.month IN (9, 10, 11) THEN '秋'
      ELSE '冬'
    END AS season,
    
    -- 野菜需要期判定
    CASE 
      WHEN sp.month IN (6, 7, 8) THEN '夏野菜需要期'
      WHEN sp.month IN (12, 1, 2) THEN '冬野菜需要期'
      WHEN sp.month IN (4, 5) THEN '春端境期'
      WHEN sp.month IN (10, 11) THEN '秋端境期'
      ELSE '通常期'
    END AS vegetable_demand_period
    
  FROM seasonal_patterns sp
  JOIN item_annual_stats ias 
    ON sp.item_name = ias.item_name
    AND sp.major_category = ias.major_category
    AND sp.sub_category = ias.sub_category
),

-- ピーク月とボトム月の特定
peak_seasons AS (
  SELECT
    item_name,
    major_category,
    sub_category,
    
    -- 出荷量ピーク月（上位3月）
    STRING_AGG(
      CAST(month AS STRING), ', ' 
      ORDER BY avg_monthly_quantity DESC 
      LIMIT 3
    ) AS peak_supply_months,
    
    -- 価格高値月（上位3月）
    STRING_AGG(
      CAST(month AS STRING), ', ' 
      ORDER BY avg_monthly_price DESC 
      LIMIT 3
    ) AS peak_price_months,
    
    -- 価格安値月（上位3月）
    STRING_AGG(
      CAST(month AS STRING), ', ' 
      ORDER BY avg_monthly_price ASC 
      LIMIT 3
    ) AS low_price_months,
    
    -- 季節性の強さ（価格変動係数）
    ROUND(AVG(price_cv), 3) AS seasonality_strength,
    
    -- 主要出荷月の価格レンジ
    MAX(CASE WHEN quantity_rank_high_to_low <= 3 THEN avg_monthly_price END) AS peak_supply_max_price,
    MIN(CASE WHEN quantity_rank_high_to_low <= 3 THEN avg_monthly_price END) AS peak_supply_min_price
    
  FROM seasonal_analysis
  GROUP BY 1, 2, 3
),

-- 野菜分類と季節性パターンの推定
vegetable_classification AS (
  SELECT
    item_name,
    major_category,
    sub_category,
    
    -- 野菜分類の推定
    CASE 
      WHEN item_name LIKE '%レタス%' OR item_name LIKE '%キャベツ%' 
           OR item_name LIKE '%白菜%' OR item_name LIKE '%ほうれん草%' 
           OR item_name LIKE '%小松菜%' THEN '葉菜類'
      WHEN item_name LIKE '%トマト%' OR item_name LIKE '%きゅうり%' 
           OR item_name LIKE '%なす%' OR item_name LIKE '%ピーマン%' 
           OR item_name LIKE '%かぼちゃ%' THEN '果菜類'
      WHEN item_name LIKE '%大根%' OR item_name LIKE '%にんじん%' 
           OR item_name LIKE '%ごぼう%' OR item_name LIKE '%玉ねぎ%' THEN '根菜類'
      WHEN item_name LIKE '%じゃがいも%' OR item_name LIKE '%さつまいも%' THEN '芋類'
      WHEN item_name LIKE '%ねぎ%' OR item_name LIKE '%にんにく%' 
           OR item_name LIKE '%生姜%' THEN '香味野菜'
      ELSE 'その他野菜'
    END AS vegetable_category,
    
    -- 季節性パターンの推定
    CASE 
      WHEN peak_supply_months LIKE '%6%' OR peak_supply_months LIKE '%7%' 
           OR peak_supply_months LIKE '%8%' THEN '夏野菜'
      WHEN peak_supply_months LIKE '%12%' OR peak_supply_months LIKE '%1%' 
           OR peak_supply_months LIKE '%2%' THEN '冬野菜'
      WHEN peak_supply_months LIKE '%3%' OR peak_supply_months LIKE '%4%' 
           OR peak_supply_months LIKE '%5%' THEN '春野菜'
      WHEN peak_supply_months LIKE '%9%' OR peak_supply_months LIKE '%10%' 
           OR peak_supply_months LIKE '%11%' THEN '秋野菜'
      ELSE '通年野菜'
    END AS seasonality_pattern
    
  FROM peak_seasons
)

SELECT
  -- 基本ディメンション
  sa.item_name,
  sa.major_category,
  sa.sub_category,
  sa.month,
  sa.season,
  sa.vegetable_demand_period,
  vc.vegetable_category,
  vc.seasonality_pattern,
  
  -- 月次パフォーマンス
  sa.avg_monthly_quantity,
  sa.avg_monthly_amount,
  sa.avg_monthly_price,
  sa.monthly_price_stddev,
  sa.historical_min_price,
  sa.historical_max_price,
  
  -- 季節性指標
  sa.seasonal_price_index,
  sa.seasonal_quantity_index,
  sa.price_rank_high_to_low,
  sa.quantity_rank_high_to_low,
  
  -- データ品質
  sa.years_with_data,
  sa.total_transactions,
  sa.months_with_data,
  sa.years_available,
  
  -- 変動性指標
  sa.quantity_cv,
  sa.price_cv,
  ps.seasonality_strength,
  
  -- 季節パターン要約
  ps.peak_supply_months,
  ps.peak_price_months,
  ps.low_price_months,
  ps.peak_supply_max_price,
  ps.peak_supply_min_price,
  
  -- 年間ベースライン
  sa.annual_avg_price,
  sa.annual_price_stddev,
  
  -- 季節性判定フラグ
  CASE 
    WHEN ps.seasonality_strength > 0.3 THEN '高季節性'
    WHEN ps.seasonality_strength > 0.15 THEN '中季節性'
    WHEN ps.seasonality_strength > 0.05 THEN '低季節性'
    ELSE '季節性なし'
  END AS seasonality_level,
  
  -- 価格ボラティリティ判定
  CASE 
    WHEN sa.price_cv > 0.4 THEN '高変動'
    WHEN sa.price_cv > 0.2 THEN '中変動'
    WHEN sa.price_cv > 0.1 THEN '低変動'
    ELSE '安定'
  END AS price_volatility_level

FROM seasonal_analysis sa
JOIN peak_seasons ps
  ON sa.item_name = ps.item_name
  AND sa.major_category = ps.major_category
  AND sa.sub_category = ps.sub_category
JOIN vegetable_classification vc
  ON sa.item_name = vc.item_name
  AND sa.major_category = vc.major_category
  AND sa.sub_category = vc.sub_category