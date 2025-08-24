
-- 価格データの基礎集計と異常値フィルタリング
WITH base_price_data AS (
  SELECT
    r.year,
    r.month,
    r.item_name,
    r.market,
    r.origin,
    r.major_category,
    r.sub_category,
    
    ROUND(AVG(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS avg_unit_price_yen,
    ROUND(MIN(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS min_unit_price_yen,
    ROUND(MAX(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS max_unit_price_yen,
    ROUND(STDDEV(SAFE_DIVIDE(r.amount_yen, NULLIF(r.quantity_kg, 0))), 2) AS price_stddev,
    
    SUM(r.quantity_kg) AS total_quantity_kg,
    SUM(r.amount_yen) AS total_amount_yen,
    COUNT(*) AS transaction_count
    
  FROM {{ ref('stg_market') }} r
  WHERE r.quantity_kg > 0 
    AND r.amount_yen > 0
    AND r.item_name IS NOT NULL
    AND SAFE_DIVIDE(r.amount_yen, r.quantity_kg) BETWEEN 1 AND 10000
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- 時系列分析用の移動平均と時間5差分計算
price_movement_calculations AS (
  SELECT
    *,
    DATE(year, month, 1) AS price_date,
    
    -- 時系列ウィンドウ関数（品目×市場×産地単位）
    LAG(avg_unit_price_yen, 1) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_month_price,
    
    LAG(avg_unit_price_yen, 3) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_3month_price,
    
    LAG(avg_unit_price_yen, 6) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_6month_price,
    
    LAG(avg_unit_price_yen, 12) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month
    ) AS prev_year_price,
    
    -- 移動平均（品目×市場×産地単位）
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS price_3month_ma,
    
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS price_6month_ma,
    
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS price_12month_ma,
    
    -- 変動率計算用の窓関数
    STDDEV(avg_unit_price_yen) OVER (
      PARTITION BY item_name, market, origin 
      ORDER BY year, month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS price_12month_stddev,
    
    -- 品目別全体統計（異常値検知用）
    AVG(avg_unit_price_yen) OVER (
      PARTITION BY item_name 
      ORDER BY year, month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS item_12month_avg_price,
    
    STDDEV(avg_unit_price_yen) OVER (
      PARTITION BY item_name 
      ORDER BY year, month 
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS item_12month_stddev_price
    
  FROM base_price_data
),

-- 変動率と乖離率の計算、Z-Scoreによる異常値検知
trend_analysis AS (
  SELECT
    *,
    
    -- 前月比変動率
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - prev_month_price, NULLIF(prev_month_price, 0)) * 100, 2
    ) AS mom_price_change_pct,
    
    -- 3ヶ月前比変動率
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - prev_3month_price, NULLIF(prev_3month_price, 0)) * 100, 2
    ) AS q3m_price_change_pct,
    
    -- 6ヶ月前比変動率
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - prev_6month_price, NULLIF(prev_6month_price, 0)) * 100, 2
    ) AS q6m_price_change_pct,
    
    -- 前年同月比変動率
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - prev_year_price, NULLIF(prev_year_price, 0)) * 100, 2
    ) AS yoy_price_change_pct,
    
    -- 移動平均からの乖離率
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - price_3month_ma, NULLIF(price_3month_ma, 0)) * 100, 2
    ) AS deviation_from_3m_ma_pct,
    
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - price_6month_ma, NULLIF(price_6month_ma, 0)) * 100, 2
    ) AS deviation_from_6m_ma_pct,
    
    ROUND(
      SAFE_DIVIDE(avg_unit_price_yen - price_12month_ma, NULLIF(price_12month_ma, 0)) * 100, 2
    ) AS deviation_from_12m_ma_pct,
    
    -- Z-Score（標準化偏差、異常値検知用）
    ROUND(
      SAFE_DIVIDE(
        avg_unit_price_yen - price_12month_ma, 
        NULLIF(price_12month_stddev, 0)
      ), 2
    ) AS price_zscore_12m,
    
    -- 品目全体から見た異常度
    ROUND(
      SAFE_DIVIDE(
        avg_unit_price_yen - item_12month_avg_price, 
        NULLIF(item_12month_stddev_price, 0)
      ), 2
    ) AS item_price_zscore_12m,
    
    -- ボラティリティ（変動係数）
    ROUND(
      SAFE_DIVIDE(price_12month_stddev, NULLIF(price_12month_ma, 0)), 3
    ) AS price_volatility_12m
    
  FROM price_movement_calculations
),

-- 価格パターンの分類とトレンド方向の判定
price_pattern_classification AS (
  SELECT
    *,
    
    -- トレンド方向判定
    CASE 
      WHEN price_3month_ma > price_6month_ma AND price_6month_ma > price_12month_ma THEN '上昇トレンド'
      WHEN price_3month_ma < price_6month_ma AND price_6month_ma < price_12month_ma THEN '下降トレンド'
      WHEN ABS(deviation_from_6m_ma_pct) <= 5 THEN '横這い'
      ELSE '不安定'
    END AS price_trend_direction,
    
    -- 価格レンジ判定
    CASE 
      WHEN avg_unit_price_yen > price_12month_ma * 1.2 THEN '高値圏'
      WHEN avg_unit_price_yen > price_12month_ma * 1.1 THEN 'やや高値'
      WHEN avg_unit_price_yen < price_12month_ma * 0.8 THEN '安値圏'
      WHEN avg_unit_price_yen < price_12month_ma * 0.9 THEN 'やや安値'
      ELSE '適正圏'
    END AS price_range_category,
    
    -- 変動性レベル
    CASE 
      WHEN price_volatility_12m > 0.4 THEN '高変動'
      WHEN price_volatility_12m > 0.2 THEN '中変動'
      WHEN price_volatility_12m > 0.1 THEN '低変動'
      ELSE '安定'
    END AS volatility_level,
    
    -- 異常値判定
    CASE 
      WHEN ABS(price_zscore_12m) > 3 THEN '異常値'
      WHEN ABS(price_zscore_12m) > 2 THEN '要注意'
      WHEN ABS(price_zscore_12m) > 1.5 THEN '監視対象'
      ELSE '正常範囲'
    END AS price_anomaly_status,
    
    -- 短期変動判定
    CASE 
      WHEN ABS(mom_price_change_pct) > 30 THEN '急激変動'
      WHEN ABS(mom_price_change_pct) > 15 THEN '大幅変動'
      WHEN ABS(mom_price_change_pct) > 5 THEN '通常変動'
      ELSE '微小変動'
    END AS short_term_movement,
    
    -- 季節調整済み評価
    CASE 
      WHEN ABS(item_price_zscore_12m) > 2 THEN '品目内異常'
      WHEN ABS(item_price_zscore_12m) > 1 THEN '品目内注意'
      ELSE '品目内正常'
    END AS item_relative_status
    
  FROM trend_analysis
)

SELECT
  -- 基本識別子
  ROW_NUMBER() OVER (ORDER BY year, month, item_name, market, origin) AS price_movement_key,
  
  -- 時間・場所・品目
  year,
  month,
  price_date,
  item_name,
  market,
  origin,
  major_category,
  sub_category,
  
  -- 基本価格データ
  avg_unit_price_yen,
  min_unit_price_yen,
  max_unit_price_yen,
  price_stddev,
  total_quantity_kg,
  total_amount_yen,
  transaction_count,
  
  -- 履歴価格
  prev_month_price,
  prev_3month_price,
  prev_6month_price,
  prev_year_price,
  
  -- 移動平均
  ROUND(price_3month_ma, 2) AS price_3month_ma,
  ROUND(price_6month_ma, 2) AS price_6month_ma,
  ROUND(price_12month_ma, 2) AS price_12month_ma,
  
  -- 変動率
  mom_price_change_pct,
  q3m_price_change_pct,
  q6m_price_change_pct,
  yoy_price_change_pct,
  
  -- 乖離率
  deviation_from_3m_ma_pct,
  deviation_from_6m_ma_pct,
  deviation_from_12m_ma_pct,
  
  -- 統計指標
  price_zscore_12m,
  item_price_zscore_12m,
  price_volatility_12m,
  
  -- 分類・判定
  price_trend_direction,
  price_range_category,
  volatility_level,
  price_anomaly_status,
  short_term_movement,
  item_relative_status,
  
  -- 品目内順位（価格高低）
  ROW_NUMBER() OVER (
    PARTITION BY item_name, year, month 
    ORDER BY avg_unit_price_yen DESC
  ) AS monthly_price_rank_within_item,
  
  -- 全品目内順位（価格高低）
  ROW_NUMBER() OVER (
    PARTITION BY year, month 
    ORDER BY avg_unit_price_yen DESC
  ) AS monthly_overall_price_rank,
  
  -- 変動幅（価格レンジ）
  ROUND(max_unit_price_yen - min_unit_price_yen, 2) AS monthly_price_range,
  ROUND(
    SAFE_DIVIDE(max_unit_price_yen - min_unit_price_yen, avg_unit_price_yen) * 100, 2
  ) AS monthly_price_range_pct,
  
  -- データ品質指標
  CASE 
    WHEN transaction_count >= 10 AND price_stddev IS NOT NULL THEN 'HIGH'
    WHEN transaction_count >= 5 THEN 'MEDIUM'
    WHEN transaction_count >= 2 THEN 'LOW'
    ELSE 'INSUFFICIENT'
  END AS data_quality_level

FROM price_pattern_classification
WHERE year >= 2020  -- 分析対象期間