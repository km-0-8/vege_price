
-- 日付スパインテーブルの生成（2020年から2030年までの月次データ）
WITH date_spine AS (
  SELECT
    year,
    month,
    DATE(year, month, 1) AS month_start_date,
    DATE(year, month, EXTRACT(DAY FROM LAST_DAY(DATE(year, month, 1)))) AS month_end_date
  FROM (
    SELECT year
    FROM UNNEST(GENERATE_ARRAY(2020, 2030)) AS year
  ) years
  CROSS JOIN (
    SELECT month  
    FROM UNNEST(GENERATE_ARRAY(1, 12)) AS month
  ) months
),

-- 時間ディメンションの属性情報を追加
time_attributes AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY year, month) AS time_key,
    year,
    month,
    month_start_date,
    month_end_date,
    
    
    CONCAT(CAST(year AS STRING), LPAD(CAST(month AS STRING), 2, '0')) AS year_month_str,
    CONCAT(CAST(year AS STRING), '/', CAST(month AS STRING)) AS year_month_slash,
    FORMAT_DATE('%Y-%m', month_start_date) AS year_month_dash,
    
    
    CASE 
      WHEN month IN (12, 1, 2) THEN '冬'
      WHEN month IN (3, 4, 5) THEN '春' 
      WHEN month IN (6, 7, 8) THEN '夏'
      WHEN month IN (9, 10, 11) THEN '秋'
    END AS season_japanese,
    
    CASE 
      WHEN month IN (12, 1, 2) THEN 'Winter'
      WHEN month IN (3, 4, 5) THEN 'Spring'
      WHEN month IN (6, 7, 8) THEN 'Summer'
      WHEN month IN (9, 10, 11) THEN 'Autumn'
    END AS season_english,
    
    CASE 
      WHEN month IN (12, 1, 2) THEN 4
      WHEN month IN (3, 4, 5) THEN 1
      WHEN month IN (6, 7, 8) THEN 2
      WHEN month IN (9, 10, 11) THEN 3
    END AS season_number,
    
    
    CASE 
      WHEN month IN (1, 2, 3) THEN 1
      WHEN month IN (4, 5, 6) THEN 2
      WHEN month IN (7, 8, 9) THEN 3
      WHEN month IN (10, 11, 12) THEN 4
    END AS quarter,
    
    
    CASE 
      WHEN month IN (3, 4, 5) THEN '春作'
      WHEN month IN (6, 7, 8) THEN '夏作'
      WHEN month IN (9, 10, 11) THEN '秋作'  
      WHEN month IN (12, 1, 2) THEN '冬作'
    END AS agricultural_season,
    
    
    CASE month
      WHEN 1 THEN '1月'
      WHEN 2 THEN '2月'
      WHEN 3 THEN '3月'
      WHEN 4 THEN '4月'
      WHEN 5 THEN '5月'
      WHEN 6 THEN '6月'
      WHEN 7 THEN '7月'
      WHEN 8 THEN '8月'
      WHEN 9 THEN '9月'
      WHEN 10 THEN '10月'
      WHEN 11 THEN '11月'
      WHEN 12 THEN '12月'
    END AS month_name_japanese,
    
    CASE month
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
    END AS month_name_english,
    
    
    CASE month
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Feb'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Apr'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Aug'
      WHEN 9 THEN 'Sep'
      WHEN 10 THEN 'Oct'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dec'
    END AS month_abbr,
    
    
    CASE 
      WHEN month >= 4 THEN year
      ELSE year - 1
    END AS fiscal_year,
    
    
    CASE 
      WHEN month IN (4, 5, 6) THEN 1
      WHEN month IN (7, 8, 9) THEN 2
      WHEN month IN (10, 11, 12) THEN 3
      WHEN month IN (1, 2, 3) THEN 4
    END AS fiscal_quarter,
    
    
    CASE 
      WHEN month IN (6, 7, 8) THEN '夏野菜需要期'
      
      WHEN month IN (12, 1, 2) THEN '冬野菜需要期'
      
      WHEN month IN (4, 5) THEN '春端境期'
      WHEN month IN (9, 10) THEN '秋端境期'
      
      ELSE '通常期'
    END AS vegetable_demand_period,
    
    
    CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_year,
    CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1 THEN TRUE ELSE FALSE END AS is_previous_year,
    CASE WHEN DATE(year, month, 1) <= CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_historical
    
  FROM date_spine
),

-- 時系列比較用のリファレンス情報を追加
comparison_periods AS (
  SELECT 
    ta.*,
    
    
    LAG(time_key, 12) OVER (ORDER BY year, month) AS previous_year_same_month_key,
    
    
    LAG(time_key, 1) OVER (ORDER BY year, month) AS previous_month_key,
    
    
    LEAD(time_key, 1) OVER (ORDER BY year, month) AS next_month_key,
    
    
    LEAD(time_key, 12) OVER (ORDER BY year, month) AS next_year_same_month_key
    
  FROM time_attributes ta
)
SELECT * FROM comparison_periods