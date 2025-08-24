
-- 品目の基本統計情報を集計
WITH base_items AS (
  SELECT 
    item_name,
    major_category,
    sub_category,
    COUNT(*) AS transaction_count,
    AVG(SAFE_DIVIDE(amount_yen, NULLIF(quantity_kg, 0))) AS avg_unit_price,
    STDDEV(SAFE_DIVIDE(amount_yen, NULLIF(quantity_kg, 0))) AS price_volatility,
    MIN(SAFE_DIVIDE(amount_yen, NULLIF(quantity_kg, 0))) AS min_unit_price,
    MAX(SAFE_DIVIDE(amount_yen, NULLIF(quantity_kg, 0))) AS max_unit_price,
    SUM(quantity_kg) AS total_quantity,
    SUM(amount_yen) AS total_amount
  FROM {{ ref('stg_market') }}
  WHERE item_name IS NOT NULL
    AND quantity_kg > 0
    AND amount_yen > 0
  GROUP BY item_name, major_category, sub_category
),

-- 品目特性とカテゴリ分類の追加
item_characteristics AS (
  SELECT 
    *,
    CASE 
      WHEN avg_unit_price >= 1000 THEN '高級品'
      WHEN avg_unit_price >= 500 THEN '中級品'
      WHEN avg_unit_price >= 200 THEN '一般品'
      ELSE '低価格品'
    END AS price_tier,
    
    
    CASE 
      WHEN price_volatility / NULLIF(avg_unit_price, 0) > 0.5 THEN '高変動'
      WHEN price_volatility / NULLIF(avg_unit_price, 0) > 0.3 THEN '中変動'
      ELSE '安定'
    END AS price_stability,
    
    
    CASE 
      WHEN total_quantity >= 1000000 THEN '大量取引品目'
      WHEN total_quantity >= 100000 THEN '中量取引品目'  
      WHEN total_quantity >= 10000 THEN '小量取引品目'
      ELSE '少量取引品目'
    END AS volume_tier,
    
    
    CASE 
      WHEN item_name LIKE '%レタス%' OR item_name LIKE '%キャベツ%' 
        OR item_name LIKE '%白菜%' OR item_name LIKE '%ほうれん草%'
        OR item_name LIKE '%小松菜%' OR item_name LIKE '%チンゲン菜%' THEN '葉菜類'
      
      
      WHEN item_name LIKE '%トマト%' OR item_name LIKE '%きゅうり%'
        OR item_name LIKE '%なす%' OR item_name LIKE '%ピーマン%'
        OR item_name LIKE '%かぼちゃ%' OR item_name LIKE '%ズッキーニ%' THEN '果菜類'
      
      
      WHEN item_name LIKE '%大根%' OR item_name LIKE '%にんじん%'
        OR item_name LIKE '%かぶ%' OR item_name LIKE '%ごぼう%'
        OR item_name LIKE '%れんこん%' THEN '根菜類'
      
      
      WHEN item_name LIKE '%じゃがいも%' OR item_name LIKE '%さつまいも%'
        OR item_name LIKE '%里芋%' OR item_name LIKE '%長芋%' THEN '芋類'
      
      
      WHEN item_name LIKE '%玉ねぎ%' OR item_name LIKE '%ねぎ%' 
        OR item_name LIKE '%にんにく%' OR item_name LIKE '%しょうが%' THEN '香味野菜'
      
      
      WHEN item_name LIKE '%しいたけ%' OR item_name LIKE '%えのき%'
        OR item_name LIKE '%しめじ%' OR item_name LIKE '%まいたけ%' THEN 'きのこ類'
      
      ELSE 'その他野菜'
    END AS vegetable_category,
    
    
    CASE 
      WHEN item_name LIKE '%たけのこ%' OR item_name LIKE '%アスパラ%'
        OR item_name LIKE '%そら豆%' OR item_name LIKE '%グリンピース%' THEN '春野菜'
      
      
      WHEN item_name LIKE '%トマト%' OR item_name LIKE '%きゅうり%'
        OR item_name LIKE '%なす%' OR item_name LIKE '%ピーマン%' 
        OR item_name LIKE '%とうもろこし%' OR item_name LIKE '%オクラ%' THEN '夏野菜'
      
      
      WHEN item_name LIKE '%かぼちゃ%' OR item_name LIKE '%さつまいも%'
        OR item_name LIKE '%里芋%' OR item_name LIKE '%れんこん%' THEN '秋野菜'
      
      
      WHEN item_name LIKE '%大根%' OR item_name LIKE '%白菜%'
        OR item_name LIKE '%ほうれん草%' OR item_name LIKE '%ねぎ%'
        OR item_name LIKE '%ブロッコリー%' THEN '冬野菜'
      
      
      WHEN item_name LIKE '%キャベツ%' OR item_name LIKE '%レタス%'
        OR item_name LIKE '%玉ねぎ%' OR item_name LIKE '%にんじん%'
        OR item_name LIKE '%じゃがいも%' THEN '通年野菜'
      
      ELSE '季節性不明'
    END AS seasonality,
    
    
    CASE 
      WHEN item_name LIKE '%じゃがいも%' OR item_name LIKE '%玉ねぎ%'
        OR item_name LIKE '%にんじん%' OR item_name LIKE '%かぼちゃ%' THEN '長期保存可'
      WHEN item_name LIKE '%キャベツ%' OR item_name LIKE '%大根%'
        OR item_name LIKE '%白菜%' THEN '中期保存可'
      WHEN item_name LIKE '%レタス%' OR item_name LIKE '%ほうれん草%'
        OR item_name LIKE '%トマト%' OR item_name LIKE '%きゅうり%' THEN '短期保存'
      ELSE '保存性不明'
    END AS storage_durability,
    
    
    CASE 
      WHEN item_name LIKE '%レタス%' OR item_name LIKE '%トマト%'
        OR item_name LIKE '%きゅうり%' THEN 'サラダ・生食'
      WHEN item_name LIKE '%じゃがいも%' OR item_name LIKE '%にんじん%'
        OR item_name LIKE '%玉ねぎ%' THEN '煮物・炒め物'
      WHEN item_name LIKE '%ほうれん草%' OR item_name LIKE '%小松菜%' THEN 'おひたし・炒め物'
      ELSE '多用途'
    END AS cooking_usage
    
  FROM base_items
),

-- 最終的な品目ディメンションテーブルの生成
final_enhanced_items AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY item_name, major_category, sub_category) AS item_key,
    item_name,
    major_category,
    sub_category,
    
    
    transaction_count,
    ROUND(avg_unit_price, 2) AS avg_unit_price_yen,
    ROUND(price_volatility, 2) AS price_volatility,
    ROUND(min_unit_price, 2) AS min_unit_price_yen,
    ROUND(max_unit_price, 2) AS max_unit_price_yen,
    total_quantity AS total_quantity_kg,
    total_amount AS total_amount_yen,
    
    
    price_tier,
    price_stability,
    volume_tier,
    vegetable_category,
    seasonality,
    storage_durability,
    cooking_usage,
    
    
    ROUND((max_unit_price - min_unit_price) / NULLIF(avg_unit_price, 0), 2) AS price_range_ratio,
    ROUND(price_volatility / NULLIF(avg_unit_price, 0), 2) AS coefficient_of_variation,
    
    
    CASE WHEN transaction_count >= 10 THEN TRUE ELSE FALSE END AS is_frequent_item,
    CASE WHEN avg_unit_price > 0 AND price_volatility > 0 THEN TRUE ELSE FALSE END AS has_price_data,
    CASE WHEN seasonality != '季節性不明' THEN TRUE ELSE FALSE END AS has_seasonality,
    
    
    UPPER(item_name) AS item_name_upper,
    REGEXP_REPLACE(item_name, r'[^\p{L}\p{N}]', '') AS item_name_clean
    
  FROM item_characteristics
)
SELECT * FROM final_enhanced_items