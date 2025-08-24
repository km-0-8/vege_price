-- 野菜市場データの基本的なクリーニングと型変換
WITH cleaned_market_data AS (
    SELECT
      SAFE_CAST(year   AS INT64)                           AS year,
      SAFE_CAST(month  AS INT64)                           AS month,
      market,
      REGEXP_REPLACE(major_category, '[ 　]+', '')         AS major_category,
      sub_category,
      item_name,
      origin,
      SAFE_CAST(quantity_kg AS INT64)                      AS quantity_kg,
      SAFE_CAST(amount_yen  AS INT64)                      AS amount_yen,
    FROM {{ source('vege_dev_raw', 'tokyo_market') }}
    WHERE REGEXP_REPLACE(major_category, '[ 　]+', '') = '野菜'
)
SELECT * FROM cleaned_market_data