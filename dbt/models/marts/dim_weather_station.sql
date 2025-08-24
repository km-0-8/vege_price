-- 観測所マスターの抽出
WITH stations AS (
  SELECT DISTINCT
    station_id,
    station_name
  FROM {{ ref('stg_weather') }}
  WHERE station_id IS NOT NULL
  AND station_name IS NOT NULL
),

-- 観測所情報にメタデータと特性情報を追加
station_with_metadata AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY station_name) AS station_key,
    station_id,
    station_name,
    CASE 
      WHEN station_id IN (
        '47412', '47575', '47629', '47626', '47662', '44132',
        '47636', '47610', '47656', '47772', '47765', '47807',
        '47830', '47827'
      ) THEN TRUE
      ELSE FALSE
    END AS is_agriculture_priority,
    CASE 
      WHEN station_id = '47412' THEN '北海道'
      WHEN station_id IN ('47575', '47584', '47590', '47582', '47588', '47595') THEN '東北'
      WHEN station_id IN ('47629', '47615', '47624', '47626', '47662', '44132', '46106') THEN '関東'
      WHEN station_id IN ('47604', '47607', '47605', '47616', '47638', '47610', '47632', '47656', '47636') THEN '中部'
      WHEN station_id IN ('47651', '47761', '47759', '47772', '47770', '47780', '47777') THEN '近畿'
      WHEN station_id IN ('47744', '47741', '47768', '47765', '47784') THEN '中国'
      WHEN station_id IN ('47895', '47891', '47893', '47898') THEN '四国'
      WHEN station_id IN ('47807', '47813', '47817', '47819', '47815', '47830', '47827', '47936') THEN '九州・沖縄'
      ELSE 'その他'
    END AS region,
    CASE 
      WHEN station_id = '47412' THEN 'じゃがいも、玉ねぎ、にんじん'
      WHEN station_id = '47575' THEN 'にんにく、ごぼう、大根'
      WHEN station_id = '47629' THEN '白菜、レタス、ピーマン'
      WHEN station_id = '47626' THEN 'ねぎ、ほうれん草、小松菜'
      WHEN station_id = '47662' THEN 'ほうれん草、ねぎ、にんじん'
      WHEN station_id = '44132' THEN '消費地（東京市場）'
      WHEN station_id = '47636' THEN 'キャベツ、白菜、大根'
      WHEN station_id = '47610' THEN 'レタス、白菜、キャベツ'
      WHEN station_id = '47656' THEN 'レタス、白菜、大根'
      WHEN station_id = '47772' THEN '消費地（関西圏）'
      WHEN station_id = '47765' THEN 'ほうれん草、小松菜'
      WHEN station_id = '47807' THEN 'キャベツ、ほうれん草'
      WHEN station_id = '47830' THEN 'ピーマン、きゅうり'
      WHEN station_id = '47827' THEN '大根、白菜、ピーマン'
      ELSE NULL
    END AS major_crops
  FROM stations
)
SELECT * FROM station_with_metadata