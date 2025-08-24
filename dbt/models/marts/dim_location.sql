
-- 産地データのクリーニングと抽出
WITH origin_data AS (
  SELECT DISTINCT 
    origin,
    REGEXP_REPLACE(UPPER(origin), r'[県府道都]$', '') AS cleaned_origin
  FROM {{ ref('stg_market') }}
  WHERE origin IS NOT NULL 
    AND origin != ''
    AND origin NOT LIKE '%その他%'
    AND origin NOT LIKE '%計%'
),

-- 産地名から都道府県への標準化マッピング
prefecture_mapping AS (
  SELECT 
    origin,
    cleaned_origin,
    CASE 
      WHEN cleaned_origin LIKE '%北海道%' OR cleaned_origin = '北海道' THEN 'Hokkaido'
      WHEN cleaned_origin LIKE '%青森%' OR cleaned_origin = '青森' THEN 'Aomori'
      WHEN cleaned_origin LIKE '%岩手%' OR cleaned_origin = '岩手' THEN '岩手県' 
      WHEN cleaned_origin LIKE '%宮城%' OR cleaned_origin = '宮城' THEN '宮城県'
      WHEN cleaned_origin LIKE '%秋田%' OR cleaned_origin = '秋田' THEN '秋田県'
      WHEN cleaned_origin LIKE '%山形%' OR cleaned_origin = '山形' THEN '山形県'
      WHEN cleaned_origin LIKE '%福島%' OR cleaned_origin = '福島' THEN '福島県'
      WHEN cleaned_origin LIKE '%茨城%' OR cleaned_origin = '茨城' THEN '茨城県'
      WHEN cleaned_origin LIKE '%栃木%' OR cleaned_origin = '栃木' THEN '栃木県'
      WHEN cleaned_origin LIKE '%群馬%' OR cleaned_origin = '群馬' THEN '群馬県'
      WHEN cleaned_origin LIKE '%埼玉%' OR cleaned_origin = '埼玉' THEN '埼玉県'
      WHEN cleaned_origin LIKE '%千葉%' OR cleaned_origin = '千葉' THEN '千葉県'
      WHEN cleaned_origin LIKE '%東京%' OR cleaned_origin = '東京' THEN '東京都'
      WHEN cleaned_origin LIKE '%神奈川%' OR cleaned_origin = '神奈川' THEN '神奈川県'
      WHEN cleaned_origin LIKE '%新潟%' OR cleaned_origin = '新潟' THEN '新潟県'
      WHEN cleaned_origin LIKE '%富山%' OR cleaned_origin = '富山' THEN '富山県'
      WHEN cleaned_origin LIKE '%石川%' OR cleaned_origin = '石川' THEN '石川県'
      WHEN cleaned_origin LIKE '%福井%' OR cleaned_origin = '福井' THEN '福井県'
      WHEN cleaned_origin LIKE '%山梨%' OR cleaned_origin = '山梨' THEN '山梨県'
      WHEN cleaned_origin LIKE '%長野%' OR cleaned_origin = '長野' THEN '長野県'
      WHEN cleaned_origin LIKE '%岐阜%' OR cleaned_origin = '岐阜' THEN '岐阜県'
      WHEN cleaned_origin LIKE '%静岡%' OR cleaned_origin = '静岡' THEN '静岡県'
      WHEN cleaned_origin LIKE '%愛知%' OR cleaned_origin = '愛知' THEN '愛知県'
      WHEN cleaned_origin LIKE '%三重%' OR cleaned_origin = '三重' THEN '三重県'
      WHEN cleaned_origin LIKE '%滋賀%' OR cleaned_origin = '滋賀' THEN '滋賀県'
      WHEN cleaned_origin LIKE '%京都%' OR cleaned_origin = '京都' THEN '京都府'
      WHEN cleaned_origin LIKE '%大阪%' OR cleaned_origin = '大阪' THEN '大阪府'
      WHEN cleaned_origin LIKE '%兵庫%' OR cleaned_origin = '兵庫' THEN '兵庫県'
      WHEN cleaned_origin LIKE '%奈良%' OR cleaned_origin = '奈良' THEN '奈良県'
      WHEN cleaned_origin LIKE '%和歌山%' OR cleaned_origin = '和歌山' THEN '和歌山県'
      WHEN cleaned_origin LIKE '%鳥取%' OR cleaned_origin = '鳥取' THEN '鳥取県'
      WHEN cleaned_origin LIKE '%島根%' OR cleaned_origin = '島根' THEN '島根県'
      WHEN cleaned_origin LIKE '%岡山%' OR cleaned_origin = '岡山' THEN '岡山県'
      WHEN cleaned_origin LIKE '%広島%' OR cleaned_origin = '広島' THEN '広島県'
      WHEN cleaned_origin LIKE '%山口%' OR cleaned_origin = '山口' THEN '山口県'
      WHEN cleaned_origin LIKE '%徳島%' OR cleaned_origin = '徳島' THEN '徳島県'
      WHEN cleaned_origin LIKE '%香川%' OR cleaned_origin = '香川' THEN '香川県'
      WHEN cleaned_origin LIKE '%愛媛%' OR cleaned_origin = '愛媛' THEN '愛媛県'
      WHEN cleaned_origin LIKE '%高知%' OR cleaned_origin = '高知' THEN '高知県'
      WHEN cleaned_origin LIKE '%福岡%' OR cleaned_origin = '福岡' THEN '福岡県'
      WHEN cleaned_origin LIKE '%佐賀%' OR cleaned_origin = '佐賀' THEN '佐賀県'
      WHEN cleaned_origin LIKE '%長崎%' OR cleaned_origin = '長崎' THEN '長崎県'
      WHEN cleaned_origin LIKE '%熊本%' OR cleaned_origin = '熊本' THEN '熊本県'
      WHEN cleaned_origin LIKE '%大分%' OR cleaned_origin = '大分' THEN '大分県'
      WHEN cleaned_origin LIKE '%宮崎%' OR cleaned_origin = '宮崎' THEN '宮崎県'
      WHEN cleaned_origin LIKE '%鹿児島%' OR cleaned_origin = '鹿児島' THEN '鹿児島県'
      WHEN cleaned_origin LIKE '%沖縄%' OR cleaned_origin = '沖縄' THEN '沖縄県'
      WHEN cleaned_origin LIKE '%輸入%' OR cleaned_origin LIKE '%外国%' THEN '輸入'
      ELSE '不明'
    END AS prefecture
  FROM origin_data
),

-- 都道府県と代表気象観測所のマッピング
weather_station_mapping AS (
  SELECT 
    prefecture,
    CASE prefecture
      WHEN '北海道' THEN '47412'      -- 札幌
      WHEN '青森県' THEN '47575'      -- 青森
      WHEN '岩手県' THEN '47584'      -- 盛岡
      WHEN '宮城県' THEN '47590'      -- 仙台
      WHEN '秋田県' THEN '47582'      -- 秋田
      WHEN '山形県' THEN '47588'      -- 山形
      WHEN '福島県' THEN '47595'      -- 福島
      WHEN '茨城県' THEN '47629'      -- 水戸
      WHEN '栃木県' THEN '47615'      -- 宇都宮
      WHEN '群馬県' THEN '47624'      -- 前橋
      WHEN '埼玉県' THEN '47626'      -- 熊谷
      WHEN '千葉県' THEN '47662'      -- 千葉
      WHEN '東京都' THEN '44132'      -- 東京
      WHEN '神奈川県' THEN '46106'    -- 横浜
      WHEN '新潟県' THEN '47604'      -- 新潟
      WHEN '富山県' THEN '47607'      -- 富山
      WHEN '石川県' THEN '47605'      -- 金沢
      WHEN '福井県' THEN '47616'      -- 福井
      WHEN '山梨県' THEN '47638'      -- 甲府
      WHEN '長野県' THEN '47610'      -- 長野
      WHEN '岐阜県' THEN '47632'      -- 岐阜
      WHEN '静岡県' THEN '47656'      -- 静岡
      WHEN '愛知県' THEN '47636'      -- 名古屋
      WHEN '三重県' THEN '47651'      -- 津
      WHEN '滋賀県' THEN '47761'      -- 彦根
      WHEN '京都府' THEN '47759'      -- 京都
      WHEN '大阪府' THEN '47772'      -- 大阪
      WHEN '兵庫県' THEN '47770'      -- 神戸
      WHEN '奈良県' THEN '47780'      -- 奈良
      WHEN '和歌山県' THEN '47777'    -- 和歌山
      WHEN '鳥取県' THEN '47744'      -- 鳥取
      WHEN '島根県' THEN '47741'      -- 松江
      WHEN '岡山県' THEN '47768'      -- 岡山
      WHEN '広島県' THEN '47765'      -- 広島
      WHEN '山口県' THEN '47784'      -- 下関
      WHEN '徳島県' THEN '47895'      -- 徳島
      WHEN '香川県' THEN '47891'      -- 高松
      WHEN '愛媛県' THEN '47893'      -- 松山
      WHEN '高知県' THEN '47898'      -- 高知
      WHEN '福岡県' THEN '47807'      -- 福岡
      WHEN '佐賀県' THEN '47813'      -- 佐賀
      WHEN '長崎県' THEN '47817'      -- 長崎
      WHEN '熊本県' THEN '47819'      -- 熊本
      WHEN '大分県' THEN '47815'      -- 大分
      WHEN '宮崎県' THEN '47830'      -- 宮崎
      WHEN '鹿児島県' THEN '47827'    -- 鹿児島
      WHEN '沖縄県' THEN '47936'      -- 那覇
      ELSE NULL
    END AS station_id,
    CASE prefecture
      WHEN '北海道' THEN '札幌'
      WHEN '青森県' THEN '青森'
      WHEN '岩手県' THEN '盛岡'
      WHEN '宮城県' THEN '仙台'
      WHEN '秋田県' THEN '秋田'
      WHEN '山形県' THEN '山形'
      WHEN '福島県' THEN '福島'
      WHEN '茨城県' THEN '水戸'
      WHEN '栃木県' THEN '宇都宮'
      WHEN '群馬県' THEN '前橋'
      WHEN '埼玉県' THEN '熊谷'
      WHEN '千葉県' THEN '千葉'
      WHEN '東京都' THEN '東京'
      WHEN '神奈川県' THEN '横浜'
      WHEN '新潟県' THEN '新潟'
      WHEN '富山県' THEN '富山'
      WHEN '石川県' THEN '金沢'
      WHEN '福井県' THEN '福井'
      WHEN '山梨県' THEN '甲府'
      WHEN '長野県' THEN '長野'
      WHEN '岐阜県' THEN '岐阜'
      WHEN '静岡県' THEN '静岡'
      WHEN '愛知県' THEN '名古屋'
      WHEN '三重県' THEN '津'
      WHEN '滋賀県' THEN '彦根'
      WHEN '京都府' THEN '京都'
      WHEN '大阪府' THEN '大阪'
      WHEN '兵庫県' THEN '神戸'
      WHEN '奈良県' THEN '奈良'
      WHEN '和歌山県' THEN '和歌山'
      WHEN '鳥取県' THEN '鳥取'
      WHEN '島根県' THEN '松江'
      WHEN '岡山県' THEN '岡山'
      WHEN '広島県' THEN '広島'
      WHEN '山口県' THEN '下関'
      WHEN '徳島県' THEN '徳島'
      WHEN '香川県' THEN '高松'
      WHEN '愛媛県' THEN '松山'
      WHEN '高知県' THEN '高知'
      WHEN '福岡県' THEN '福岡'
      WHEN '佐賀県' THEN '佐賀'
      WHEN '長崎県' THEN '長崎'
      WHEN '熊本県' THEN '熊本'
      WHEN '大分県' THEN '大分'
      WHEN '宮崎県' THEN '宮崎'
      WHEN '鹿児島県' THEN '鹿児島'
      WHEN '沖縄県' THEN '那覇'
      ELSE NULL
    END AS station_name,
    CASE 
      WHEN prefecture IN (
        '北海道', '青森県', '茨城県', '埼玉県', '千葉県', '東京都', 
        '愛知県', '長野県', '静岡県', '大阪府', '広島県', '福岡県', '宮崎県', '鹿児島県'
      ) THEN TRUE
      ELSE FALSE
    END AS is_agriculture_priority
  FROM (SELECT DISTINCT prefecture FROM prefecture_mapping WHERE prefecture != '不明') pref
),

-- 最終的な産地ディメンションテーブルの生成
final_mapping AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY pm.origin) AS location_key,
    pm.origin AS origin_name,
    pm.cleaned_origin,
    pm.prefecture,
    wsm.station_id,
    wsm.station_name,
    COALESCE(wsm.is_agriculture_priority, FALSE) AS is_agriculture_priority,
    CASE 
      WHEN pm.prefecture IN ('北海道') THEN '北海道'
      WHEN pm.prefecture IN ('青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県') THEN '東北'
      WHEN pm.prefecture IN ('茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県') THEN '関東'
      WHEN pm.prefecture IN ('新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県', '静岡県', '愛知県') THEN '中部'
      WHEN pm.prefecture IN ('三重県', '滋賀県', '京都府', '大阪府', '兵庫県', '奈良県', '和歌山県') THEN '近畿'
      WHEN pm.prefecture IN ('鳥取県', '島根県', '岡山県', '広島県', '山口県') THEN '中国'
      WHEN pm.prefecture IN ('徳島県', '香川県', '愛媛県', '高知県') THEN '四国'
      WHEN pm.prefecture IN ('福岡県', '佐賀県', '長崎県', '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県') THEN '九州・沖縄'
      WHEN pm.prefecture = '輸入' THEN '輸入'
      ELSE '不明'
    END AS region,
    CASE 
      WHEN wsm.station_id IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS has_weather_data
  FROM prefecture_mapping pm
  LEFT JOIN weather_station_mapping wsm ON pm.prefecture = wsm.prefecture
)
SELECT * FROM final_mapping
