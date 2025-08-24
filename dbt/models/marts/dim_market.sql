SELECT
  market,
  DENSE_RANK() OVER (ORDER BY market) AS market_key
FROM (
  SELECT DISTINCT market
  FROM {{ ref('stg_market') }}
)