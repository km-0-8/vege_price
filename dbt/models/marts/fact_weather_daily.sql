-- 観測所別日別気象データの集計
WITH daily_weather AS (
  SELECT
    observation_date,
    observation_year,
    observation_month,
    observation_day,
    station_id,
    AVG(temperature) AS avg_temperature,
    MAX(temperature) AS max_temperature,
    MIN(temperature) AS min_temperature,
    AVG(humidity) AS avg_humidity,
    MAX(precipitation) AS max_precipitation_1h,
    SUM(precipitation) AS total_precipitation,
    AVG(wind_speed) AS avg_wind_speed,
    MAX(wind_speed) AS max_wind_speed,
    SUM(sunshine) AS total_sunshine
  FROM {{ ref('stg_weather') }}
  WHERE observation_date IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),

-- 観測所キーとの結合
with_station_key AS (
  SELECT
    w.*,
    s.station_key
  FROM daily_weather w
  JOIN {{ ref('dim_weather_station') }} s 
    ON w.station_id = s.station_id
)
SELECT * FROM with_station_key