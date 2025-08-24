-- 天気観測データの重複排除とクリーニング
WITH cleaned_weather_data AS (
    SELECT
      station_id,
      station_name,
      observation_datetime,
      ROUND(temperature, 1) AS temperature,
      ROUND(humidity, 1) AS humidity,
      ROUND(precipitation, 1) AS precipitation,
      ROUND(wind_speed, 1) AS wind_speed,
      ROUND(wind_direction, 1) AS wind_direction,
      ROUND(sunshine, 1) AS sunshine,
      SAFE_CAST(DATE(observation_datetime) AS DATE) AS observation_date,
      SAFE_CAST(EXTRACT(YEAR FROM observation_datetime) AS INT64) AS observation_year,
      SAFE_CAST(EXTRACT(MONTH FROM observation_datetime) AS INT64) AS observation_month,
      SAFE_CAST(EXTRACT(DAY FROM observation_datetime) AS INT64) AS observation_day,
      SAFE_CAST(EXTRACT(HOUR FROM observation_datetime) AS INT64) AS observation_hour
    FROM {{ source('vege_dev_raw', 'weather_hourly') }}
    WHERE station_name IS NOT NULL
    AND observation_datetime IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY station_id, observation_datetime 
        ORDER BY created_at DESC
    ) = 1
)
SELECT 
    station_id,
    station_name,
    observation_datetime,
    temperature,
    humidity,
    precipitation,
    wind_speed,
    wind_direction,
    sunshine,
    observation_date,
    observation_year,
    observation_month,
    observation_day,
    observation_hour
FROM cleaned_weather_data