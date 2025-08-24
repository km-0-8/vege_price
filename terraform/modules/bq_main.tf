resource "google_storage_bucket" "raw" {
  name          = "${var.dataset_prefix}-${var.environment}-raw-data"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  uniform_bucket_level_access = true
  
  versioning {
    enabled = false
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                  = "${var.dataset_prefix}_${var.environment}_raw"
  location                    = var.bq_location
  delete_contents_on_destroy  = true

  description = "生データ用データセット - ${var.environment}"
}

resource "google_bigquery_dataset" "stg" {
  dataset_id                  = "${var.dataset_prefix}_${var.environment}_stg"
  location                    = var.bq_location
  delete_contents_on_destroy  = true

  description = "ステージングデータ用データセット - ${var.environment}"
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                  = "${var.dataset_prefix}_${var.environment}_mart"
  location                    = var.bq_location
  delete_contents_on_destroy  = true

  description = "マートデータ用データセット - ${var.environment}"
}

resource "google_bigquery_table" "raw_tokyo_market" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "tokyo_market"
  deletion_protection = false
  schema = jsonencode([
    { name = "year_month_slash",     type = "STRING",  mode = "NULLABLE" },
    { name = "year_month_num",       type = "STRING",  mode = "NULLABLE" },
    { name = "year",                 type = "INTEGER", mode = "NULLABLE" },
    { name = "month",                type = "INTEGER", mode = "NULLABLE" },
    { name = "market_code",          type = "STRING",  mode = "NULLABLE" },
    { name = "market",               type = "STRING",  mode = "NULLABLE" },
    { name = "major_category_code",  type = "STRING",  mode = "NULLABLE" },
    { name = "major_category",       type = "STRING",  mode = "NULLABLE" },
    { name = "sub_category_code",    type = "STRING",  mode = "NULLABLE" },
    { name = "sub_category",         type = "STRING",  mode = "NULLABLE" },
    { name = "item_code",            type = "STRING",  mode = "NULLABLE" },
    { name = "item_name",            type = "STRING",  mode = "NULLABLE" },
    { name = "origin_code",          type = "STRING",  mode = "NULLABLE" },
    { name = "origin",               type = "STRING",  mode = "NULLABLE" },
    { name = "domestic_foreign",     type = "STRING",  mode = "NULLABLE" },
    { name = "quantity_kg",          type = "INTEGER", mode = "NULLABLE" },
    { name = "amount_yen",           type = "INTEGER", mode = "NULLABLE" }
  ])
  clustering = ["item_name", "market"]
  description = "東京都中央卸売市場の生データ（年月・品目・市場・取引量・金額）"
}

resource "google_bigquery_table" "raw_weather_hourly" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "weather_hourly"
  deletion_protection = false

  schema = jsonencode([
    { name = "year",                 type = "INTEGER",   mode = "NULLABLE" },
    { name = "month",                type = "INTEGER",   mode = "NULLABLE" },
    { name = "day",                  type = "INTEGER",   mode = "NULLABLE" },
    { name = "hour",                 type = "INTEGER",   mode = "NULLABLE" },
    { name = "precipitation",        type = "FLOAT",     mode = "NULLABLE" },
    { name = "temperature",          type = "FLOAT",     mode = "NULLABLE" },
    { name = "humidity",             type = "FLOAT",     mode = "NULLABLE" },
    { name = "wind_direction",       type = "FLOAT",     mode = "NULLABLE" },
    { name = "wind_speed",           type = "FLOAT",     mode = "NULLABLE" },
    { name = "sunshine",             type = "FLOAT",     mode = "NULLABLE" },
    { name = "station_id",           type = "STRING",    mode = "REQUIRED" },
    { name = "station_name",         type = "STRING",    mode = "NULLABLE" },
    { name = "created_at",           type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "observation_datetime", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

  time_partitioning {
    type  = "DAY"
    field = "observation_datetime"
  }
  require_partition_filter = false

  description = "時別気象観測データ"
}

resource "google_bigquery_table" "raw_ml_price_pred" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "ml_price_pred"
  deletion_protection = false

  schema = jsonencode([
    { name = "prediction_id",                 type = "STRING",    mode = "NULLABLE" },
    { name = "prediction_date",               type = "DATE",      mode = "NULLABLE" },
    { name = "prediction_year",               type = "INTEGER",   mode = "NULLABLE" },
    { name = "prediction_month",              type = "INTEGER",   mode = "NULLABLE" },
    { name = "prediction_generated_at",       type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "item_name",                     type = "STRING",    mode = "NULLABLE" },
    { name = "market",                        type = "STRING",    mode = "NULLABLE" },
    { name = "origin",                        type = "STRING",    mode = "NULLABLE" },
    { name = "model_type",                    type = "STRING",    mode = "NULLABLE" },
    { name = "model_version",                 type = "STRING",    mode = "NULLABLE" },
    { name = "predicted_price",               type = "FLOAT",     mode = "NULLABLE" },
    { name = "lower_bound_price",             type = "FLOAT",     mode = "NULLABLE" },
    { name = "upper_bound_price",             type = "FLOAT",     mode = "NULLABLE" },
    { name = "confidence_interval",           type = "FLOAT",     mode = "NULLABLE" },
    { name = "training_data_points",          type = "INTEGER",   mode = "NULLABLE" },
    { name = "training_period_start",         type = "DATE",      mode = "NULLABLE" },
    { name = "training_period_end",           type = "DATE",      mode = "NULLABLE" },
    { name = "last_actual_price",             type = "FLOAT",     mode = "NULLABLE" },
    { name = "last_actual_date",              type = "DATE",      mode = "NULLABLE" },
    { name = "model_mae",                     type = "FLOAT",     mode = "NULLABLE" },
    { name = "model_mse",                     type = "FLOAT",     mode = "NULLABLE" },
    { name = "model_rmse",                    type = "FLOAT",     mode = "NULLABLE" },
    { name = "model_r2",                      type = "FLOAT",     mode = "NULLABLE" },
    { name = "data_quality_score",            type = "FLOAT",     mode = "NULLABLE" },
    { name = "prediction_reliability",        type = "STRING",    mode = "NULLABLE" },
    { name = "created_at",                    type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "updated_at",                    type = "TIMESTAMP", mode = "NULLABLE" }
  ])

  clustering = ["item_name", "model_type", "prediction_date"]

  description = "機械学習による価格予測結果データ"
}