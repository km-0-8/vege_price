output "raw_dataset_id" {
  description = "Raw データセット ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "stg_dataset_id" {
  description = "Staging データセット ID"
  value       = google_bigquery_dataset.stg.dataset_id
}

output "mart_dataset_id" {
  description = "Mart データセット ID"
  value       = google_bigquery_dataset.mart.dataset_id
}


output "gcs_bucket_name" {
  description = "GCS バケット名"
  value       = google_storage_bucket.raw.name
}

output "raw_tokyo_market_table_id" {
  description = "Raw 東京市場テーブル ID"
  value       = google_bigquery_table.raw_tokyo_market.table_id
}

output "raw_weather_hourly_table_id" {
  description = "Raw 気象観測テーブル ID"
  value       = google_bigquery_table.raw_weather_hourly.table_id
}

output "raw_ml_price_pred_table_id" {
  description = "Raw ML価格予測テーブル ID"
  value       = google_bigquery_table.raw_ml_price_pred.table_id
}
