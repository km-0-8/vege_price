output "raw_dataset_id" {
  description = "Raw データセット ID"
  value       = module.bq_stack.raw_dataset_id
}

output "stg_dataset_id" {
  description = "Staging データセット ID"
  value       = module.bq_stack.stg_dataset_id
}

output "mart_dataset_id" {
  description = "Mart データセット ID"
  value       = module.bq_stack.mart_dataset_id
}

output "gcs_bucket_name" {
  description = "GCS バケット名"
  value       = module.bq_stack.gcs_bucket_name
}

output "project_setup_summary" {
  description = "プロジェクトセットアップ概要"
  value = {
    project_id  = var.project_id
    environment = var.environment
    region      = var.region
    bq_location = var.bq_location
  }
}
