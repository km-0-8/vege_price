variable "project_id" {
  description = "GCP プロジェクト ID"
  type        = string
}

variable "region" {
  description = "GCP リージョン"
  type        = string
}

variable "environment" {
  description = "環境名 (dev, stg, prod)"
  type        = string
}

variable "bq_location" {
  description = "BigQuery データセットのロケーション"
  type        = string
}

variable "dataset_prefix" {
  description = "データセット名のプレフィックス"
  type        = string
}
