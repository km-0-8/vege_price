variable "project_id" {
  description = "GCP プロジェクト ID"
  type        = string
}

variable "region" {
  description = "GCP リージョン（GCS用）"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "環境名 (dev, stg, prod)"
  type        = string
  default     = "dev"
}

variable "dataset_prefix" {
  description = "データセット名のプレフィックス"
  type        = string
  default     = "vege"
}

variable "bq_location" {
  description = "BigQuery データセットのロケーション（マルチリージョン）"
  type        = string
  default     = "US"
}

