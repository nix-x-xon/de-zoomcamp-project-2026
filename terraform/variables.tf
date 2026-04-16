variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-central2"
}

variable "gcs_bucket_name" {
  description = "Name of the raw data lake bucket (must be globally unique)"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw loaded data"
  type        = string
  default     = "energy_raw"
}

variable "bq_dataset_marts" {
  description = "BigQuery dataset for dbt marts"
  type        = string
  default     = "energy_marts"
}
