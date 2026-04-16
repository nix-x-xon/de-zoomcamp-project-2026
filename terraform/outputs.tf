output "gcs_bucket" {
  value = google_storage_bucket.raw_data.name
}

output "bq_dataset_raw" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_marts" {
  value = google_bigquery_dataset.marts.dataset_id
}

output "spark_temp_bucket" {
  value = google_storage_bucket.spark_temp.name
}
