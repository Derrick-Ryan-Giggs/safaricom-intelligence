output "gcs_bucket_url" {
  description = "GCS data lake bucket URL"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "bq_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}"
}

output "bq_staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.staging.dataset_id}"
}

output "bq_mart_dataset" {
  description = "BigQuery mart dataset ID"
  value       = "${var.project_id}.${google_bigquery_dataset.mart.dataset_id}"
}

output "raw_tables" {
  description = "All four raw table full IDs"
  value = {
    company_overview  = "${var.project_id}.raw.company_overview"
    mpesa_metrics     = "${var.project_id}.raw.mpesa_metrics"
    revenue_segments  = "${var.project_id}.raw.revenue_segments"
    kenya_ethiopia    = "${var.project_id}.raw.kenya_ethiopia"
  }
}