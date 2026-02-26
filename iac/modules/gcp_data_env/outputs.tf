output "raw_bucket_name" {
  description = "Nombre del bucket RAW de Cloud Storage"
  value       = google_storage_bucket.raw.name
}

output "raw_bucket_url" {
  description = "URL gs:// del bucket RAW"
  value       = google_storage_bucket.raw.url
}

output "bigquery_dataset_id" {
  description = "ID del dataset de BigQuery"
  value       = google_bigquery_dataset.dw.dataset_id
}

output "cloud_run_url" {
  description = "URL del servicio Cloud Run"
  value       = google_cloud_run_service.daily_pipeline.status[0].url
}

output "vertex_ai_endpoint_id" {
  description = "ID del endpoint de Vertex AI"
  value       = google_vertex_ai_endpoint.example.id
}
