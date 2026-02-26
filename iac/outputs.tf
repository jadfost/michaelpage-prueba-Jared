output "dev_raw_bucket" {
  value = module.dev.raw_bucket_name
}

output "dev_bigquery_dataset" {
  value = module.dev.bigquery_dataset_id
}

output "dev_cloud_run_url" {
  value = module.dev.cloud_run_url
}

output "prod_raw_bucket" {
  value = module.prod.raw_bucket_name
}

output "prod_bigquery_dataset" {
  value = module.prod.bigquery_dataset_id
}

output "prod_cloud_run_url" {
  value = module.prod.cloud_run_url
}
