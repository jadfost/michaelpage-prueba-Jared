locals {
  env_short = var.environment == "production" ? "prd" : var.environment
  common_labels = {
    environment = var.environment
    owner       = var.owner
    managed_by  = "terraform"
    purpose     = "data-platform"
  }
}

# ──────────────────────────────────────────────────────────────
# APIs habilitadas
# ──────────────────────────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "run.googleapis.com",
    "aiplatform.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ──────────────────────────────────────────────────────────────
# Cloud Storage – bucket RAW por entorno
# ──────────────────────────────────────────────────────────────
resource "google_storage_bucket" "raw" {
  name          = "raw-${local.env_short}-${var.project_id}"
  project       = var.project_id
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = var.environment == "prod" ? 365 : 90 }
  }

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# BigQuery – dataset por entorno
# ──────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "dw" {
  dataset_id                 = "dw_${var.environment}"
  project                    = var.project_id
  location                   = var.bigquery_location
  friendly_name              = "Data Warehouse – ${var.environment}"
  description                = "Dataset principal del entorno ${var.environment}"
  delete_contents_on_destroy = var.environment != "prod"

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# BigQuery Connection – para tablas externas BigLake sobre GCS
# ──────────────────────────────────────────────────────────────
resource "google_bigquery_connection" "gcs_biglake" {
  connection_id = "biglake-gcs-${var.environment}"
  project       = var.project_id
  location      = var.bigquery_location
  friendly_name = "BigLake GCS – ${var.environment}"
  description   = "Conexión para tablas externas Delta Lake en GCS (${var.environment})"

  cloud_resource {}

  depends_on = [google_project_service.apis]
}

# Permiso: la SA de la conexión BigLake puede leer el bucket GCS
resource "google_storage_bucket_iam_member" "biglake_reader" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.gcs_biglake.cloud_resource[0].service_account_id}"

  depends_on = [google_bigquery_connection.gcs_biglake]
}

# ──────────────────────────────────────────────────────────────
# Cloud Run – pipeline diario de datos (simulado)
# ──────────────────────────────────────────────────────────────
resource "google_cloud_run_service" "daily_pipeline" {
  name     = "${var.prefix}-daily-pipeline-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    metadata {
      labels = local.common_labels
      annotations = {
        "autoscaling.knative.dev/minScale" = "0"
        "autoscaling.knative.dev/maxScale" = var.environment == "prod" ? "10" : "3"
      }
    }

    spec {
      containers {
        image = var.cloud_run_image
        env {
          name  = "ENV"
          value = var.environment
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.raw.name
        }
        env {
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.dw.dataset_id
        }
        resources {
          limits = {
            cpu    = "1"
            memory = var.environment == "prod" ? "1Gi" : "512Mi"
          }
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# Vertex AI – endpoint mínimo de ejemplo
# ──────────────────────────────────────────────────────────────
resource "google_vertex_ai_endpoint" "example" {
  name         = "${var.prefix}-endpoint-${var.environment}"
  display_name = "Example Endpoint – ${var.environment}"
  location     = var.region
  project      = var.project_id

  labels = local.common_labels

  depends_on = [google_project_service.apis]
}
