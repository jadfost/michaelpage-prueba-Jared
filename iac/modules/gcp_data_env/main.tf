locals {
  env_short = var.environment
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
    "artifactregistry.googleapis.com",
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ──────────────────────────────────────────────────────────────
# Cloud Storage – bucket RAW (us-central1)
# Almacena datos crudos, Delta Lake GCS y staging de materialización.
# ──────────────────────────────────────────────────────────────
resource "google_storage_bucket" "raw" {
  name          = "raw-${local.env_short}-${var.project_id}"
  project       = var.project_id
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true
  versioning { enabled = true }

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = var.environment == "prod" ? 365 : 90 }
  }

  labels     = local.common_labels
  depends_on = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# BigQuery – dataset principal en US
# Contiene tablas nativas: transactions_staging, customers, final_table.
# ──────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "dw" {
  dataset_id                 = "dw_${var.environment}"
  project                    = var.project_id
  location                   = "US"
  friendly_name              = "Data Warehouse – ${var.environment}"
  description                = "Dataset principal del entorno ${var.environment}. Tablas nativas para MERGE y tablas de staging."
  delete_contents_on_destroy = var.environment != "prod"
  labels                     = local.common_labels
  depends_on                 = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# BigQuery – dataset Omni en azure-eastus2
#
# BigQuery Omni requiere que el dataset esté en la misma región
# que la conexión (adls-biglake-conn vive en azure-eastus2).
# RESTRICCIÓN de BigQuery Omni: en este dataset SOLO se pueden
# crear tablas externas — no tablas nativas ni destinos de MERGE.
# Por eso existe el paso de materialización: los datos se exportan
# a GCS y se cargan en dw_{env} (US) antes del MERGE.
# ──────────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "dw_omni" {
  dataset_id                 = "dw_${var.environment}_omni"
  project                    = var.project_id
  location                   = "azure-eastus2"
  friendly_name              = "Data Warehouse Omni – ${var.environment}"
  description                = "Dataset azure-eastus2 para tablas externas sobre ADLS Gen2 vía BigQuery Omni. Solo tablas externas."
  delete_contents_on_destroy = var.environment != "prod"
  labels                     = local.common_labels
  depends_on                 = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# Cloud Run – pipeline diario de datos
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
  display_name = "Example Endpoint - ${var.environment}"
  location     = var.region
  project      = var.project_id
  labels       = local.common_labels
  depends_on   = [google_project_service.apis]
}

# ──────────────────────────────────────────────────────────────
# Artifact Registry – repositorio Docker por entorno
# ──────────────────────────────────────────────────────────────
resource "google_artifact_registry_repository" "docker" {
  repository_id = "mp-images-${var.environment}"
  project       = var.project_id
  location      = var.region
  format        = "DOCKER"
  description   = "Imágenes Docker del entorno ${var.environment}"
  labels        = local.common_labels
  depends_on    = [google_project_service.apis]
}
