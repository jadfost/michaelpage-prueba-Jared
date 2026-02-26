variable "environment" {
  description = "Nombre del entorno: dev, qa, prod"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prod"], var.environment)
    error_message = "El entorno debe ser dev, qa o prod."
  }
}

variable "project_id" {
  description = "ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "Región de GCP para los recursos"
  type        = string
  default     = "us-central1"
}

variable "prefix" {
  description = "Prefijo para los nombres de recursos"
  type        = string
  default     = "myorg"
}

variable "owner" {
  description = "Equipo o persona responsable de los recursos"
  type        = string
  default     = "data-team"
}

variable "cloud_run_image" {
  description = "Imagen Docker para el servicio Cloud Run (pipeline diario)"
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

variable "bigquery_location" {
  description = "Localización del dataset de BigQuery"
  type        = string
  default     = "US"
}
