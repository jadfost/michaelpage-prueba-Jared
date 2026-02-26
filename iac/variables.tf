variable "project_id" {
  description = "ID del proyecto GCP donde se despliegan los recursos"
  type        = string
}

variable "region" {
  description = "Región principal de GCP"
  type        = string
  default     = "us-central1"
}

variable "prefix" {
  description = "Prefijo organizacional para nombres de recursos (ej: myorg, acme)"
  type        = string
  default     = "myorg"
}

variable "owner" {
  description = "Equipo propietario de los recursos"
  type        = string
  default     = "data-team"
}
