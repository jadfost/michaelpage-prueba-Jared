terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Backend remoto: ajustar bucket y prefix según proyecto real
  backend "gcs" {
    bucket = "tfstate-michaelpage-prueba"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ──────────────────────────────────────────────────────────────
# Entorno DEV
# ──────────────────────────────────────────────────────────────
module "dev" {
  source      = "./modules/gcp_data_env"
  environment = "dev"
  project_id  = var.project_id
  region      = var.region
  prefix      = var.prefix
  owner       = var.owner
}

# ──────────────────────────────────────────────────────────────
# Entorno QA  (diseñado; aplicar con: terraform apply -target=module.qa)
# ──────────────────────────────────────────────────────────────
module "qa" {
  source      = "./modules/gcp_data_env"
  environment = "qa"
  project_id  = var.project_id
  region      = var.region
  prefix      = var.prefix
  owner       = var.owner
}

# ──────────────────────────────────────────────────────────────
# Entorno PROD
# ──────────────────────────────────────────────────────────────
module "prod" {
  source      = "./modules/gcp_data_env"
  environment = "prod"
  project_id  = var.project_id
  region      = var.region
  prefix      = var.prefix
  owner       = var.owner
}
