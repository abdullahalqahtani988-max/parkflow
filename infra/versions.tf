terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
  # Remote state in its own bucket (created by scripts/terraform_bootstrap.sh):
  # versioned, never in Git (state can contain resource details).
  backend "gcs" {
    bucket = "parkflow-514314-tfstate"
    prefix = "parkflow"
  }
}

provider "google" {
  project = var.project
  region  = var.region
}
