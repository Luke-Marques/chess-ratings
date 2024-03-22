terraform {
  required_version = ">= 1.7"
  backend "local" {} # use "gcs" or "s3" to preserve tf state online"
}

provider "google" {
  project = var.project
  region  = var.region
}

# data lake bucket
# reference: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${var.project}" # concat dl bucket + project name for unique naming
  location = var.region

  # optional (recommended) options
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }
  force_destroy = true
}

# data warehouse
# reference: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}