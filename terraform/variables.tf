locals {
  data_lake_bucket = "chess-ratings-datalake"
}

variable "project" {
  description = "Google Cloud Project ID"
  default     = "fide-chess-ratings"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-north1"
  type        = string
}

variable "credentials" {
  description = "Google Cloud Service Account key for project."
  default     = "/app/.google/sa-key.json"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "chess_ratings_data"
}

variable "TABLE_NAME" {
  description = "BigQuery Table that raw data (from GCS) will be written to"
  type        = string
  default     = "chess_ratings_table"
}