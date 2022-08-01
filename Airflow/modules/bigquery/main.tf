resource "google_bigquery_dataset" "dataset_stg" {
  dataset_id    = var.dataset_stg_name
  friendly_name = var.dataset_stg_name
  description   = "stage area"
  location      = var.location
}

resource "google_bigquery_dataset" "dataset_dw" {
  dataset_id    = var.dataset_dw_name
  friendly_name = var.dataset_dw_name
  description   = "dim and fact tables"
  location      = var.location
}
