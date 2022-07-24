resource "google_storage_bucket" "auto-expire" {
  name          = var.stg_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}