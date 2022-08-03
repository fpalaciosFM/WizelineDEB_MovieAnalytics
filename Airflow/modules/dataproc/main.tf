resource "google_dataproc_workflow_template" "template" {
  name     = "${var.name}-movie-review"
  location = var.region
  placement {
    managed_cluster {
      cluster_name = "${var.cluster_name}-movie-review"
      config {
        gce_cluster_config {
          zone = var.location
        }
        master_config {
          num_instances = 1
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 30
          }
        }

        worker_config {
          num_instances = 2
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_size_gb = 30
            num_local_ssds    = 1
          }
        }

        software_config {
          image_version = "2.0-debian10"
        }
      }
    }
  }
  jobs {
    step_id = "movie_review_identify_positives"
    pyspark_job {
      main_python_file_uri = "gs://wizeline-deb-movie-analytics-fpa/Dataproc/movie_review_identify_positives.py"
    }
  }
}

resource "google_dataproc_workflow_template" "template_log_review" {
  name     = "${var.name}-log-review"
  location = var.region
  placement {
    managed_cluster {
      cluster_name = "${var.cluster_name}-log-review"
      config {
        gce_cluster_config {
          zone = var.location
        }
        master_config {
          num_instances = 1
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 30
          }
        }

        worker_config {
          num_instances = 2
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_size_gb = 30
            num_local_ssds    = 1
          }
        }

        software_config {
          image_version = "2.0-debian10"
        }
      }
    }
  }
  jobs {
    step_id = "log_review_expand_xml"
    pyspark_job {
      main_python_file_uri = "gs://wizeline-deb-movie-analytics-fpa/Dataproc/log_review_expand_xml.py"
    }
  }
}

resource "google_dataproc_workflow_template" "template_user_purchase" {
  name     = "${var.name}-user-purchase"
  location = var.region
  placement {
    managed_cluster {
      cluster_name = "${var.cluster_name}-user-purchase"
      config {
        gce_cluster_config {
          zone = var.location
        }

        master_config {
          num_instances = 1
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_type    = "pd-ssd"
            boot_disk_size_gb = 30
          }
        }

        worker_config {
          num_instances = 2
          machine_type  = "n1-standard-2"
          disk_config {
            boot_disk_size_gb = 30
            num_local_ssds    = 1
          }
        }

        software_config {
          image_version = "2.0-debian10"
        }
      }
    }
  }
  jobs {
    step_id = "user_purchase_extract"
    pyspark_job {
      main_python_file_uri = "gs://wizeline-deb-movie-analytics-fpa/Dataproc/user_purchase_extract.py"
    }
  }
}
