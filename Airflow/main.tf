terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.77.0"
    }
  }
  required_version = ">= 0.13.0"
}

module "vpc" {
  source = "./modules/vpc"

  project_id = var.project_id

}

module "gke" {
  source = "./modules/gke"

  project_id    = var.project_id
  cluster_name  = "airflow-gke-data-bootcamp"
  location      = var.location
  vpc_id        = module.vpc.vpc
  subnet_id     = module.vpc.private_subnets[0]
  gke_num_nodes = var.gke_num_nodes
  machine_type  = var.machine_type

}

module "cloudsql" {
  source = "./modules/cloudsql"

  region           = var.region
  location         = var.location
  instance_name    = var.instance_name
  database_version = var.database_version
  instance_tier    = var.instance_tier
  disk_space       = var.disk_space
  database_name    = var.database_name
  db_username      = var.db_username
  db_password      = var.db_password
}

module "gcs" {
  source = "./modules/gcs"

  location        = var.region
  stg_bucket_name = var.stg_bucket_name
  instance_name   = var.instance_name
}

module "dataproc" {
  source = "./modules/dataproc"

  location     = var.location
  region       = var.region
  name         = var.dataproc_template_name
  cluster_name = var.dataproc_cluster_name
  machine_type = var.machine_type
}

module "bigquery" {
  source = "./modules/bigquery"

  dataset_stg_name = var.dataset_stage_name
  dataset_dw_name  = var.dataset_datawarehouse_name
  location         = var.region
}
