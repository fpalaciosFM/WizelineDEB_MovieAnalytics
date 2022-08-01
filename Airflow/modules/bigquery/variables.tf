variable "dataset_stg_name" {
  description = "dataset name for stage area"
  default     = "stg"
}

variable "dataset_dw_name" {
  description = "dataset name for dim and fact tables"
  default     = "dw"
}

variable "location" {
  description = "value"
  default     = "us-west1"
}
