variable "bucket" {
  description = "Your bucket name"
  default = "datalake"
  type        = string
}

variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Your project region"
  default = "europe-west9"
  type        = string
}

variable "zone" {
  description = "Your project zone"
  default = "europe-west9-a"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default     = "STANDARD"
  type        = string
}

variable "network" {
  description = "Network for your instance/cluster"
  default     = "default"
  type        = string
}

variable "credentials" {
  description = "Your google credentials"
  type        = string
}

variable "dataset" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "london_cycles_dataset"
  type        = string
}

locals {
  topic_name = "NewCSVHires"
  notification_suffix_path = "hires/"
  gcs_path_to_dag = "dags/spark_etl.py"
  local_path_to_dag = "../scripts/spark_etl.py"
  gcs_path_to_zip = "zip/cloud_function.zip"
  local_path_to_zip = "../cloud_function/cloud_function.zip"
  dag_entry_point = "spark_submit"
  table_hires = "cycle_hires"
  table_daily_agg = "daily_agg"
}
