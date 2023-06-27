terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
  credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

resource "google_storage_bucket" "bucket" {
    name = "${var.bucket}_${var.project}"
    location = var.region
    force_destroy = true

    uniform_bucket_level_access = true

    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
        age = 30 # days
      }
    }
}

resource "google_dataproc_cluster" "multinode_spark_cluster" {
  name   = "multinode-spark-cluster"
  region = var.region

  cluster_config {

    staging_bucket = "${var.bucket}_${var.project}"

    gce_cluster_config {
      network = var.network
      zone    = var.zone

      shielded_instance_config {
        enable_secure_boot = true
      }
    }

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 30
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

  }
  depends_on = [google_storage_bucket.bucket]
}

resource "google_bigquery_dataset" "stg_dataset" {
  dataset_id                 = var.dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}

data "google_storage_project_service_account" "gcs_account" {
}

resource "google_pubsub_topic_iam_binding" "binding" {
  topic   = google_pubsub_topic.topic.id
  role    = "roles/pubsub.publisher"
  members = ["serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"]
}

resource "google_pubsub_topic" "topic" {
  name = local.topic_name
}

resource "google_storage_notification" "notification" {
  bucket = "${var.bucket}_${var.project}"
  payload_format = "JSON_API_V1"

  topic = google_pubsub_topic.topic.id

  object_name_prefix =local.notification_suffix_path
  event_types       = ["OBJECT_FINALIZE"]

  depends_on = [google_pubsub_topic_iam_binding.binding, google_storage_bucket.bucket]
}


resource "google_cloudfunctions_function" "my_function" {
  name         = "spark_etl"
  description  = "Spark ETL"
  region       = "europe-west6"
  runtime      = "python310"
  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = local.gcs_path_to_zip
  entry_point  = local.dag_entry_point

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = "projects/${var.project}/topics/${local.topic_name}"
  }

  available_memory_mb = 256
  timeout             = 540

  environment_variables = {
    "PROJECT" = var.project
    "BUCKET"  = "${var.bucket}_${var.project}"
    "REGION"  = var.region
    "DAG_PATH"= local.gcs_path_to_dag
    "DATASET" = var.dataset
  }

  depends_on = [google_storage_bucket.bucket, google_storage_bucket_object.zip]
}

resource "google_storage_bucket_object" "dag" {
 name         = local.gcs_path_to_dag
 source       = local.local_path_to_dag
 content_type = "text/plain"
 bucket       = google_storage_bucket.bucket.id

 depends_on = [google_storage_bucket.bucket]
}

resource "google_storage_bucket_object" "zip" {
 name         = local.gcs_path_to_zip
 source       = local.local_path_to_zip
 content_type = "text/plain"
 bucket       = google_storage_bucket.bucket.id

 depends_on = [google_storage_bucket.bucket]
}