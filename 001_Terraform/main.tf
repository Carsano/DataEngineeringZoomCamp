terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = "dtc-de-course-484507"
  region  = "europe-west9"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dtc-de-course-484507-terra-bucket"
  location      = "FR"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}