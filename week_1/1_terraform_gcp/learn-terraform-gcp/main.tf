terraform {
  required_providers {
    google = {
      source = "hashicorp/google"

    }
  }
}

provider "google" {
  //credentials = file("<NAME>.json")

  project = "datazoomcamp-33300"
  region  = "europe-west9"
  zone    = "europe-west9-a"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}
