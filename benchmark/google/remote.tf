variable "google_project" {}

provider "google" {
  credentials = file("google_credentials.json")
  region  = "europe-north1"
  zone  = "europe-north1-c"
}

resource "google_compute_subnetwork" "main" {
  project = var.google_project
  name          = "flamestream"
  ip_cidr_range = "10.0.0.0/24"
  network       = "default"
}

resource "google_compute_instance" "manager" {
  project = var.google_project
  name = "flamestream-manager"
  machine_type = "n1-standard-1"

  boot_disk {
    initialize_params {
      image = "gce-uefi-images/ubuntu-1804-lts"
    }
  }

  metadata = {
    sshKeys = "ubuntu:${file("~/.ssh/id_rsa.pub")}"
  }

  network_interface {
    subnetwork       = google_compute_subnetwork.main.self_link
    access_config {
    }
  }
}

resource "google_compute_instance" "workers" {
  project = var.google_project
  count = 0
  name = "flamestream-worker-${count.index}"
  machine_type = "n1-standard-1"

  boot_disk {
    initialize_params {
      image = "gce-uefi-images/ubuntu-1804-lts"
    }
  }

  metadata = {
    sshKeys = "ubuntu:${file("~/.ssh/id_rsa.pub")}"
  }

  network_interface {
    subnetwork       = google_compute_subnetwork.main.self_link
  }
}

output "manager_public_ip" {
  value = google_compute_instance.manager.network_interface.0.access_config.0.nat_ip
}
output "manager_private_ip" {
  value = google_compute_instance.manager.network_interface.0.network_ip
}
output "worker_private_ips" {
  value = google_compute_instance.workers.*.network_interface.0.network_ip
}
