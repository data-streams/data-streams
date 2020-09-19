variable "google_project" {}
variable "cluster_size" {}

provider "google" {
  credentials = file("google_credentials.json")
  project = var.google_project
  region  = "europe-north1"
  zone  = "europe-north1-c"
}

resource "google_compute_firewall" "main" {
  name    = "flamestream"
  network = "default"
  source_ranges = [google_compute_subnetwork.main.ip_cidr_range]

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }
}

resource "google_compute_subnetwork" "main" {
  name          = "flamestream"
  ip_cidr_range = "10.0.0.0/24"
  network       = "default"
}

resource "google_compute_disk" "manager" {
  name  = "flamestream-manager"
  type  = "pd-ssd"
  zone  = "europe-north1-c"
  snapshot = "ekf3eap91l7v"
}

resource "google_compute_instance" "manager" {
  name = "flamestream-manager"
  machine_type = "n1-standard-1"

  boot_disk {
    source = google_compute_disk.manager.self_link
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

resource "google_compute_disk" "workers" {
  count = var.cluster_size
  name = "flamestream-worker-${count.index}"
  type  = "pd-ssd"
  zone  = "europe-north1-c"
  snapshot = "ekf3eap91l7v"
}

resource "google_compute_instance" "workers" {
  count = var.cluster_size
  name = "flamestream-worker-${count.index}"
  machine_type = "n1-standard-1"

  boot_disk {
    source = google_compute_disk.workers[count.index].self_link
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
