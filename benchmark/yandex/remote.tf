variable "yandex_cloud_id" {}
variable "yandex_folder_id" {}
//variable "yandex_token" {}
variable "snapshot_id" {}
variable "subnet_id" {}

provider "yandex" {
  cloud_id  = var.yandex_cloud_id
  folder_id = var.yandex_folder_id
  // token = var.yandex_token
  service_account_key_file = "/Users/nikitasokolov/key.json"
  zone      = "ru-central1-a"
}

resource "yandex_compute_instance" "manager" {
  boot_disk {
    initialize_params {
//      image_id = "fd8vmcue7aajpmeo39kk"
      snapshot_id = var.snapshot_id
      size = 15
      type = "network-nvme"
    }
  }

  resources {
    cores  = 2
    memory = 8
  }
  metadata = {
    ssh-keys = "ubuntu:${file("~/.ssh/id_rsa.pub")}"
  }
  network_interface {
    subnet_id = var.subnet_id
    nat = true
  }
  scheduling_policy {
    preemptible = true
  }
}

resource "yandex_compute_instance" "worker" {
  count = 5
  boot_disk {
    initialize_params {
//      image_id = "fd8vmcue7aajpmeo39kk"
      snapshot_id = var.snapshot_id
      size = 15
      type = "network-nvme"
    }
  }

  resources {
    cores  = 2
    memory = 8
  }
  metadata = {
    ssh-keys = "ubuntu:${file("~/.ssh/id_rsa.pub")}"
  }
  network_interface {
    subnet_id = var.subnet_id
  }
  scheduling_policy {
    preemptible = true
  }
  allow_stopping_for_update = true
}

output "manager_public_ip" {
  value = yandex_compute_instance.manager.network_interface[0].nat_ip_address
}
output "manager_private_ip" {
  value = yandex_compute_instance.manager.network_interface[0].ip_address
}
output "worker_private_ips" {
  value = yandex_compute_instance.worker.*.network_interface.0.ip_address
}
