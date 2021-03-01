variable "yandex_cloud_id" {}
variable "yandex_folder_id" {}
//variable "yandex_token" {}
variable "service_account_key_file" {}
variable "subnet_id" {}

terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
      version = "0.51.1"
    }
  }
}

provider "yandex" {
  cloud_id = var.yandex_cloud_id
  folder_id = var.yandex_folder_id
  //  token = var.yandex_token
  zone = "ru-central1-a"
  service_account_key_file = var.service_account_key_file
}

resource "yandex_compute_instance" "worker" {
  count = 5
  name = "flamestream-worker-${count.index}"
  boot_disk {
    initialize_params {
      name = "flamestream-worker-${count.index}"
      image_id = "fd8vmcue7aajpmeo39kk"
      size = 15
      type = "network-ssd"
    }
  }

  resources {
    cores = 2
    memory = 8
    gpus = 0
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
  allow_stopping_for_update = true
}

output "public_ips" {
  value = yandex_compute_instance.worker.*.network_interface.0.nat_ip_address
}

output "private_ips" {
  value = yandex_compute_instance.worker.*.network_interface.0.ip_address
}
