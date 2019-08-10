variable "ssh_username" {}
variable "yandex_cloud_id" {}
variable "yandex_folder_id" {}
variable "yandex_token" {}

provider "yandex" {
  cloud_id  = "${var.yandex_cloud_id}"
  folder_id = "${var.yandex_folder_id}"
  token     = "${var.yandex_token}"
  zone      = "ru-central1-a"
}

resource "yandex_vpc_network" "workers" {
}

resource "yandex_vpc_subnet" "workers" {
  network_id = "${yandex_vpc_network.workers.id}"
  v4_cidr_blocks = ["10.0.0.0/24"]
}

resource "yandex_compute_instance" "bench_stand" {
  boot_disk {
    initialize_params {
      image_id = "fd87va5cc00gaq2f5qfb"
    }
  }

  resources {
    cores  = 1
    memory = 2
  }
  metadata = {
    ssh-keys = "${var.ssh_username}:${file("~/.ssh/id_rsa.pub")}"
  }
  network_interface {
    subnet_id = "${yandex_vpc_subnet.workers.id}"
    nat = true
  }
}

resource "yandex_compute_instance" "worker" {
  count = 7
  boot_disk {
    initialize_params {
      image_id = "fd87va5cc00gaq2f5qfb"
    }
  }

  resources {
    cores  = 1
    memory = 2
  }
  metadata = {
    ssh-keys = "${var.ssh_username}:${file("~/.ssh/id_rsa.pub")}"
  }
  network_interface {
    subnet_id = "${yandex_vpc_subnet.workers.id}"
  }
}

output "public_ips" {
  value = "${join(", ", yandex_compute_instance.bench_stand.*.network_interface.0.nat_ip_address)}"
}
output "private_ips" {
  value = "${join(", ", concat([yandex_compute_instance.bench_stand], yandex_compute_instance.worker).*.network_interface.0.ip_address)}"
}
