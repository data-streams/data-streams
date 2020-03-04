variable "access_key" {}
variable "secret_key" {}
variable "key_pair" {}
variable "cluster_size" {
  default = 3
}

variable "prefix" {
  default = "flamestream-benchmarks"
}

provider "azurerm" {
  version = "=1.33.0"
}

data "azurerm_subscription" "current" {}

resource "azurerm_resource_group" "main" {
  name     = "${var.prefix}-resources"
  location = "West Europe"
}

resource "azurerm_virtual_network" "main" {
  name                = "${var.prefix}-network"
  address_space       = ["10.0.0.0/16"]
  location            = "${azurerm_resource_group.main.location}"
  resource_group_name = "${azurerm_resource_group.main.name}"
}

resource "azurerm_subnet" "internal" {
  name                = "${var.prefix}-internal"
  resource_group_name  = "${azurerm_resource_group.main.name}"
  virtual_network_name = "${azurerm_virtual_network.main.name}"
  address_prefix       = "10.0.2.0/24"
}

resource "azurerm_public_ip" "manager" {
  name                    = "test-pip"
  location                = "${azurerm_resource_group.main.location}"
  resource_group_name     = "${azurerm_resource_group.main.name}"
  allocation_method       = "Dynamic"
  idle_timeout_in_minutes = 30
}

resource "azurerm_network_interface" "manager" {
  name                = "${var.prefix}-manager"
  location            = "${azurerm_resource_group.main.location}"
  resource_group_name = "${azurerm_resource_group.main.name}"

  ip_configuration {
    name                          = "testconfiguration1"
    subnet_id                     = "${azurerm_subnet.internal.id}"
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = "${azurerm_public_ip.manager.id}"
  }
}

resource "azurerm_managed_disk" "manager" {
  name                 = "${var.prefix}-manager"
  location             = "${azurerm_resource_group.main.location}"
  resource_group_name  = "${azurerm_resource_group.main.name}"
  storage_account_type = "Standard_LRS"
  create_option        = "Copy"
  source_resource_id   = "${data.azurerm_subscription.current.id}/resourceGroups/FlamestreamBenchmarks/providers/Microsoft.Compute/snapshots/FlamestreamBenchmarks"
  disk_size_gb         = "30"
}

resource "azurerm_virtual_machine" "manager" {
  name                  = "${var.prefix}-manager"
  location              = "${azurerm_resource_group.main.location}"
  resource_group_name   = "${azurerm_resource_group.main.name}"
  network_interface_ids = ["${azurerm_network_interface.manager.id}"]
  vm_size               = "Standard_B1ms"

  delete_os_disk_on_termination = true
  delete_data_disks_on_termination = true

  storage_os_disk {
    name              = "${azurerm_managed_disk.manager.name}"
    caching           = "ReadWrite"
    create_option     = "Attach"
    managed_disk_id   = "${azurerm_managed_disk.manager.id}"
    os_type           = "Linux"
  }
  os_profile_linux_config {
    disable_password_authentication = true
    ssh_keys {
      key_data = file("~/.ssh/id_rsa.pub")
      path = "/home/ubuntu/.ssh/authorized_keys"
    }
  }
}

data "azurerm_public_ip" "manager" {
  name                = "${azurerm_public_ip.manager.name}"
  resource_group_name = "${azurerm_virtual_machine.manager.resource_group_name}"
}

resource "azurerm_network_interface" "workers" {
  count               = var.cluster_size
  name                = "${var.prefix}-worker-${count.index}"
  location            = "${azurerm_resource_group.main.location}"
  resource_group_name = "${azurerm_resource_group.main.name}"

  ip_configuration {
    name                          = "testconfiguration1"
    subnet_id                     = "${azurerm_subnet.internal.id}"
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_managed_disk" "workers" {
  count                 = var.cluster_size
  name                  = "${var.prefix}-worker-${count.index}"
  location             = "${azurerm_resource_group.main.location}"
  resource_group_name  = "${azurerm_resource_group.main.name}"
  storage_account_type = "Standard_LRS"
  create_option        = "Copy"
  source_resource_id   = "${data.azurerm_subscription.current.id}/resourceGroups/FlamestreamBenchmarks/providers/Microsoft.Compute/snapshots/FlamestreamBenchmarks"
  disk_size_gb         = "30"
}

resource "azurerm_virtual_machine" "workers" {
  count                 = var.cluster_size
  name                  = "${var.prefix}-worker-${count.index}"
  location              = "${azurerm_resource_group.main.location}"
  resource_group_name   = "${azurerm_resource_group.main.name}"
  network_interface_ids = ["${azurerm_network_interface.workers[count.index].id}"]
  vm_size               = "Standard_B1ms"

  delete_os_disk_on_termination = true
  delete_data_disks_on_termination = true

  storage_os_disk {
    name              = azurerm_managed_disk.workers[count.index].name
    caching           = "ReadWrite"
    create_option     = "Attach"
    managed_disk_id   = azurerm_managed_disk.workers[count.index].id
    os_type           = "Linux"
  }
  os_profile_linux_config {
    disable_password_authentication = true
    ssh_keys {
      key_data = file("~/.ssh/id_rsa.pub")
      path = "/home/ubuntu/.ssh/authorized_keys"
    }
  }
}

output "manager_public_ip" {
  value = "${data.azurerm_public_ip.manager.ip_address}"
}

output "manager_private_ip" {
  value = "${azurerm_network_interface.manager.private_ip_address}"
}

output "worker_private_ips" {
  value = "${azurerm_network_interface.workers.*.private_ip_address}"
}
