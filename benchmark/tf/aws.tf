variable "access_key" {}
variable "secret_key" {}
variable "key_pair" {}
variable "cluster_size" {
  default = 11
}
variable "region" {
  default = "us-east-2"
}

provider "aws" {
  access_key = "${var.access_key}"
  secret_key = "${var.secret_key}"
  region = "${var.region}"
}

resource "aws_key_pair" "mega-benchmarker" {
  key_name = "mega-benchmarker"
  public_key = "${var.key_pair}"
}

resource "aws_vpc" "workers" {
  cidr_block = "10.0.0.0/24"
}

resource "aws_internet_gateway" "workers" {
  vpc_id = "${aws_vpc.workers.id}"
}

resource "aws_subnet" "workers" {
  vpc_id = "${aws_vpc.workers.id}"
  cidr_block = "10.0.0.0/24"
  map_public_ip_on_launch = true
}

resource "aws_route_table" "workers" {
  vpc_id = "${aws_vpc.workers.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.workers.id}"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id = "${aws_subnet.workers.id}"
  route_table_id = "${aws_route_table.workers.id}"
}

resource "aws_security_group" "workers" {
  vpc_id = "${aws_vpc.workers.id}"

  ingress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "${aws_vpc.workers.cidr_block}"]
  }

  ingress {
    from_port = 8081
    to_port = 8081
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  egress {
    protocol = -1
    from_port = 0
    to_port = 0
    cidr_blocks = [
      "0.0.0.0/0"]
  }
}

resource "aws_instance" "worker" {
  count = "${var.cluster_size}"
  ami = "ami-965e6bf3"
  instance_type = "t2.small"
  key_name = "${aws_key_pair.mega-benchmarker.key_name}"
  subnet_id = "${aws_subnet.workers.id}"
  vpc_security_group_ids = [
    "${aws_security_group.workers.id}"]
  associate_public_ip_address = true
}

output "public_ips" {
  value = "${join(", ", aws_instance.worker.*.public_ip)}"
}
output "private_ips" {
  value = "${join(", ", aws_instance.worker.*.private_ip)}"
}
