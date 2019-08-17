#!/usr/bin/env python3
import inspect
import itertools
import os


class Machines:
    def __init__(self, manager_public_ip, manager_private_ip, worker_private_ips):
        self.manager_public_ip = manager_public_ip
        self.manager_private_ip = manager_private_ip
        self.worker_private_ips = worker_private_ips

    def destroyed_hosts(self, hosts):
        return [
            host
            for host in hosts
            if host.split('\s') not in self.hosts_to_add(hosts)
        ]

    def applied_hosts(self, hosts):
        return self.destroyed_hosts(hosts) + self.hosts_to_add(hosts)

    def hosts_to_add(self, hosts):
        existing_hosts = sum(map(lambda host: host.split('\s'), hosts), [])
        ip_by_host = dict(list(self.worker_ip_by_host().items()) + [(self.manager_host(), manager_public_ip)])
        return [f"{ip}\t{host}\n" for host, ip in ip_by_host.items() if host not in existing_hosts]

    def manager_host(self):
        return "flamestream-benchmarks-manager"

    def worker_ip_by_host(self):
        return {
            f"flamestream-benchmarks-worker-{index}": ip
            for index, ip in enumerate(self.worker_private_ips)
        }

    def ssh_config(self):
        return "".join([
            f"Host {self.manager_host()}\n\tUser ubuntu\n",
            *[
                f"Host {host}\n\tUser ubuntu\n\tProxyJump {self.manager_host()}\n"
                for host, ip in self.worker_ip_by_host().items()
            ]
        ])


if __name__ == "__main__":
    import json
    import sys
    import subprocess

    hosts_file = "/etc/hosts"


    def read_hosts():
        with open(hosts_file) as input:
            return input.readlines()


    def read_ssh_config():
        with open(os.path.expanduser("~/.ssh/config")) as input:
            return input.readlines()


    hosts = read_hosts()
    ssh_config = read_ssh_config()
    terraform_run = subprocess.run(["terraform", "output", "--json"], capture_output=True)
    terraform_run.check_returncode()
    terraform_output = json.loads(terraform_run.stdout)
    manager_public_ip, = [ip.strip() for ip in terraform_output['public_ips']['value']]
    manager_private_ip, *worker_private_ips = [ip.strip() for ip in terraform_output['private_ips']['value']]
    machines = Machines(manager_public_ip, manager_private_ip, worker_private_ips)

    if len(sys.argv) != 2:
        raise Exception("exactly 1 argument is expected")

    if sys.argv[1] == "apply":
        updated_hosts = "".join(machines.applied_hosts(hosts))
        included_ssh_config = machines.ssh_config()
        with open(os.path.expanduser("~/.ssh/flamestream-benchmarks_config"), "w") as output:
            output.write(included_ssh_config)
    elif sys.argv[1] == "destroy":
        updated_hosts = "".join(machines.destroyed_hosts(hosts))
        with open(os.path.expanduser("~/.ssh/flamestream-benchmarks_config"), "w") as output:
            pass
    else:
        raise Exception("unknown argument")
    os.system(f"echo ")
    print(updated_hosts)

    ssh_config_include = "Include ~/.ssh/flamestream-benchmarks_config\n"
    if ssh_config_include not in ssh_config:
        with open(os.path.expanduser("~/.ssh/config"), 'a') as output:
            output.write("Host *\n")
            output.write(ssh_config_include)


    def acknowledge_host(host):
        os.system(f"ssh -o StrictHostKeyChecking=no {host} exit")


    acknowledge_host(machines.manager_host())
    machines.worker_ip_by_host().keys()
    subprocess.run(
        ["ssh", "-T", machines.manager_host(), "sudo tee -a /etc/hosts"],
        input="".join(f"{ip}\t{host}\n" for host, ip in machines.worker_ip_by_host().items()).encode()
    )
    for host in machines.worker_ip_by_host().keys():
        acknowledge_host(host)
