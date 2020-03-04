#!/usr/bin/env python3.7
from inspect import cleandoc

if __name__ == "__main__":
    import json
    import sys
    import subprocess

    if len(sys.argv) != 2:
        raise Exception("exactly 1 argument is expected")

    terraform_run = subprocess.run(["terraform", "output", "--json"], capture_output=True)
    terraform_run.check_returncode()
    terraform_output = json.loads(terraform_run.stdout)
    manager_public_ip = terraform_output['manager_public_ip']['value'].strip()
    manager_private_ip = terraform_output['manager_private_ip']['value'].strip()
    worker_private_ips = [ip.strip() for ip in terraform_output['worker_private_ips']['value']]
    bolt_properties = (
        f"manager_public_ip={manager_public_ip}",
        f"manager_private_ip={manager_private_ip}",
        f"worker_private_ips={json.dumps(worker_private_ips)}",
        "--concurrency=10",
    )

    if sys.argv[1] == "apply":
        input(cleandoc(f"""
          Prepend following to ~/.ssh/config: Include flamestream-benchmarks.config
          Put this to /etc/hosts: {manager_public_ip}\tflamestream-benchmarks-manager
          [CONTINUE]
        """))
        subprocess.run(("bolt", "plan", "run", "my::localhost") + bolt_properties).check_returncode()
        subprocess.run(("bolt", "plan", "run", "my::hosts", "--run-as=root") + bolt_properties).check_returncode()
        subprocess.run(("bolt", "plan", "run", "my::install", "--run-as=root") + bolt_properties).check_returncode()
        subprocess.run(("bolt", "plan", "run", "my::ssh") + bolt_properties).check_returncode()
        subprocess.run(("bolt", "plan", "run", "my::bench") + bolt_properties).check_returncode()
    elif sys.argv[1] == "destroy":
        subprocess.run(("bolt", "plan", "run", "my::destroy") + bolt_properties).check_returncode()
    else:
        raise Exception("unknown argument")
