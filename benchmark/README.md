# Benchmarks

#### Contents of `ansible` directory:

- `aws.tf` - terraform script for cluster setup
- `aws.yml` - inventory for aws hosts
- `docker-compose.yml` - the setup for local testing
- `local.yml` - inventory for local docker hosts
- `flamestream.yml` - playbook for flamestream benchmarking (does all the job)
- `flink.yml` - same for flink
- `roles/` - roles, they seems to be self-explanatory

#### Cluster lifecycle management

1. Fill `terraform.tfvars` with valid AWS credentials and your public key
2. Run `terraform init` to initialize terraform working directory
3. Modify `aws.tf` to match your benchstand requirements:
    - Set `cluster_size` to required cluster size (default = 11)
    - Add ingress rules to open some ports
    - Set `instance_type` to required type (default = `t2.small`)
4. Instantiate AWS cluster via `terraform apply`
5. Run `bash init_inventory.sh` to create Ansible inventory from terraform output
6. Run `ansible-playbook -v -i aws.yml hosts.yml` to setup local hostname on each node __THIS IS IMPORTANT FOR AKKA_
7. Do some benchmarking...
8. Destroy cluster via `terraform destroy`
9. You can start next benchmarking session from 4th step

#### How to run benchmarks

0. Copy wiki.xml to `flamestream-bench` job
1. Package the project `mvn package`
2. Copy artifacts to Ansible roles
3. Run playbook with inventory of your choice
4. Results would appear in `results` directory

To run locally:

0. Copy wiki.xml
1. Package
2. Copy
3. `docker-compose up`
4. In `flamestream-common` job change _synchronize_ with _copy_, as sync doesn't work with docker connection
5. Run playbook

All this steps are automated with script in the root of the project. On the last line there is a default function call, replace it with local/remote, flink/flamestream function.

Caveats:

- If something goes wrong you need to manually kill bench. I ssh into a host and `sudo pkill -f spbsu` (To run on every host: `ansible -v -i aws.yml all -a 'sudo pkill -f spb'`
- You need to manually check resources on each host. So I suggest to ssh into _bench_ and _input_ hosts and random worker. Run `vmstat -Sm 1`. To monitor memory and cpu usage.
- When you are copying hosts to inventory from aws console check twice
