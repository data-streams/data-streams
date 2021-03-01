import json
import sys

import yaml


def group(nodes):
    hosts = {private: {'ansible_host': public} for private, public in nodes}
    return {'hosts': hosts}


data = json.load(sys.stdin)

private_list = data['private_ips']['value']
public_list = data['public_ips']['value']

# private_list = [ip.strip() for ip in private_ips.split(',')]
# public_list = [ip.strip() for ip in public_ips.split(',')]

assert len(private_list) >= 2
assert len(public_list) >= 2

manager, *workers = zip(private_list, public_list)

result = {'all': {
    'children': {
        'manager': group([manager]),
        'workers': group(workers),
        'bench': group([manager]),
        'acker': group([workers[0]]),
        'input': group([workers[1] if len(workers) > 1 else workers[0]])
    },
    'vars': {
        'ansible_user': 'ubuntu',
        'ansible_become': True
    }
}}

yaml.dump(result, sys.stdout, default_flow_style=False)
