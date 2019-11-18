#!/usr/bin/env python3

import datetime
import itertools
import json
import os
from functools import reduce

default_args = dict(
    tracking_frequency=1, parallelism=20, stream_length=50000, local_acker_flush_delay_in_millis=5,
    iterations=10,
)


def run_benchmarks(bench_environment={}, worker_environment={}, **args):
    args = dict(
        {**default_args, **args},
        bench_environment={**dict(WARM_UP_STREAM_LENGTH="50000", WARM_UP_DELAY_MS="5"), **bench_environment},
        worker_environment={
            **dict(BARRIER_DISABLED="TRUE", LOCAL_ACKER_FLUSH_COUNT=1000000000),
            **worker_environment,
        },
    )
    print(args)
    results_name = str(datetime.datetime.now())
    extra_vars = json.dumps(dict(**args, results_name=results_name))
    if os.system(f"ansible-playbook --extra-vars '{extra_vars}' -i remote.yml flamestream.yml") == 0:
        with open(f"results/{results_name}/vars.json", 'w') as vars_json:
            print(extra_vars, file=vars_json)


for rate in [0.09, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2]:
    run_benchmarks(tracking="disabled", distributed_acker=False, rate=rate)
