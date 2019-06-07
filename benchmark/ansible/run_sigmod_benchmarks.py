#!/usr/bin/env python3

import itertools
import os


def run_benchmarks(parallelism=3, rate=5, watermarks=False):
    print(f"rate={rate}, watermarks={watermarks}, parallelism={parallelism}")
    return os.system(
        f"ansible-playbook -e parallelism={parallelism} -e rate={rate} -e watermarks={watermarks} -i remote.yml flamestream.yml"
    )

for watermarks in [False, True]:
    for rate in [1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 50, 100]:
        for parallelism in [1, 2, 3]:
            run_benchmarks(rate=rate, watermarks=watermarks, parallelism=parallelism)
