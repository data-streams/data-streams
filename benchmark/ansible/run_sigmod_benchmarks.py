#!/usr/bin/env python3

import itertools
import os


def run_benchmarks(parallelism=3, rate=5, watermarks=False, iterations=10, distributed_acker=False, stream_length=2000):
    print(f"rate={rate}, watermarks={watermarks}, parallelism={parallelism}, iterations={iterations}, distributed_acker={distributed_acker}")
    return os.system(
        f"ansible-playbook -e stream_length={stream_length} -e results_name=sigmod.06.09.nightly -e parallelism={parallelism} -e rate={rate} -e watermarks={watermarks} -e iterations={iterations} -e distributed_acker={distributed_acker} -i hse.yml flamestream.yml"
    )

exit(0)

for parallelism in [3]:
    for iterations in range(5, 0, -5):
        for rate in range(10, 0, -2):
            for distributed_acker in [True]:
                run_benchmarks(rate=rate, distributed_acker=distributed_acker, parallelism=parallelism, iterations=iterations)


for parallelism in [3, 2]:
    for iterations in range(100, 0, -5):
        for rate in range(40, 2, -2):
            for watermarks in [False, True]:
                run_benchmarks(rate=rate, watermarks=watermarks, parallelism=parallelism, iterations=iterations)

for watermarks in [False, True]:
    for rate in range(50, 0, -2):
        for parallelism in [2]:
            run_benchmarks(rate=rate, watermarks=watermarks, parallelism=parallelism, iterations=100)

for iterations in [100, 50, 25, 10]:
    for watermarks in [False, True]:
        for rate in [5, 7, 10, 15, 20, 25, 50, 100]:
            for parallelism in [2]:
                run_benchmarks(rate=rate, watermarks=watermarks, parallelism=parallelism, iterations=iterations)

for iterations in [25, 50, 100]:
    for watermarks in [False, True]:
        for rate in [1, 2, 3, 4, 5, 7, 10, 15, 20]:
            for parallelism in [1, 2, 3]:
                run_benchmarks(rate=rate, watermarks=watermarks, parallelism=parallelism, iterations=iterations)
