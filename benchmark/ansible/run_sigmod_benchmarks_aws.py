#!/usr/bin/env python3

import itertools
import os


def run_benchmarks(parallelism=3, rate=5., watermarks=False, iterations=10, distributed_acker=False, stream_length=2000):
    print(f"rate={rate}, watermarks={watermarks}, parallelism={parallelism}, iterations={iterations}, distributed_acker={distributed_acker}")
    return os.system(
        f"ansible-playbook -e stream_length={stream_length} -e results_name=sigmod.06.28 -e parallelism={parallelism} -e rate={rate} -e watermarks={watermarks} -e iterations={iterations} -e distributed_acker={distributed_acker} -i aws.yml flamestream.yml"
    )


for rate in [0.5]:
    run_benchmarks(rate=rate, watermarks=False, parallelism=7, iterations=50, stream_length=80000)

exit(0)

# Throughput
for rate in [5.5, 6]:
    run_benchmarks(rate=rate, watermarks=True, parallelism=2, iterations=50, stream_length=10000)
for rate in [10, 11]:
    run_benchmarks(rate=rate, watermarks=True, parallelism=5, iterations=50, stream_length=10000)
for rate in [21, 22, 25]:
    run_benchmarks(rate=rate, watermarks=True, parallelism=10, iterations=50, stream_length=10000)

for rate in [1.5, 2, 3]:
    run_benchmarks(rate=rate, watermarks=False, parallelism=2, iterations=50, stream_length=10000)
for rate in [1, 1.5]:
    run_benchmarks(rate=rate, watermarks=False, parallelism=5, iterations=50, stream_length=10000)
for rate in [1.3, 1.4, 1.5]:
    run_benchmarks(rate=rate, watermarks=False, parallelism=6, iterations=50, stream_length=10000)
for rate in [0.5, 1, 1.5, 2.5]:
    run_benchmarks(rate=rate, watermarks=False, parallelism=10, iterations=50, stream_length=10000)

exit(0)


for parallelism in [10]:
    run_benchmarks(rate=20, distributed_acker=True, parallelism=parallelism, iterations=35)
    for iterations in range(30, 0, -5):
        for rate in reversed(range(20, 0, -2)):
            run_benchmarks(rate=rate, distributed_acker=True, parallelism=parallelism, iterations=iterations)
    for iterations in range(100, 0, -5):
        for rate in reversed(range(30, 0, -2)):
            run_benchmarks(rate=rate, distributed_acker=False, parallelism=parallelism, iterations=iterations)
            run_benchmarks(rate=rate, watermarks=True, parallelism=parallelism, iterations=iterations)

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
