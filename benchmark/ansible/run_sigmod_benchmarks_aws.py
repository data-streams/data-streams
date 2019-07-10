#!/usr/bin/env python3

import json
import os


def run_benchmarks(**kwargs):
    extra_vars = {**dict(
        results_name="sigmod.07.15", warm_up_stream_length=1000, parallelism=3, rate=5., watermarks=False,
               iterations=10, distributed_acker=False, stream_length=2000
    ), **kwargs}
    print(kwargs)
    return os.system(
        f"ansible-playbook --extra-vars '{json.dumps(extra_vars)}' -i aws.yml flamestream.yml"
    )


for rate in reversed([1.4, 2.]):
    for parallelism in [3]:
        run_benchmarks(
            warm_up_stream_length=5000, rate=rate, watermarks=True, parallelism=parallelism, iterations=50, stream_length=80000
        )

exit(0)

for parallelism in [2, 3, 4, 5]:
    for rate in reversed([0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1., 1.05, 1.1, 1.15, 1.2]):
        run_benchmarks(warm_up_stream_length=5000, rate=rate, watermarks=False, parallelism=parallelism, iterations=50,
                       stream_length=80000)

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

for rate in [0.6, 0.7, 0.8, 0.9, 1., 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.5, 3, 5, 10]:
    run_benchmarks(warm_up_stream_length=5000, rate=rate, watermarks=False, parallelism=7, iterations=50,
                   stream_length=80000)

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
