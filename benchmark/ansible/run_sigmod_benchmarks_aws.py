#!/usr/bin/env python3.9
import itertools
import json
import os


def run_benchmarks(rate=2., iterations=100, results_name="", bench_environment={}, worker_environment={}, **kwargs):
    extra_vars = {
        **dict(
            parallelism=5, iterations=iterations, stream_length=800975, local_acker_flush_delay_in_millis=5,
            rate=rate,
            bench_environment={**dict(
                EDGES_TAIL_FILE='/home/ubuntu/tail_head_offset.bin',
                EDGES_HEAD_FILE='/home/ubuntu/edge_head.bin',
                WARM_UP_STREAM_LENGTH="10080",
                WARM_UP_DELAY_MS="10",
                FRONTS_NUMBER="1",
            ), **bench_environment},
            worker_environment={**dict(), **worker_environment},
        ),
        **kwargs
    }
    print(extra_vars)
    results_name = f"sigmod.07.29/iterations={iterations}/rate={rate}/{results_name}"
    os.system(
        f"ansible-playbook --extra-vars '{json.dumps(dict(**extra_vars, results_name=results_name))}' -i remote.yml flamestream_pre.yml"
    )
    flamestream_dir = "/home/ubuntu/flamestream"
    os.system(
      "ssh -t flamestream-benchmarks-manager 'PATH=/usr/lib/jvm/java-8-oracle/jre/bin/:$PATH' " \
      'EDGES_TAIL_FILE=/home/ubuntu/tail_head_offset.bin EDGES_HEAD_FILE=/home/ubuntu/edge_head.bin SIMULTANEOUS_REQUESTS=10 ' \
      "java -Daeron.term.buffer.length=4194304 -Daeron.mtu.length=16384 " \
      "-Xms500m -Xmx1500m -Xlog:gc,gc+cpu=info::utc " \
      "-XX:+HeapDumpOnOutOfMemoryError -cp \\'flamestream/lib/*\\' " \
      "com.spbsu.flamestream.example.benchmark.LentaBenchStand "\
      f"flamestream/bench.conf flamestream/deployer.conf '| tee {flamestream_dir}/bench.log'"
    )
    os.system(
        f"ansible-playbook --extra-vars '{json.dumps(dict(**extra_vars, results_name=results_name))}' -i remote.yml flamestream_post.yml"
    )


for rate in [3]:
    # run_benchmarks(results_name="tracking_frequency=0", tracking_frequency=0, tracking="disabled", rate=rate,
    #                bench_environment=dict(warm_up_stream_length=20000))
    for tracking_frequency in [1]:
        for distributed in [False]:
            run_benchmarks(
                results_name=f"tracking_frequency={tracking_frequency}/tracking=acking/distributed={distributed}",
                tracking_frequency=tracking_frequency,
                tracking="acking",
                distributed_acker=distributed,
                rate=rate,
                worker_environment=dict(LOCAL_ACKER_FLUSH_COUNT=1000000000),
            )
        # run_benchmarks(
        #     results_name=f"tracking_frequency={tracking_frequency}/tracking=watermarking",
        #     tracking_frequency=tracking_frequency,
        #     tracking="watermarking",
        #     rate=rate
        # )

exit(0)

for local_acker_flush_delay_in_millis in [1, 25]:
    for parallelism in [2, 4, 8]:
        for iterations in [10, 50, 100]:
            for watermarks in [True, False]:
                run_benchmarks(
                    warm_up_stream_length=5000, rate=20, watermarks=watermarks, parallelism=parallelism,
                    iterations=iterations, stream_length=10000,
                    local_acker_flush_delay_in_millis=local_acker_flush_delay_in_millis
                )

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
