package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.example.bl.AckerGraph;
import com.spbsu.flamestream.example.bl.no_barrier.WatermarksGraph;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class AckerBenchStand {
  public static void main(String[] args) throws Exception {
    final Config benchConfig;
    final Config deployerConfig;
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("deployer.conf").getConfig("deployer");
    }
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final AckerBenchStand lentaBenchStand = new AckerBenchStand(benchConfig);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(deployerConfig),
                    AckerGraph.apply(lentaBenchStand.iterations),
                    new SocketFrontType(lentaBenchStand.benchHost, lentaBenchStand.frontPort, Integer.class),
                    new SocketRearType(lentaBenchStand.benchHost, lentaBenchStand.rearPort)
            )
    ) {
      lentaBenchStand.run(graphDeployer);
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AckerBenchStand.class);

  private final int sleepBetweenDocs;
  private final int streamLength;
  private final String benchHost;
  private final String inputHost;
  private final int frontPort;
  private final int rearPort;
  private final int parallelism;
  private final int iterations;

  public AckerBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getInt("sleep-between-docs-ms");
    streamLength = benchConfig.getInt("stream-length");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
    iterations = benchConfig.getInt("iterations");
  }

  public void run(GraphDeployer graphDeployer) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(streamLength);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    try (
            AutoCloseable ignored = benchStandComponentFactory.producer(
                    Integer.class,
                    IntStream.range(0, streamLength).peek(id -> {
                      latencies.put(id, new LatencyMeasurer());
                      LockSupport.parkNanos((long) (nextExp(1.0 / sleepBetweenDocs) * 1.0e6));
                    }).boxed(),
                    inputHost,
                    frontPort
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    Integer.class,
                    id -> {
                      latencies.get(id).finish();
                      awaitConsumer.accept(id);
                      if (awaitConsumer.got() % 100 == 0) {
                        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(60, TimeUnit.MINUTES);
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
    }
    final String latenciesString = latencies.values()
            .stream()
            .map(latencyMeasurer -> Long.toString(latencyMeasurer.statistics().getMax()))
            .collect(Collectors.joining(", "));
    try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/lat.data")))) {
      pw.println(latenciesString);
    }
    LOG.info("Result: {}", latenciesString);
    final long[] skipped = latencies.values()
            .stream()
            .skip(200)
            .mapToLong(l -> l.statistics().getMax())
            .toArray();
    LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
    LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
  }

  private final Random payloadDelayRandom = new Random(7);

  private double nextExp(double lambda) {
    return StrictMath.log(1 - payloadDelayRandom.nextDouble()) / -lambda;
  }
}
