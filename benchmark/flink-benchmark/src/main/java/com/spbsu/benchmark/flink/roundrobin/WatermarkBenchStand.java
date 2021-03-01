package com.spbsu.benchmark.flink.roundrobin;

import com.spbsu.flamestream.example.benchmark.BenchStandComponentFactory;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.benchmark.LatencyMeasurer;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class WatermarkBenchStand {
  public static final Class[] CLASSES_TO_REGISTER = {Integer.class, long[].class};
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkBenchStand.class);

  private final double sleepBetweenDocs;
  private final int itemsCount;
  public final String benchHost;
  public final int frontPort;
  public final int rearPort;

  public WatermarkBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    benchHost = benchConfig.getString("bench-host");
    itemsCount = benchConfig.getInt("items-count");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
  }

  public void run(GraphDeployer graphDeployer, int hosts) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    //noinspection unchecked
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(this.itemsCount);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    try (
            AutoCloseable ignored = benchStandComponentFactory.producer(
                    IntStream.range(0, this.itemsCount).peek(item -> {
                      LockSupport.parkNanos((long) (1.0 / sleepBetweenDocs * 1.0e6));
                      latencies.put(item, new LatencyMeasurer());
                    })::iterator,
                    frontPort,
                    Stream.generate(UUID::randomUUID).map(UUID::toString).limit(hosts),
                    Integer.class
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      if (!(object instanceof Integer)) {
                        return;
                      }
                      final Integer item = (Integer) object;
                      latencies.get(item).finish();
                      awaitConsumer.accept(item);
                      if (awaitConsumer.got() % 1000 == 0) {
                        LOG.warn("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort,
                    CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(5, TimeUnit.MINUTES);
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
    System.out.println("Median: " +  (long) percentiles().index(50).compute(skipped));
    System.out.println("75: " +  (long) percentiles().index(75).compute(skipped));
    System.out.println("95: " +  (long) percentiles().index(95).compute(skipped));
    System.out.println("99: " +  (long) percentiles().index(99).compute(skipped));
  }

  private final Random payloadDelayRandom = new Random(7);

  private double nextExp(double lambda) {
    return StrictMath.log(1 - payloadDelayRandom.nextDouble()) / -lambda;
  }
}
