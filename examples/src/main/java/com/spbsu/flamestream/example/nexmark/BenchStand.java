package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.example.benchmark.BenchStandComponentFactory;
import com.spbsu.flamestream.example.benchmark.FlameGraphDeployer;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchStand {

  private final String workerIdPrefix;

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
    final var benchStand = new BenchStand(benchConfig, 5);
    final var baseTime = System.currentTimeMillis();
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(
                            deployerConfig,
                            new SystemConfig.Builder().workersResourcesDistributor(
                                    new SystemConfig.WorkersResourcesDistributor.Enumerated(
                                            benchStand.workerIdPrefix,
                                            0
                                    )
                            ).build()
                    ),
                    Materializer.materialize(Query8.create(benchStand.nexmarkConfiguration.windowSizeSec)),
                    new GeneratorFrontType(
                            benchStand.nexmarkConfiguration,
                            IntStream.range(0, benchStand.parallelism).boxed().collect(Collectors.toMap(
                                    integer -> benchStand.workerIdPrefix + (integer + 1),
                                    Function.identity()
                            )),
                            baseTime,
                            benchStand.maxEvents
                    ),
                    new TimingsRearType<>(
                            new SocketRearType(
                                    benchStand.benchHost,
                                    benchStand.rearPort,
                                    TimingsRearType.Timings.class
                            ),
                            10,
                            Instant.ofEpochMilli(baseTime)
                    )
            )
    ) {
      benchStand.run(graphDeployer, benchConfig.getString("worker-id"));
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BenchStand.class);

  private final double sleepBetweenDocs;
  private final String wikiDumpPath;
  private final String validatorClass;
  public final String benchHost;
  private final String inputHost;
  public final int frontPort;
  public final int rearPort;
  private final int parallelism;
  public final long maxEvents = 1000000;
  public final NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();

  {
    nexmarkConfiguration.bidProportion = 1;
    nexmarkConfiguration.personProportion = 1;
    nexmarkConfiguration.auctionProportion = 2;
  }

  public BenchStand(Config benchConfig, int parallelism) {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    wikiDumpPath = benchConfig.getString("wiki-dump-path");
    validatorClass = benchConfig.getString("validator");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    this.parallelism = parallelism;
    nexmarkConfiguration.nextEventRate /= parallelism;
    nexmarkConfiguration.firstEventRate /= parallelism;
    workerIdPrefix = benchConfig.getString("worker-id-prefix");
  }

  private class Window {
    int left = parallelism;
    long processed = Long.MIN_VALUE;
    long notified = Long.MIN_VALUE;

    synchronized void handle(TimingsRearType.Timings timings) {
      processed = Long.max(processed, timings.processed);
      notified = Long.max(notified, timings.notified);
      if (--left == 0) {
        System.out.println(format(timings.window) + " " + format(processed) + " " + format(notified));
      }
    }

    private String format(long number) {
      return NumberFormat.getNumberInstance(Locale.US).format(number);
    }
  }

  public void run(GraphDeployer graphDeployer, String inputHostId) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final var expectedSize =
            parallelism * (int) (10 + (maxEvents - 1) / nexmarkConfiguration.firstEventRate / parallelism / nexmarkConfiguration.windowSizeSec + 1);
    System.out.println(expectedSize);
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(
            expectedSize
    );
    final Map<Long, Window> windows = new ConcurrentHashMap<>();
    try (
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    x -> {
                      if (x instanceof DataItem) {
                        final var timings = ((DataItem) x).payload(TimingsRearType.Timings.class);
                        awaitConsumer.accept(timings);
                        windows.computeIfAbsent(timings.window, __ -> new Window()).handle(timings);
                      }
                    },
                    rearPort, TimingsRearType.Timings.class
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(5, TimeUnit.MINUTES);
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
    }
  }
}
