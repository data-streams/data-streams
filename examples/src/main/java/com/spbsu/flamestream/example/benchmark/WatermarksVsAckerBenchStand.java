package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.example.bl.WatermarksVsAckerGraph;
import com.spbsu.flamestream.runtime.WorkerApplication;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class WatermarksVsAckerBenchStand {
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
    final WatermarksVsAckerBenchStand benchStand = new WatermarksVsAckerBenchStand(benchConfig);
    WorkerApplication.WorkerConfig.Builder workerBuilder = new WorkerApplication.WorkerConfig.Builder()
            .millisBetweenCommits(1000000000)
            .barrierDisabled(true);
    benchStand.trackingFactory.buildWorker(workerBuilder);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(
                            deployerConfig,
                            workerBuilder::build
                    ),
                    WatermarksVsAckerGraph.apply(
                            benchStand.parallelism,
                            benchStand.iterations,
                            benchStand.childrenNumber
                    ),
                    new SocketFrontType(
                            benchStand.benchHost,
                            benchStand.frontPort,
                            WatermarksVsAckerGraph.Element.class,
                            WatermarksVsAckerGraph.Data.class,
                            WatermarksVsAckerGraph.Child.class,
                            WatermarksVsAckerGraph.Watermark.class
                    ),
                    new SocketRearType(
                            benchStand.benchHost,
                            benchStand.rearPort,
                            WatermarksVsAckerGraph.Element.class,
                            WatermarksVsAckerGraph.Data.class,
                            WatermarksVsAckerGraph.Child.class,
                            WatermarksVsAckerGraph.Watermark.class
                    )
            )
    ) {
      benchStand.run(graphDeployer);
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(WatermarksVsAckerBenchStand.class);

  private final double sleepBetweenDocs;
  private final int streamLength;
  private final String benchHost;
  private final String inputHost;
  private final int frontPort;
  private final int rearPort;
  private final int parallelism;
  private final int iterations;
  private final int childrenNumber;
  private final TrackingFactory trackingFactory;
  private final Tracking tracking;

  interface TrackingFactory {
    static TrackingFactory fromConfig(Config config) throws ClassNotFoundException {
      //noinspection unchecked
      return ConfigBeanFactory.create(
              config.withoutPath("class"),
              (Class<TrackingFactory>) ClassLoader.getSystemClassLoader().loadClass(config.getString("class"))
      );
    }

    Tracking create(int streamLength);

    default void buildWorker(WorkerApplication.WorkerConfig.Builder builder) {}

    class Disabled implements TrackingFactory {
      @Override
      public Tracking create(int streamLength) {
        return new Tracking.Disabled();
      }

      @Override
      public void buildWorker(WorkerApplication.WorkerConfig.Builder builder) {
        builder.acking(SystemConfig.Acking.DISABLED);
      }
    }

    class Acking implements TrackingFactory {
      @Override
      public Tracking create(int streamLength) {
        return new Tracking.Acking(new NotificationAwaitTimes(streamLength));
      }

      @Override
      public void buildWorker(WorkerApplication.WorkerConfig.Builder builder) {
        builder.acking(SystemConfig.Acking.CENTRALIZED).ackerWindow(10);
      }
    }

    class Watermarking implements TrackingFactory {
      private int frequency = 10;

      @Override
      public Tracking create(int streamLength) {
        if (streamLength % frequency != 0) {
          throw new IllegalArgumentException("watermarks frequency should be a stream length divisor");
        }
        return new Tracking.Watermarking(new NotificationAwaitTimes(streamLength), frequency);
      }

      @Override
      public void buildWorker(WorkerApplication.WorkerConfig.Builder builder) {
        builder.acking(SystemConfig.Acking.DISABLED);
      }

      void setFrequency(int frequency) {
        this.frequency = frequency;
      }
    }
  }

  static final class NotificationAwaitTimes implements Closeable {
    private final long[] all;
    private final AwaitCountConsumer awaitCountConsumer;

    NotificationAwaitTimes(int streamLength) {
      all = new long[streamLength];
      awaitCountConsumer = new AwaitCountConsumer(streamLength);
    }

    void begin(int id) {
      assert all[id] == 0;
      all[id] = -System.nanoTime();
    }

    void end(int id) {
      assert all[id] < 0;
      awaitCountConsumer.accept(id);
      all[id] += System.nanoTime();
    }

    void await(long timeout, TimeUnit unit) throws InterruptedException {
      awaitCountConsumer.await(timeout, unit);
    }

    @Override
    public void close() throws IOException {
      try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
              "/tmp/notification_await_times.csv"
      )))) {
        for (final long notificationAwaitTime : all) {
          assert notificationAwaitTime > 0;
          printWriter.println(notificationAwaitTime);
        }
      }
    }
  }

  static abstract class Tracking {
    public final NotificationAwaitTimes notificationAwaitTimes;

    Tracking(NotificationAwaitTimes notificationAwaitTimes) {
      this.notificationAwaitTimes = notificationAwaitTimes;
    }

    Stream<WatermarksVsAckerGraph.Element> followingElements(int id) {
      return Stream.empty();
    }

    abstract void accept(Object object);

    static class Disabled extends Tracking {
      Disabled() {
        super(new NotificationAwaitTimes(0));
      }

      @Override
      void accept(Object object) {}
    }

    static class Acking extends Tracking {
      final PriorityQueue<DataItem> awaitingMinTimes =
              new PriorityQueue<>(Comparator.comparing(o -> o.meta().globalTime()));

      Acking(NotificationAwaitTimes notificationAwaitTimes) {
        super(notificationAwaitTimes);
      }

      @Override
      public void accept(Object object) {
        if (object instanceof WatermarksVsAckerGraph.Element) {
          throw new RuntimeException("it is not possible to track notification await times unless using socket rear");
        }
        if (object instanceof Rear.MinTime) {
          Rear.MinTime minTime = (Rear.MinTime) object;
          while (!awaitingMinTimes.isEmpty()
                  && awaitingMinTimes.peek().meta().globalTime().compareTo(minTime.time) <= 0) {
            notificationAwaitTimes.end(awaitingMinTimes.peek().payload(WatermarksVsAckerGraph.Element.class).id);
            awaitingMinTimes.poll();
          }
        } else if (object instanceof DataItem) {
          DataItem dataItem = (DataItem) object;
          awaitingMinTimes.add(dataItem);
          notificationAwaitTimes.begin(dataItem.payload(WatermarksVsAckerGraph.Element.class).id);
        }
      }
    }

    static class Watermarking extends Tracking {
      private final int frequency;

      Watermarking(NotificationAwaitTimes notificationAwaitTimes, int frequency) {
        super(notificationAwaitTimes);
        this.frequency = frequency;
      }

      @Override
      public Stream<WatermarksVsAckerGraph.Element> followingElements(int id) {
        return ((id + 1) % frequency == 0) ?
                Stream.of(new WatermarksVsAckerGraph.Watermark(id)) : Stream.empty();
      }

      @Override
      public void accept(Object object) {
        final WatermarksVsAckerGraph.Element element;
        if (object instanceof DataItem) {
          element = ((DataItem) object).payload(WatermarksVsAckerGraph.Element.class);
        } else {
          return;
        }
        if (element instanceof WatermarksVsAckerGraph.Data) {
          notificationAwaitTimes.begin(element.id);
        } else {
          for (int offset = 0; offset < frequency; offset++) {
            notificationAwaitTimes.end(element.id - offset);
          }
        }
      }
    }
  }

  public WatermarksVsAckerBenchStand(Config benchConfig) throws ClassNotFoundException {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    streamLength = benchConfig.getInt("stream-length");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
    iterations = benchConfig.getInt("iterations");
    childrenNumber = benchConfig.getInt("children-number");
    trackingFactory = TrackingFactory.fromConfig(benchConfig.getConfig("tracking"));
    tracking = trackingFactory.create(streamLength);
  }

  public void run(GraphDeployer graphDeployer) throws Exception {
    final int warmUpStreamLength = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_STREAM_LENGTH", "200"));
    final long warmUpDelayNanos = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_DELAY_MS", "50")) * 1000000;
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(streamLength);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    final long start = System.nanoTime();
    final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger processingCount = new AtomicInteger();
    try (
            FileWriter durationOutput = new FileWriter("/tmp/duration");
            Closeable ignored2 = benchStandComponentFactory.recordNanoDuration(durationOutput);
            AutoCloseable ignored = benchStandComponentFactory.producer(
                    IntStream.range(-warmUpStreamLength, streamLength).peek(id -> {
                      processingCount.incrementAndGet();
                      if (id < 0) {
                        LockSupport.parkNanos(warmUpDelayNanos);
                        return;
                      }
                      LockSupport.parkNanos((long) (sleepBetweenDocs * 1.0e6));
                      latencies.put(id, new LatencyMeasurer());
                      if ((id + 1) % 100 == 0) {
                        LOG.info("Sending: {}", id + 1);
                      }
                    }).boxed().flatMap(id ->
                            Stream.concat(
                                    Stream.of(new WatermarksVsAckerGraph.Data(id)),
                                    tracking.followingElements(id)
                            )
                    ),
                    inputHost,
                    frontPort,
                    WatermarksVsAckerGraph.Element.class,
                    WatermarksVsAckerGraph.Data.class,
                    WatermarksVsAckerGraph.Child.class,
                    WatermarksVsAckerGraph.Watermark.class
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      final WatermarksVsAckerGraph.Element element;
                      if (object instanceof DataItem) {
                        element = ((DataItem) object).payload(WatermarksVsAckerGraph.Element.class);
                      } else if (object instanceof WatermarksVsAckerGraph.Element) {
                        element = (WatermarksVsAckerGraph.Element) object;
                      } else {
                        element = null;
                      }
                      if (element != null) {
                        processingCount.decrementAndGet();
                        if (element.id < 0) {
                          return;
                        }
                        if (element instanceof WatermarksVsAckerGraph.Data) {
                          durations.add(System.nanoTime() - start);
                          latencies.get(element.id).finish();
                          awaitConsumer.accept(element.id);
                          if (awaitConsumer.got() % 100 == 0) {
                            LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                          }
                          if (element.id % 100 == 0) {
                            LOG.info("Got id {}", element.id);
                          }
                        }
                      }
                      tracking.accept(object);
                    },
                    rearPort,
                    WatermarksVsAckerGraph.Element.class,
                    WatermarksVsAckerGraph.Data.class,
                    WatermarksVsAckerGraph.Child.class,
                    WatermarksVsAckerGraph.Watermark.class

            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(60, TimeUnit.MINUTES);
      tracking.notificationAwaitTimes.await(5, TimeUnit.MINUTES);
      tracking.notificationAwaitTimes.close();
      try (FileWriter durationsOutput = new FileWriter("/tmp/durations")) {
        durationsOutput.write(
                durations.stream().map(duration -> Long.toString(duration)).collect(Collectors.joining(", "))
        );
      }
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
