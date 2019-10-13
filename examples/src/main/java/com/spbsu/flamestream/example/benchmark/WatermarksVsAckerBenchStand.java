package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryonet.Connection;
import com.google.common.collect.Iterables;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.example.bl.WatermarksVsAckerGraph;
import com.spbsu.flamestream.runtime.config.HashUnit;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    final SystemConfig.Builder builder = new SystemConfig.Builder()
            .millisBetweenCommits(1000000000)
            .barrierDisabled(true)
            .workersResourcesDistributor(
                    new SystemConfig.WorkersResourcesDistributor.Enumerated(
                            benchStand.workerIdPrefix,
                            benchStand.trackingFactory.getAckersNumber()
                    )
            );
    final List<String> workerIds =
            IntStream.range(1 + benchStand.trackingFactory.getAckersNumber(), benchStand.parallelism)
                    .mapToObj(index -> benchStand.workerIdPrefix + index)
                    .collect(Collectors.toList());
    final List<String> frontHostIds = System.getenv().containsKey("FRONTS_NUMBER")
            ? workerIds.subList(0, Integer.parseInt(System.getenv("FRONTS_NUMBER")))
            : workerIds;
    benchStand.trackingFactory.configureSystem(builder);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(deployerConfig, builder.build()),
                    WatermarksVsAckerGraph.apply(
                            frontHostIds.size(),
                            HashUnit.covering(workerIds.size()).collect(Collectors.toCollection(ArrayList::new)),
                            benchStand.iterations
                    ),
                    new SocketFrontType(
                            benchStand.benchHost,
                            benchStand.frontPort,
                            WatermarksVsAckerGraph.Element.class,
                            WatermarksVsAckerGraph.Data.class,
                            WatermarksVsAckerGraph.Watermark.class
                    ),
                    new SocketRearType(
                            benchStand.benchHost,
                            benchStand.rearPort,
                            WatermarksVsAckerGraph.Element.class,
                            WatermarksVsAckerGraph.Data.class,
                            WatermarksVsAckerGraph.Watermark.class
                    )
            )
    ) {
      benchStand.run(graphDeployer, frontHostIds);
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(WatermarksVsAckerBenchStand.class);

  private final double sleepBetweenDocs;
  private final int streamLength;
  private final String benchHost;
  private final int frontPort;
  private final int rearPort;
  private final int parallelism;
  private final String workerIdPrefix;
  private final int iterations;
  private final TrackingFactory trackingFactory;
  private final List<String> frontHostIds;
  private final Tracking<?> tracking;

  interface TrackingFactory {
    static TrackingFactory fromConfig(Config config) throws ClassNotFoundException {
      //noinspection unchecked
      return ConfigBeanFactory.create(
              config.withoutPath("class"),
              (Class<TrackingFactory>) ClassLoader.getSystemClassLoader().loadClass(config.getString("class"))
      );
    }

    Tracking create(int streamLength, int workersNumber);

    default void configureSystem(SystemConfig.Builder builder) {}

    default int getAckersNumber() {
      return 0;
    }

    class Disabled implements TrackingFactory {
      @Override
      public Tracking create(int streamLength, int workersNumber) {
        return new Tracking.Disabled();
      }

    }

    class Acking implements TrackingFactory {
      private int ackersNumber = 1;

      @Override
      public Tracking create(int streamLength, int workersNumber) {
        return new Tracking.Acking(new NotificationAwaitTimes<>(
                streamLength,
                new PriorityQueue<>(Comparator.comparing(o -> o.meta().globalTime())),
                (dataItem, time) -> dataItem.meta().globalTime().compareTo(time) <= 0
        ));
      }

      @Override
      public void configureSystem(SystemConfig.Builder builder) {
        builder.ackerWindow(10);
      }

      @Override
      public int getAckersNumber() {
        return ackersNumber;
      }

      public void setAckersNumber(int ackersNumber) {
        this.ackersNumber = ackersNumber;
      }

    }

    class Watermarking implements TrackingFactory {
      private int frequency = 1;

      @Override
      public Tracking create(int streamLength, int workersNumber) {
        return new Tracking.Watermarking(new NotificationAwaitTimes<>(
                streamLength,
                new PriorityQueue<>(Comparator.comparingInt(o -> o.payload(WatermarksVsAckerGraph.Data.class).id)),
                (dataItem, integer) -> dataItem.payload(WatermarksVsAckerGraph.Data.class).id <= integer
        ), frequency, workersNumber);
      }

      public int getFrequency() {
        return this.frequency;
      }

      public void setFrequency(int frequency) {
        this.frequency = frequency;
      }
    }
  }

  static final class NotificationAwaitTimes<Time> implements Closeable {
    private final long[] all;
    private final AwaitCountConsumer awaitCountConsumer;
    private final PriorityQueue<DataItem> dataItems;
    private final BiPredicate<DataItem, Time> dataItemMatchesTime;

    NotificationAwaitTimes(
            int streamLength,
            PriorityQueue<DataItem> dataItems,
            BiPredicate<DataItem, Time> dataItemMatchesTime
    ) {
      all = new long[streamLength];
      awaitCountConsumer = new AwaitCountConsumer(streamLength);
      this.dataItems = dataItems;
      this.dataItemMatchesTime = dataItemMatchesTime;
    }

    void add(DataItem dataItem) {
      dataItems.add(dataItem);
      begin(dataItem.payload(WatermarksVsAckerGraph.Element.class).id);
    }

    void minTimeUpdate(Time time) {
      while (!dataItems.isEmpty() && dataItemMatchesTime.test(dataItems.peek(), time)) {
        end(dataItems.poll().payload(WatermarksVsAckerGraph.Element.class).id);
      }
    }

    private void log(Logger logger) {
      if (logger != null) {
        logger.info(
                "NotificationAwaitTimes: dataItems.size() = {}, dataItems.peek() = {}, awaitCountConsumer = {}",
                dataItems.size(),
                dataItems.isEmpty() ? null : dataItems.peek()
                        .payload(WatermarksVsAckerGraph.Element.class).id,
                awaitCountConsumer
        );
      }
    }

    private void begin(int id) {
      if (id < 0 || id >= all.length) {
        return;
      }
      assert all[id] == 0;
      all[id] = -System.nanoTime();
    }

    private void end(int id) {
      if (id < 0 || id >= all.length) {
        return;
      }
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

  static abstract class Tracking<Time> {
    public final NotificationAwaitTimes<Time> notificationAwaitTimes;

    Tracking(NotificationAwaitTimes<Time> notificationAwaitTimes) {
      this.notificationAwaitTimes = notificationAwaitTimes;
    }

    void send(int id, List<Connection> connections) {
    }

    abstract void accept(Object object, Logger logger);

    static class Disabled extends Tracking {
      Disabled() {
        super(null);
      }

      @Override
      void accept(Object object, Logger logger) {}
    }

    static class Acking extends Tracking<GlobalTime> {
      final PriorityQueue<DataItem> awaitingMinTimes =
              new PriorityQueue<>(Comparator.comparing(o -> o.meta().globalTime()));

      Acking(NotificationAwaitTimes<GlobalTime> notificationAwaitTimes) {
        super(notificationAwaitTimes);
      }

      @Override
      public void accept(Object object, Logger logger) {
        if (object instanceof WatermarksVsAckerGraph.Element) {
          throw new RuntimeException("it is not possible to track notification await times unless using socket rear");
        }
        if (object instanceof Rear.MinTime) {
          notificationAwaitTimes.minTimeUpdate(((Rear.MinTime) object).time);
        } else if (object instanceof DataItem) {
          notificationAwaitTimes.add((DataItem) object);
        }
      }
    }

    static class Watermarking extends Tracking<Integer> {
      private final int frequency;
      private final int[] watermarks;
      int minWatermark = Integer.MIN_VALUE;

      Watermarking(NotificationAwaitTimes<Integer> notificationAwaitTimes, int frequency, int workersNumber) {
        super(notificationAwaitTimes);
        this.frequency = frequency;
        watermarks = new int[workersNumber];
        Arrays.fill(watermarks, minWatermark);
      }

      @Override
      void send(int id, List<Connection> connections) {
        if (Math.floorMod(Math.floorDiv(id, connections.size()), frequency) == 0) {
          final int partition = Math.floorMod(id, connections.size());
          connections.get(partition).sendTCP(new WatermarksVsAckerGraph.Watermark(id, partition));
        }
      }

      @Override
      public void accept(Object object, Logger logger) {
        if (!(object instanceof DataItem)) {
          return;
        }
        final DataItem dataItem = (DataItem) object;
        final WatermarksVsAckerGraph.Element element = dataItem.payload(WatermarksVsAckerGraph.Element.class);
        if (element instanceof WatermarksVsAckerGraph.Data) {
          notificationAwaitTimes.add(dataItem);
        }
        if (element instanceof WatermarksVsAckerGraph.Watermark) {
          final WatermarksVsAckerGraph.Watermark incoming = (WatermarksVsAckerGraph.Watermark) element;
          if (watermarks[incoming.fromPartition] < incoming.id) {
            watermarks[incoming.fromPartition] = incoming.id;
            int watermarkToEmit = watermarks[0];
            for (final int watermark : watermarks) {
              watermarkToEmit = Math.min(watermarkToEmit, watermark);
            }
            if (minWatermark < watermarkToEmit) {
              minWatermark = watermarkToEmit;
              notificationAwaitTimes.minTimeUpdate(minWatermark);
            }
          }
        }
      }
    }
  }

  private static <T> Iterable<T> delayedIterable(long delayNanos, Iterable<T> iterator) {
    return () -> {
      final long start = System.nanoTime();
      final int[] index = {0};
      return Iterables.transform(iterator, object -> {
        LockSupport.parkNanos(delayNanos * ++index[0] - System.nanoTime() + start);
        return object;
      }).iterator();
    };
  }

  public WatermarksVsAckerBenchStand(Config benchConfig) throws ClassNotFoundException {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    streamLength = benchConfig.getInt("stream-length");
    benchHost = benchConfig.getString("bench-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
    workerIdPrefix = benchConfig.getString("worker-id-prefix");
    iterations = benchConfig.getInt("iterations");
    trackingFactory = TrackingFactory.fromConfig(benchConfig.getConfig("tracking"));
    frontHostIds = IntStream.range(1 + trackingFactory.getAckersNumber(), parallelism)
            .mapToObj(index -> workerIdPrefix + index)
            .collect(Collectors.toList());
    tracking = trackingFactory.create(streamLength, frontHostIds.size());
  }

  public void run(GraphDeployer graphDeployer, List<String> inputHostIds) throws Exception {
    final int warmUpStreamLength = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_STREAM_LENGTH", "200"));
    final long warmUpDelayNanos = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_DELAY_MS", "50")) * 1000000;
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(streamLength);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    final long benchStart = System.nanoTime();
    final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger processingCount = new AtomicInteger();
    final long[] consumerNotifyAt = new long[]{System.nanoTime()};
    final boolean[] isDone = new boolean[]{false};
    try (
            FileWriter durationOutput = new FileWriter("/tmp/duration");
            Closeable ignored2 = benchStandComponentFactory.recordNanoDuration(durationOutput);
            AutoCloseable ignored = benchStandComponentFactory.producerConnections(
                    connections -> {
                      final ArrayList<Connection> connectionsList = new ArrayList<>(connections.values());
                      for (int id : Iterables.concat(
                              delayedIterable(
                                      warmUpDelayNanos,
                                      IntStream.range(-warmUpStreamLength, 0).boxed()::iterator
                              ),
                              Iterables.transform(delayedIterable(
                                      (long) (sleepBetweenDocs * 1.0e6),
                                      () -> new Iterator<Integer>() {
                                        private int count;

                                        @Override
                                        public boolean hasNext() {
                                          return !isDone[0];
                                        }

                                        @Override
                                        public Integer next() {
                                          return count++;
                                        }
                                      }
                              ), id -> {
                                latencies.put(id, new LatencyMeasurer());
                                if ((id + 1) % 1000 == 0) {
                                  LOG.info("Sending: {}", id + 1);
                                }
                                return id;
                              })
                      )) {
                        processingCount.incrementAndGet();
                        connections.get(inputHostIds.get(Math.floorMod(id, inputHostIds.size())))
                                .sendTCP(new WatermarksVsAckerGraph.Data(id));
                        tracking.send(id, connectionsList);
                      }
                    },
                    frontPort,
                    inputHostIds,
                    WatermarksVsAckerGraph.Element.class,
                    WatermarksVsAckerGraph.Data.class,
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
                      Logger log = null;
                      if (element != null) {
                        if (element instanceof WatermarksVsAckerGraph.Data && consumerNotifyAt[0] < System.nanoTime()) {
                          consumerNotifyAt[0] = (long) (System.nanoTime() + 1E9);
                          log = LOG;
                          log.info(
                                  "Got id {}, Progress: {}/{}",
                                  element.id,
                                  awaitConsumer.got(),
                                  awaitConsumer.expected()
                          );
                        }
                        processingCount.decrementAndGet();
                        if (element.id >= 0 && element.id < streamLength
                                && element instanceof WatermarksVsAckerGraph.Data) {
                          durations.add(System.nanoTime() - benchStart);
                          latencies.get(element.id).finish();
                          awaitConsumer.accept(element.id);
                        }
                      }
                      tracking.accept(object, log);
                      if (tracking.notificationAwaitTimes != null)
                        tracking.notificationAwaitTimes.log(log);
                    },
                    rearPort,
                    WatermarksVsAckerGraph.Element.class,
                    WatermarksVsAckerGraph.Data.class,
                    WatermarksVsAckerGraph.Watermark.class

            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(60, TimeUnit.MINUTES);
      try (NotificationAwaitTimes notificationAwaitTimes = tracking.notificationAwaitTimes) {
        if (notificationAwaitTimes != null) {
          notificationAwaitTimes.await(5, TimeUnit.MINUTES);
        }
      }
      isDone[0] = true;
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
