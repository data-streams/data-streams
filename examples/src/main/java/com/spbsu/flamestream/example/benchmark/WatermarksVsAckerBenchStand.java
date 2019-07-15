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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;
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
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(
                            deployerConfig,
                            new WorkerApplication.WorkerConfig.Builder()
                                    .millisBetweenCommits(1000000000)
                                    .barrierDisabled(true)
                                    .acking(
                                            benchStand.watermarks ?
                                                    SystemConfig.Acking.DISABLED :
                                                    SystemConfig.Acking.CENTRALIZED
                                    )
                                    ::build
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
  private final boolean watermarks;
  private final int iterations;
  private final int childrenNumber;
  private final int watermarksFrequency;

  static interface Flow<Input, Output> extends Closeable {
    static class Control {
    }

    static class Continue extends Control {
      static Continue INSTANCE = new Continue();

      private Continue() {}
    }

    static class Interrupt extends Control {
      static Interrupt INSTANCE = new Interrupt();

      private Interrupt() {}
    }

    Control beforeSend(Input input);

    Control onReceive(Output output);
  }

  static class ContinueFlow<Input, Output> implements Flow<Input, Output> {
    @Override
    public Control beforeSend(Input input) {
      return Continue.INSTANCE;
    }

    @Override
    public Control onReceive(Output output) {
      return Continue.INSTANCE;
    }

    @Override
    public void close() throws IOException {
      notify();
    }
  }

  static class ProcessingLimitFlow<Input, Output> implements Flow<Input, Output> {
    final int limit;
    final Flow<? super Input, ? super Output> other;
    final AtomicInteger count = new AtomicInteger();

    ProcessingLimitFlow(int limit, Flow other) {
      this.limit = limit;
      this.other = other;
    }

    @Override
    public Control beforeSend(Input input) {
      if (count.incrementAndGet() < limit) {
        return other.beforeSend(input);
      }
      return Interrupt.INSTANCE;
    }

    @Override
    public Control onReceive(Output output) {
      count.decrementAndGet();
      return other.onReceive(output);
    }

    @Override
    public void close() throws IOException {
    }
  }

  //static class WarmupFlow implements Flow {
  //  final int length;
  //  final Flow other;
  //  private long delayNanos;
  //
  //  WarmupFlow(int length, long delayNanos, Flow other) {
  //    this.length = length;
  //    this.other = other;
  //    this.delayNanos = delayNanos;
  //  }
  //
  //  @Override
  //  public Control beforeSend(int id) {
  //    if (id < 0) {
  //      LockSupport.parkNanos(delayNanos);
  //      return Continue.INSTANCE;
  //    }
  //    return other.beforeSend(id);
  //  }
  //
  //  @Override
  //  public Control onReceive(int id) {
  //    if (id < 0) {
  //      return Continue.INSTANCE;
  //    }
  //    return other.onReceive(id);
  //  }
  //
  //  @Override
  //  public void close() throws IOException {
  //  }
  //}

  static interface FlowV2<Input, Output> extends Iterator<Input>, Sink<Output> {
  }

  public static interface Sink<Output> {
    void accept(Output output) throws Done;

    static class Control {
    }

    static class Continue extends Control {
      static Continue INSTANCE = new Continue();

      private Continue() {}
    }

    static class Interrupt extends Control {
      static Interrupt INSTANCE = new Interrupt();

      private Interrupt() {}
    }

    static class Done extends Exception {
    }
  }

  static interface IndexedProgressTracker<Progress> {
    void start(int index, Progress progress);

    Progress get(int index);

    void update(int index, Progress progress);

    void finish(int index);
  }

  static class RingBufferIndexedProgressTracker<Progress> implements IndexedProgressTracker<Progress> {
    private final Progress[] progresses;
    private int currentIndex = 0, currentIndexArrayIndex = 0;

    RingBufferIndexedProgressTracker(int bufferSize) {
      progresses = (Progress[]) new Object[bufferSize];
    }

    @Override
    public void start(int index, Progress progress) {
    }

    @Override
    public Progress get(int index) {
      return null;
    }

    @Override
    public void update(int index, Progress progress) {

    }

    @Override
    public void finish(int index) {

    }
  }

  static class InfinityLikeFlow implements FlowV2<Integer, Integer> {

    private final boolean[] received;
    private final Sink<Integer> sink;
    private int emittedCount = 0, receivedCount = 0;
    private boolean isDone = false;

    InfinityLikeFlow(int length, Sink<Integer> sink) {
      this.received = new boolean[length];
      this.sink = sink;
    }

    @Override
    public boolean hasNext() {
      if (!isDone) {
        return true;
      }
      //noinspection ConstantConditions
      isDone = true;
      return false;
    }

    @Override
    public Integer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return emittedCount++;
    }

    @Override
    public void accept(Integer output) throws Done {
      assert !received[output];
      received[output] = true;
      ++receivedCount;

      try {
        sink.accept(output);
        if (receivedCount == received.length) {
          throw new Done();
        }
      } catch (Done done) {
        isDone = true;
        throw done;
      }
    }
  }

  public WatermarksVsAckerBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    streamLength = benchConfig.getInt("stream-length");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
    watermarks = benchConfig.getBoolean("watermarks");
    iterations = benchConfig.getInt("iterations");
    childrenNumber = benchConfig.getInt("children-number");
    watermarksFrequency = benchConfig.getInt("watermarks-frequency");
    if (streamLength % watermarksFrequency != 0) {
      throw new IllegalArgumentException("watermarks frequency should be a stream length divisor");
    }
  }

  public void run(GraphDeployer graphDeployer) throws Exception {
    final int warmUpStreamLength = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_STREAM_LENGTH", "200"));
    final long warmUpDelayNanos = Integer.parseInt(System.getenv().getOrDefault("WARM_UP_DELAY_MS", "50")) * 1000000;
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(streamLength);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    final long start = System.nanoTime();
    final List<Long> durations = Collections.synchronizedList(new ArrayList<>());
    final long[] notificationAwaitTimes = new long[streamLength];
    final AtomicInteger processingCount = new AtomicInteger();
    final PriorityQueue<DataItem> awaitingMinTimes =
            new PriorityQueue<>(Comparator.comparing(o -> o.meta().globalTime()));
    final IntConsumer finishedIds = id -> {
      durations.add(System.nanoTime() - start);
      latencies.get(id).finish();
      awaitConsumer.accept(id);
      if (awaitConsumer.got() % 100 == 0) {
        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
      }
      if (id % 100 == 0) {
        LOG.info("Got id {}", id);
      }
    };
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
                                    watermarks && ((id + 1) % watermarksFrequency == 0) ?
                                            Stream.of(new WatermarksVsAckerGraph.Watermark(id)) : Stream.empty()
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
                        if (object instanceof Rear.MinTime) {
                          Rear.MinTime minTime = (Rear.MinTime) object;
                          while (!awaitingMinTimes.isEmpty()
                                  && awaitingMinTimes.peek().meta().globalTime().compareTo(minTime.time) <= 0) {
                            final int id = awaitingMinTimes.peek().payload(WatermarksVsAckerGraph.Element.class).id;
                            notificationAwaitTimes[id] += System.nanoTime();
                            awaitingMinTimes.poll();
                            finishedIds.accept(id);
                          }
                        }
                        return;
                      }
                      processingCount.decrementAndGet();
                      if (element.id < 0) {
                        return;
                      }
                      if (watermarks) {
                        if (element instanceof WatermarksVsAckerGraph.Data) {
                          notificationAwaitTimes[element.id] = -System.nanoTime();
                        } else {
                          for (int offset = 0; offset < watermarksFrequency; offset++) {
                            final int id = element.id - offset;
                            notificationAwaitTimes[id] += System.nanoTime();
                            finishedIds.accept(id);
                          }
                        }
                      } else {
                        if (object instanceof DataItem) {
                          awaitingMinTimes.add((DataItem) object);
                          notificationAwaitTimes[element.id] = -System.nanoTime();
                        } else {
                          finishedIds.accept(element.id);
                        }
                      }
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
      try (FileWriter durationsOutput = new FileWriter("/tmp/durations")) {
        durationsOutput.write(
                durations.stream().map(duration -> Long.toString(duration)).collect(Collectors.joining(", "))
        );
      }
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
    }
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/notification_await_times.csv"
    )))) {
      for (final long notificationAwaitTime : notificationAwaitTimes) {
        assert notificationAwaitTime > 0;
        printWriter.println(notificationAwaitTime);
      }
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
