package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryonet.Connection;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.example.labels.BinarySocialGraph;
import com.spbsu.flamestream.example.labels.BreadthSearchGraph;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
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
public class BreadthSearchGraphBenchStand {
  private static final BinarySocialGraph binarySocialGraph;

  static {
    try {
      binarySocialGraph = new BinarySocialGraph(
              new File(System.getenv("EDGES_TAIL_FILE")),
              new File(System.getenv("EDGES_HEAD_FILE"))
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final Class<?>[] FRONT_CLASSES_TO_REGISTER = {
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.Request.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.Request.Identifier.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.VertexIdentifier.class
  }, REAR_CLASSES_TO_REGISTER = {
          BreadthSearchGraph.RequestOutput.class,
          BreadthSearchGraph.Request.Identifier.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.VertexIdentifier.class,
          ArrayList.class,
          long[].class,
          Left.class,
          Right.class,
          };
  private final int streamLength;
  private final int parallelism;

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
    final BreadthSearchGraphBenchStand wikiBenchStand = new BreadthSearchGraphBenchStand(benchConfig);
    final String workerIdPrefix = benchConfig.getString("worker-id-prefix");
    final SystemConfig.WorkersResourcesDistributor.Enumerated workersResourcesDistributor =
            new SystemConfig.WorkersResourcesDistributor.Enumerated(workerIdPrefix, 1);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(
                            deployerConfig,
                            new SystemConfig.Builder().workersResourcesDistributor(workersResourcesDistributor).build()
                    ),
                    Materializer.materialize(BreadthSearchGraph.immutableFlow(hashGroup -> {
                      try {
                        return binarySocialGraph.new BinaryOutboundEdges(
                                hashGroup,
                                Integer.parseInt(System.getenv("TAIL_EDGES_LIMIT"))
                        );
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    })),
                    new SocketFrontType(wikiBenchStand.benchHost, wikiBenchStand.frontPort, FRONT_CLASSES_TO_REGISTER),
                    new SocketRearType(wikiBenchStand.benchHost, wikiBenchStand.rearPort, REAR_CLASSES_TO_REGISTER)
            )
    ) {
      final String frontsNumber = System.getenv("FRONTS_NUMBER");
      wikiBenchStand.run(
              graphDeployer,
              IntStream.range(1, frontsNumber == null ? wikiBenchStand.parallelism : Integer.parseInt(frontsNumber))
                      .mapToObj(i -> workerIdPrefix + i).collect(Collectors.toList())
      );
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BreadthSearchGraphBenchStand.class);

  private final int sleepBetweenDocs;
  public final String benchHost;
  public final int frontPort;
  public final int rearPort;

  public BreadthSearchGraphBenchStand(Config benchConfig) {
    streamLength = benchConfig.getInt("stream-length");
    sleepBetweenDocs = benchConfig.getInt("sleep-between-docs-ms");
    benchHost = benchConfig.getString("bench-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
  }

  public void run(GraphDeployer graphDeployer, List<String> inputHosts) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final int requestsNumber = streamLength;
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(requestsNumber);
    final Map<Integer, Integer> remainingRequestResponses = new ConcurrentHashMap<>();
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    final Map<Integer, int[]> requestOutputCount = new HashMap<>();
    final int[] allTails = new int[binarySocialGraph.size()];
    try (final BinarySocialGraph.CloseableTailsIterator tails = binarySocialGraph.new CloseableTailsIterator()) {
      int i = 0;
      while (tails.next()) {
        allTails[i++] = tails.vertex();
      }
    }
    final Random random = new Random(0);
    final Semaphore semaphore = new Semaphore(Integer.parseInt(System.getenv("SIMULTANEOUS_REQUESTS")));
    final CompletableFuture<Map<String, Connection>> producerConnections = new CompletableFuture<>();
    final Thread producer = new Thread(() -> {
      try {
        final Map<String, Connection> connections = producerConnections.get();
        int requestId = 0;
        while (true) {
          for (final Connection connection : connections.values()) {
            semaphore.acquire();
            final int vertex = allTails[random.nextInt(allTails.length)];
            System.out.println("produced " + requestId + " " + vertex);
            latencies.put(requestId, new LatencyMeasurer());
            remainingRequestResponses.put(requestId, parallelism - 1);
            requestOutputCount.put(requestId, new int[1]);
            connection.sendTCP(new BreadthSearchGraph.Request(
                    new BreadthSearchGraph.Request.Identifier(requestId),
                    new BreadthSearchGraph.VertexIdentifier(vertex),
                    2
            ));
            requestId++;
          }
        }
      } catch (InterruptedException ignored) {
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
    producer.start();
    try (
            AutoCloseable ignored = benchStandComponentFactory.producerConnections(
                    producerConnections::complete,
                    frontPort,
                    inputHosts,
                    FRONT_CLASSES_TO_REGISTER
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      final Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.Request.Identifier> output;
                      if (object instanceof DataItem) {
                        output = ((DataItem) object).payload(BreadthSearchGraph.OUTPUT_CLASS);
                      } else if (object instanceof Either) {
                        output = (Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.Request.Identifier>) object;
                      } else {
                        return;
                      }
                      if (output.isRight()) {
                        final BreadthSearchGraph.Request.Identifier identifier = output.right().get();
                        final Integer compute = remainingRequestResponses.compute(
                                identifier.id,
                                (__, value) -> value > 1 ? value - 1 : null
                        );
                        if (compute == null) {
                          System.out.println(
                                  "consumed " + identifier.id + " " + requestOutputCount.remove(identifier.id)[0]
                          );
                          latencies.get(identifier.id).finish();
                          awaitConsumer.accept(output);
                          semaphore.release();
                          if (awaitConsumer.got() % 10000 == 0) {
                            LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                          }
                        }
                      } else {
                        requestOutputCount.get(output.left().get().requestIdentifier.id)[0]++;
                      }
                    },
                    rearPort,
                    REAR_CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(1, TimeUnit.MINUTES);
      producer.interrupt();
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
